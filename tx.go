package masterkeeper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

type TableChangeSet struct {
	Cleared bool
	Inserts map[any]any // id -> record struct
	Updates map[any]any // id -> record struct
	Deletes map[any]struct{}
}

func NewTableChangeSet() *TableChangeSet {
	return &TableChangeSet{
		Inserts: make(map[any]any),
		Updates: make(map[any]any),
		Deletes: make(map[any]struct{}),
	}
}

func (tableChangeSet *TableChangeSet) IsEmpty() bool {
	return !tableChangeSet.Cleared && len(tableChangeSet.Inserts) == 0 && len(tableChangeSet.Updates) == 0 && len(tableChangeSet.Deletes) == 0
}

type IndexChange struct {
	TableName  string
	IndexName  string
	IndexValue   any
	PrimaryKey any
}

type IndexChangeSet struct {
	Added   []IndexChange
	Removed []IndexChange
}

func (indexChangeSet *IndexChangeSet) Add(tableName, indexName string, indexValue, primaryKey any) {
	indexChangeSet.Added = append(indexChangeSet.Added, IndexChange{
		TableName:  tableName,
		IndexName:  indexName,
		IndexValue:   indexValue,
		PrimaryKey: primaryKey,
	})
}

func (indexChangeSet *IndexChangeSet) Remove(tableName, indexName string, indexValue, primaryKey any) {
	indexChangeSet.Removed = append(indexChangeSet.Removed, IndexChange{
		TableName:  tableName,
		IndexName:  indexName,
		IndexValue:   indexValue,
		PrimaryKey: primaryKey,
	})
}

func (indexChangeSet *IndexChangeSet) Clear() {
	indexChangeSet.Added = nil
	indexChangeSet.Removed = nil
}

type TransactionChangeSet struct {
	TableChanges   map[string]*TableChangeSet
	IndexChanges   IndexChangeSet
	RollbackOnly   bool
	RollbackReason string
}

func NewTransactionChangeSet() *TransactionChangeSet {
	return &TransactionChangeSet{
		TableChanges: make(map[string]*TableChangeSet),
	}
}

func (transactionChangeSet *TransactionChangeSet) GetTableChanges(tableName string) *TableChangeSet {
	tableChangeSet, found := transactionChangeSet.TableChanges[tableName]
	if !found {
		tableChangeSet = NewTableChangeSet()
		transactionChangeSet.TableChanges[tableName] = tableChangeSet
	}
	return tableChangeSet
}

type Transaction struct {
	transactionID  int64
	database       *Database
	committedState *DatabaseState
	changeSet      *TransactionChangeSet
	active         bool
}

func NewTransaction(transactionID int64, database *Database, committedState *DatabaseState) *Transaction {
	return &Transaction{
		transactionID:  transactionID,
		database:       database,
		committedState: committedState,
		changeSet:      NewTransactionChangeSet(),
		active:         true,
	}
}

func (transaction *Transaction) TransactionID() int64 {
	return transaction.transactionID
}

func (transaction *Transaction) IsActive() bool {
	return transaction.active
}

func (transaction *Transaction) SetRollbackOnly(reason string) {
	transaction.changeSet.RollbackOnly = true
	transaction.changeSet.RollbackReason = reason
}

func (transaction *Transaction) IsRollbackOnly() bool {
	return transaction.changeSet.RollbackOnly
}

func (transaction *Transaction) RollbackReason() string {
	return transaction.changeSet.RollbackReason
}

func (transaction *Transaction) Rollback() error {
	if !transaction.active {
		return nil // Idempotent
	}

	transaction.active = false
	defer transaction.database.releaseWriterLock()

	nextGenerationeration := transaction.database.currentGeneration() + 1
	error := transaction.database.walManager.AppendRollbackMarker(transaction.transactionID, nextGenerationeration, transaction.changeSet.RollbackReason)
	if error != nil {
		return fmt.Errorf("failed to write rollback marker to WAL: %w", error)
	}

	return nil
}

func (transaction *Transaction) Commit() error {
	if !transaction.active {
		return NotActiveTransactionError
	}
	if transaction.changeSet.RollbackOnly {
		_ = transaction.Rollback()
		return fmt.Errorf("%w: %s", RollbackOnlyTransactionError, transaction.changeSet.RollbackReason)
	}

	transaction.active = false
	defer transaction.database.releaseWriterLock()

	// 1. Validate unique indexes
	if error := transaction.validateUniqueIndexes(); error != nil {
		_ = transaction.Rollback()
		return error
	}

	nextGenerationeration := transaction.database.currentGeneration() + 1

	// 2. Prepare WAL records and Table appends
	var walRecords []WalRecord
	tableAppends := make(map[string][][]byte)

	insertsOrder := make(map[string][]any)
	updatesOrder := make(map[string][]any)

	for tableName, tableChangeSet := range transaction.changeSet.TableChanges {
		if tableChangeSet.IsEmpty() {
			continue
		}

		committedTable := transaction.committedState.Tables[tableName]
		var entityClassName string
		var idClassName string
		if committedTable != nil {
			entityClassName = committedTable.EntityType.String()
			idClassName = committedTable.IdType.String()
		}

		// Table clear log
		if tableChangeSet.Cleared {
			var buf bytes.Buffer
			_ = writeString(&buf, tableName)
			walRecords = append(walRecords, WalRecord{
				Type:          OpClearTable,
				TransactionID: transaction.transactionID,
				Generation:    nextGenerationeration,
				Payload:       buf.Bytes(),
			})
		}

		// Deletes log
		for key := range tableChangeSet.Deletes {
			var buf bytes.Buffer
			_ = writeString(&buf, tableName)
			_ = writeString(&buf, idClassName)

			// serialize the primary key
			var keyBuf bytes.Buffer
			_ = writeValue(&keyBuf, reflect.ValueOf(key))
			_, _ = buf.Write(keyBuf.Bytes())

			walRecords = append(walRecords, WalRecord{
				Type:          OpDelete,
				TransactionID: transaction.transactionID,
				Generation:    nextGenerationeration,
				Payload:       buf.Bytes(),
			})
		}

		// Collect inserts and updates to slices to preserve deterministic order
		var insertsList []any
		for _, record := range tableChangeSet.Inserts {
			insertsList = append(insertsList, record)
		}
		insertsOrder[tableName] = insertsList

		var updatesList []any
		for _, record := range tableChangeSet.Updates {
			updatesList = append(updatesList, record)
		}
		updatesOrder[tableName] = updatesList

		// Inserts log and preparation for table appends
		for _, record := range insertsList {
			recBytes, error := Marshal(record)
			if error != nil {
				return fmt.Errorf("failed to serialize record for commit: %w", error)
			}
			tableAppends[tableName] = append(tableAppends[tableName], recBytes)

			var buf bytes.Buffer
			_ = writeString(&buf, tableName)
			_ = writeString(&buf, entityClassName)
			_ = binary.Write(&buf, binary.BigEndian, int32(len(recBytes)))
			_, _ = buf.Write(recBytes)

			walRecords = append(walRecords, WalRecord{
				Type:          OpInsert,
				TransactionID: transaction.transactionID,
				Generation:    nextGenerationeration,
				Payload:       buf.Bytes(),
			})
		}

		// Updates log and preparation for table appends
		for _, record := range updatesList {
			recBytes, error := Marshal(record)
			if error != nil {
				return fmt.Errorf("failed to serialize record for commit: %w", error)
			}
			tableAppends[tableName] = append(tableAppends[tableName], recBytes)

			var buf bytes.Buffer
			_ = writeString(&buf, tableName)
			_ = writeString(&buf, entityClassName)
			_ = binary.Write(&buf, binary.BigEndian, int32(len(recBytes)))
			_, _ = buf.Write(recBytes)

			walRecords = append(walRecords, WalRecord{
				Type:          OpUpdate,
				TransactionID: transaction.transactionID,
				Generation:    nextGenerationeration,
				Payload:       buf.Bytes(),
			})
		}
	}

	// 3. Submit write task to background writer goroutine and wait
	if len(walRecords) > 0 || len(tableAppends) > 0 {
		doneChan := make(chan WriteResult, 1)
		task := &WriteTask{
			TxID:         transaction.transactionID,
			Generation:   nextGenerationeration,
			WalRecords:   walRecords,
			TableAppends: tableAppends,
			Done:         doneChan,
		}

		transaction.database.walManager.Submit(task)
		result := <-doneChan
		if result.Error != nil {
			return fmt.Errorf("background write failure: %w", result.Error)
		}

		// 4. Update DatabaseState in memory
		nextState := transaction.committedState.Copy(nextGenerationeration)
		for tableName, tableChangeSet := range transaction.changeSet.TableChanges {
			tableState := nextState.Tables[tableName]
			if tableState == nil {
				continue
			}

			if tableChangeSet.Cleared {
				tableState.Clear()
				if shadowError := transaction.database.shadowClear(tableName); shadowError != nil {
					return shadowError
				}
			}

			modifiedShadow := false

			// Apply deletes
			for key := range tableChangeSet.Deletes {
				oldRecord, error := transaction.readCommittedRecord(tableName, key)
				if error != nil {
					return error
				}
				tableState.Delete(key, oldRecord)
				if shadowError := transaction.database.shadowDeleteNoCommit(tableName, key, oldRecord, uint64(nextGenerationeration)); shadowError != nil {
					return shadowError
				}
				modifiedShadow = true
			}

			// Apply inserts using generated pointers from background writer
			generatedPointers := result.Pointers[tableName]
			pointerIndex := 0

			// Inserts first
			for _, record := range insertsOrder[tableName] {
				ptr := generatedPointers[pointerIndex]
				pointerIndex++
				tableState.Insert(record, ptr)
				if shadowError := transaction.database.shadowInsertNoCommit(tableName, record, ptr, uint64(nextGenerationeration)); shadowError != nil {
					return shadowError
				}
				modifiedShadow = true
			}

			// Updates second
			for _, record := range updatesOrder[tableName] {
				ptr := generatedPointers[pointerIndex]
				pointerIndex++
				key := getPrimaryKey(record)
				oldRecord, error := transaction.readCommittedRecord(tableName, key)
				if error != nil {
					return error
				}
				tableState.Update(record, oldRecord, ptr)
				if shadowError := transaction.database.shadowDeleteNoCommit(tableName, key, oldRecord, uint64(nextGenerationeration)); shadowError != nil {
					return shadowError
				}
				if shadowError := transaction.database.shadowInsertNoCommit(tableName, record, ptr, uint64(nextGenerationeration)); shadowError != nil {
					return shadowError
				}
				modifiedShadow = true
			}

			if modifiedShadow {
				if shadowError := transaction.database.shadowCommit(tableName, uint64(nextGenerationeration)); shadowError != nil {
					return shadowError
				}
			}
		}

		transaction.database.publish(nextState)
	}

	return nil
}

func (transaction *Transaction) readCommittedRecord(tableName string, key any) (any, error) {
	tableState := transaction.committedState.Tables[tableName]
	if tableState == nil {
		return nil, nil
	}
	namedIndex, loadError := transaction.database.getShadowIndex(tableName)
	if loadError != nil {
		return nil, loadError
	}
	keyBytes, serializeError := serializeKey(key)
	if serializeError != nil {
		return nil, serializeError
	}
	recordPointer, found, findError := namedIndex.Find(keyBytes, uint64(transaction.committedState.Generation))
	if findError != nil || !found {
		return nil, nil
	}
	tableStorage, error := transaction.database.getTableStorage(tableName)
	if error != nil {
		return nil, error
	}
	bytesValue, error := tableStorage.ReadRecord(recordPointer)
	if error != nil {
		return nil, error
	}
	// deserialize
	newRecordValue := reflect.New(tableState.EntityType)
	error = Unmarshal(bytesValue, newRecordValue.Interface())
	if error != nil {
		return nil, error
	}
	return newRecordValue.Elem().Interface(), nil
}

func (transaction *Transaction) validateUniqueIndexes() error {
	for tableName, tableChangeSet := range transaction.changeSet.TableChanges {
		if tableChangeSet.IsEmpty() {
			continue
		}

		committedTable := transaction.committedState.Tables[tableName]
		if committedTable == nil {
			continue
		}

		for _, indexMetadata := range committedTable.IndexMetadataList {
			if !indexMetadata.Unique {
				continue
			}

			txAddedUnique := make(map[any]any)
			txRemovedUnique := make(map[any]struct{})

			if !tableChangeSet.Cleared {
				for key := range tableChangeSet.Deletes {
					oldRecord, error := transaction.readCommittedRecord(tableName, key)
					if error == nil && oldRecord != nil {
						indexValue := getFieldValue(oldRecord, indexMetadata.FieldName)
						if indexValue != nil {
							txRemovedUnique[canonicalizeKey(indexValue)] = struct{}{}
						}
					}
				}
				for key := range tableChangeSet.Updates {
					oldRecord, error := transaction.readCommittedRecord(tableName, key)
					if error == nil && oldRecord != nil {
						indexValue := getFieldValue(oldRecord, indexMetadata.FieldName)
						if indexValue != nil {
							txRemovedUnique[canonicalizeKey(indexValue)] = struct{}{}
						}
					}
				}
			}

			checkConflict := func(key, indexValue any) error {
				if indexValue == nil {
					return nil
				}
				indexValue = canonicalizeKey(indexValue)

				if existingKey, exists := txAddedUnique[indexValue]; exists {
					if existingKey != key {
						return &DuplicateIndexError{
							TableName: tableName,
							IndexName: indexMetadata.IndexName,
							Value:     indexValue,
							Message:   fmt.Sprintf("duplicate value '%v' in unique index '%s' on table '%s'", indexValue, indexMetadata.IndexName, tableName),
						}
					}
					return nil
				}

				if !tableChangeSet.Cleared {
					if _, removed := txRemovedUnique[indexValue]; !removed {
						namedIndex, loadError := transaction.database.getShadowNamedIndex(tableName, indexMetadata.IndexName)
						if loadError == nil {
							keyBytes, serializeError := serializeKey(indexValue)
							if serializeError == nil {
								recordPointer, exists, findError := namedIndex.Find(keyBytes, uint64(transaction.committedState.Generation))
								if findError == nil && exists {
									// Read record to find its primary key
									tableStorage, storageError := transaction.database.getTableStorage(tableName)
									if storageError == nil {
										bytesValue, readError := tableStorage.ReadRecord(recordPointer)
										if readError == nil {
											newRecordValue := reflect.New(committedTable.EntityType)
											if Unmarshal(bytesValue, newRecordValue.Interface()) == nil {
												existingKey := getPrimaryKey(newRecordValue.Elem().Interface())
												if existingKey != key {
													return &DuplicateIndexError{
														TableName: tableName,
														IndexName: indexMetadata.IndexName,
														Value:     indexValue,
														Message:   fmt.Sprintf("duplicate value '%v' in unique index '%s' on table '%s'", indexValue, indexMetadata.IndexName, tableName),
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}

				txAddedUnique[indexValue] = key
				return nil
			}

			for key, record := range tableChangeSet.Inserts {
				if error := checkConflict(key, getFieldValue(record, indexMetadata.FieldName)); error != nil {
					return error
				}
			}

			for key, record := range tableChangeSet.Updates {
				if error := checkConflict(key, getFieldValue(record, indexMetadata.FieldName)); error != nil {
					return error
				}
			}
		}
	}
	return nil
}

func (transaction *Transaction) InsertDynamic(tableName string, record any) error {
	if !transaction.active {
		return NotActiveTransactionError
	}

	primaryKeyValue := getPrimaryKey(record)
	if primaryKeyValue == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}

	tableChangeSet := transaction.changeSet.GetTableChanges(tableName)
	exists := false
	if _, found := tableChangeSet.Inserts[primaryKeyValue]; found {
		exists = true
	} else if _, found := tableChangeSet.Updates[primaryKeyValue]; found {
		exists = true
	} else if !tableChangeSet.Cleared {
		if _, found := tableChangeSet.Deletes[primaryKeyValue]; !found {
			tableState := transaction.committedState.Tables[tableName]
			if tableState != nil {
				namedIndex, loadError := transaction.database.getShadowIndex(tableName)
				if loadError == nil {
					keyBytes, serializeError := serializeKey(primaryKeyValue)
					if serializeError == nil {
						_, existsVal, findError := namedIndex.Find(keyBytes, uint64(transaction.committedState.Generation))
						if findError == nil && existsVal {
							exists = true
						}
					}
				}
			}
		}
	}

	if exists {
		return fmt.Errorf("record already exists in table '%s'", tableName)
	}

	tableChangeSet.Inserts[primaryKeyValue] = record

	// Index additions
	tableState := transaction.committedState.Tables[tableName]
	if tableState != nil {
		for _, indexMetadata := range tableState.IndexMetadataList {
			indexValue := getFieldValue(record, indexMetadata.FieldName)
			transaction.changeSet.IndexChanges.Add(tableName, indexMetadata.IndexName, indexValue, primaryKeyValue)
		}
	}

	return nil
}
