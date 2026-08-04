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
	IndexVal   any
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
		IndexVal:   indexValue,
		PrimaryKey: primaryKey,
	})
}

func (indexChangeSet *IndexChangeSet) Remove(tableName, indexName string, indexValue, primaryKey any) {
	indexChangeSet.Removed = append(indexChangeSet.Removed, IndexChange{
		TableName:  tableName,
		IndexName:  indexName,
		IndexVal:   indexValue,
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

	nextGen := transaction.database.currentGeneration() + 1
	err := transaction.database.walManager.AppendRollbackMarker(transaction.transactionID, nextGen, transaction.changeSet.RollbackReason)
	if err != nil {
		return fmt.Errorf("failed to write rollback marker to WAL: %w", err)
	}

	return nil
}

func (transaction *Transaction) Commit() error {
	if !transaction.active {
		return ErrTransactionNotActive
	}
	if transaction.changeSet.RollbackOnly {
		_ = transaction.Rollback()
		return fmt.Errorf("%w: %s", ErrRollbackOnly, transaction.changeSet.RollbackReason)
	}

	transaction.active = false
	defer transaction.database.releaseWriterLock()

	// 1. Validate unique indexes
	if err := transaction.validateUniqueIndexes(); err != nil {
		_ = transaction.Rollback()
		return err
	}

	nextGen := transaction.database.currentGeneration() + 1

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
				Generation:    nextGen,
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
				Generation:    nextGen,
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
			recBytes, err := Marshal(record)
			if err != nil {
				return fmt.Errorf("failed to serialize record for commit: %w", err)
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
				Generation:    nextGen,
				Payload:       buf.Bytes(),
			})
		}

		// Updates log and preparation for table appends
		for _, record := range updatesList {
			recBytes, err := Marshal(record)
			if err != nil {
				return fmt.Errorf("failed to serialize record for commit: %w", err)
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
				Generation:    nextGen,
				Payload:       buf.Bytes(),
			})
		}
	}

	// 3. Submit write task to background writer goroutine and wait
	if len(walRecords) > 0 || len(tableAppends) > 0 {
		doneChan := make(chan WriteResult, 1)
		task := &WriteTask{
			TxID:         transaction.transactionID,
			Generation:   nextGen,
			WalRecords:   walRecords,
			TableAppends: tableAppends,
			Done:         doneChan,
		}

		transaction.database.walManager.Submit(task)
		result := <-doneChan
		if result.Err != nil {
			return fmt.Errorf("background write failure: %w", result.Err)
		}

		// 4. Update DatabaseState in memory
		nextState := transaction.committedState.Copy(nextGen)
		for tableName, tableChangeSet := range transaction.changeSet.TableChanges {
			tableState := nextState.Tables[tableName]
			if tableState == nil {
				continue
			}

			if tableChangeSet.Cleared {
				tableState.Clear()
			}

			// Apply deletes
			for key := range tableChangeSet.Deletes {
				oldRecord, err := transaction.readCommittedRecord(tableName, key)
				if err != nil {
					return err
				}
				tableState.Delete(key, oldRecord)
			}

			// Apply inserts using generated pointers from background writer
			generatedPointers := result.Pointers[tableName]
			pointerIdx := 0

			// Inserts first
			for _, record := range insertsOrder[tableName] {
				ptr := generatedPointers[pointerIdx]
				pointerIdx++
				tableState.Insert(record, ptr)
			}

			// Updates second
			for _, record := range updatesOrder[tableName] {
				ptr := generatedPointers[pointerIdx]
				pointerIdx++
				key := getPrimaryKey(record)
				oldRecord, err := transaction.readCommittedRecord(tableName, key)
				if err != nil {
					return err
				}
				tableState.Update(record, oldRecord, ptr)
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
	recordPointer, found := tableState.RecordPointers[key]
	if !found {
		return nil, nil
	}
	tableStorage, err := transaction.database.getTableStorage(tableName)
	if err != nil {
		return nil, err
	}
	bytesValue, err := tableStorage.ReadRecord(recordPointer)
	if err != nil {
		return nil, err
	}
	// deserialize
	newRecordVal := reflect.New(tableState.EntityType)
	err = Unmarshal(bytesValue, newRecordVal.Interface())
	if err != nil {
		return nil, err
	}
	return newRecordVal.Elem().Interface(), nil
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

		for _, committedIndex := range committedTable.Indexes {
			if !committedIndex.Metadata.Unique {
				continue
			}

			indexMetadata := committedIndex.Metadata
			txAddedUnique := make(map[any]any)
			txRemovedUnique := make(map[any]struct{})

			if !tableChangeSet.Cleared {
				for key := range tableChangeSet.Deletes {
					oldRecord, err := transaction.readCommittedRecord(tableName, key)
					if err == nil && oldRecord != nil {
						indexValue := getFieldValue(oldRecord, indexMetadata.FieldName)
						if indexValue != nil {
							txRemovedUnique[indexValue] = struct{}{}
						}
					}
				}
				for key := range tableChangeSet.Updates {
					oldRecord, err := transaction.readCommittedRecord(tableName, key)
					if err == nil && oldRecord != nil {
						indexValue := getFieldValue(oldRecord, indexMetadata.FieldName)
						if indexValue != nil {
							txRemovedUnique[indexValue] = struct{}{}
						}
					}
				}
			}

			checkConflict := func(key, indexValue any) error {
				if indexValue == nil {
					return nil
				}

				if existingKey, exists := txAddedUnique[indexValue]; exists {
					if existingKey != key {
						return &ErrDuplicateIndex{
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
						if existingKey, exists := committedIndex.UniqueMap[indexValue]; exists {
							if existingKey != key {
								return &ErrDuplicateIndex{
									TableName: tableName,
									IndexName: indexMetadata.IndexName,
									Value:     indexValue,
									Message:   fmt.Sprintf("duplicate value '%v' in unique index '%s' on table '%s'", indexValue, indexMetadata.IndexName, tableName),
								}
							}
						}
					}
				}

				txAddedUnique[indexValue] = key
				return nil
			}

			for key, record := range tableChangeSet.Inserts {
				if err := checkConflict(key, getFieldValue(record, indexMetadata.FieldName)); err != nil {
					return err
				}
			}

			for key, record := range tableChangeSet.Updates {
				if err := checkConflict(key, getFieldValue(record, indexMetadata.FieldName)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (transaction *Transaction) InsertDynamic(tableName string, record any) error {
	if !transaction.active {
		return ErrTransactionNotActive
	}

	primaryKeyVal := getPrimaryKey(record)
	if primaryKeyVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}

	tableChangeSet := transaction.changeSet.GetTableChanges(tableName)
	exists := false
	if _, found := tableChangeSet.Inserts[primaryKeyVal]; found {
		exists = true
	} else if _, found := tableChangeSet.Updates[primaryKeyVal]; found {
		exists = true
	} else if !tableChangeSet.Cleared {
		if _, found := tableChangeSet.Deletes[primaryKeyVal]; !found {
			tableState := transaction.committedState.Tables[tableName]
			if tableState != nil {
				if _, found := tableState.RecordPointers[primaryKeyVal]; found {
					exists = true
				}
			}
		}
	}

	if exists {
		return fmt.Errorf("record already exists in table '%s'", tableName)
	}

	tableChangeSet.Inserts[primaryKeyVal] = record

	// Index additions
	tableState := transaction.committedState.Tables[tableName]
	if tableState != nil {
		for _, indexState := range tableState.Indexes {
			indexValue := getFieldValue(record, indexState.Metadata.FieldName)
			transaction.changeSet.IndexChanges.Add(tableName, indexState.Metadata.IndexName, indexValue, primaryKeyVal)
		}
	}

	return nil
}
