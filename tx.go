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

func (tcs *TableChangeSet) IsEmpty() bool {
	return !tcs.Cleared && len(tcs.Inserts) == 0 && len(tcs.Updates) == 0 && len(tcs.Deletes) == 0
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

func (ics *IndexChangeSet) Add(tableName, indexName string, indexVal, primaryKey any) {
	ics.Added = append(ics.Added, IndexChange{
		TableName:  tableName,
		IndexName:  indexName,
		IndexVal:   indexVal,
		PrimaryKey: primaryKey,
	})
}

func (ics *IndexChangeSet) Remove(tableName, indexName string, indexVal, primaryKey any) {
	ics.Removed = append(ics.Removed, IndexChange{
		TableName:  tableName,
		IndexName:  indexName,
		IndexVal:   indexVal,
		PrimaryKey: primaryKey,
	})
}

func (ics *IndexChangeSet) Clear() {
	ics.Added = nil
	ics.Removed = nil
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

func (tcs *TransactionChangeSet) GetTableChanges(tableName string) *TableChangeSet {
	cs, ok := tcs.TableChanges[tableName]
	if !ok {
		cs = NewTableChangeSet()
		tcs.TableChanges[tableName] = cs
	}
	return cs
}

type Transaction struct {
	txID           int64
	db             *Database
	committedState *DatabaseState
	changeSet      *TransactionChangeSet
	active         bool
}

func NewTransaction(txID int64, db *Database, committedState *DatabaseState) *Transaction {
	return &Transaction{
		txID:           txID,
		db:             db,
		committedState: committedState,
		changeSet:      NewTransactionChangeSet(),
		active:         true,
	}
}

func (tx *Transaction) TransactionID() int64 {
	return tx.txID
}

func (tx *Transaction) IsActive() bool {
	return tx.active
}

func (tx *Transaction) SetRollbackOnly(reason string) {
	tx.changeSet.RollbackOnly = true
	tx.changeSet.RollbackReason = reason
}

func (tx *Transaction) IsRollbackOnly() bool {
	return tx.changeSet.RollbackOnly
}

func (tx *Transaction) RollbackReason() string {
	return tx.changeSet.RollbackReason
}

func (tx *Transaction) Rollback() error {
	if !tx.active {
		return nil // Idempotent
	}

	tx.active = false
	defer tx.db.releaseWriterLock()

	nextGen := tx.db.currentGeneration() + 1
	err := tx.db.walManager.AppendRollbackMarker(tx.txID, nextGen, tx.changeSet.RollbackReason)
	if err != nil {
		return fmt.Errorf("failed to write rollback marker to WAL: %w", err)
	}

	return nil
}

func (tx *Transaction) Commit() error {
	if !tx.active {
		return ErrTransactionNotActive
	}
	if tx.changeSet.RollbackOnly {
		_ = tx.Rollback()
		return fmt.Errorf("%w: %s", ErrRollbackOnly, tx.changeSet.RollbackReason)
	}

	tx.active = false
	defer tx.db.releaseWriterLock()

	// 1. Validate unique indexes
	if err := tx.validateUniqueIndexes(); err != nil {
		_ = tx.Rollback()
		return err
	}

	nextGen := tx.db.currentGeneration() + 1

	// 2. Prepare WAL records and Table appends
	var walRecords []WalRecord
	tableAppends := make(map[string][][]byte)

	insertsOrder := make(map[string][]any)
	updatesOrder := make(map[string][]any)

	for tableName, tcs := range tx.changeSet.TableChanges {
		if tcs.IsEmpty() {
			continue
		}

		committedTable := tx.committedState.Tables[tableName]
		var entityClassName string
		var idClassName string
		if committedTable != nil {
			entityClassName = committedTable.EntityType.String()
			idClassName = committedTable.IdType.String()
		}

		// Table clear log
		if tcs.Cleared {
			var buf bytes.Buffer
			_ = writeString(&buf, tableName)
			walRecords = append(walRecords, WalRecord{
				Type:          OpClearTable,
				TransactionID: tx.txID,
				Generation:    nextGen,
				Payload:       buf.Bytes(),
			})
		}

		// Deletes log
		for key := range tcs.Deletes {
			var buf bytes.Buffer
			_ = writeString(&buf, tableName)
			_ = writeString(&buf, idClassName)

			// serialize the primary key
			var keyBuf bytes.Buffer
			_ = writeValue(&keyBuf, reflect.ValueOf(key))
			_, _ = buf.Write(keyBuf.Bytes())

			walRecords = append(walRecords, WalRecord{
				Type:          OpDelete,
				TransactionID: tx.txID,
				Generation:    nextGen,
				Payload:       buf.Bytes(),
			})
		}

		// Collect inserts and updates to slices to preserve deterministic order
		var insertsList []any
		for _, record := range tcs.Inserts {
			insertsList = append(insertsList, record)
		}
		insertsOrder[tableName] = insertsList

		var updatesList []any
		for _, record := range tcs.Updates {
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
				TransactionID: tx.txID,
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
				TransactionID: tx.txID,
				Generation:    nextGen,
				Payload:       buf.Bytes(),
			})
		}
	}

	// 3. Submit write task to background writer goroutine and wait
	if len(walRecords) > 0 || len(tableAppends) > 0 {
		doneChan := make(chan WriteResult, 1)
		task := &WriteTask{
			TxID:         tx.txID,
			Generation:   nextGen,
			WalRecords:   walRecords,
			TableAppends: tableAppends,
			Done:         doneChan,
		}

		tx.db.walManager.Submit(task)
		result := <-doneChan
		if result.Err != nil {
			return fmt.Errorf("background write failure: %w", result.Err)
		}

		// 4. Update DatabaseState in memory
		nextState := tx.committedState.Copy(nextGen)
		for tableName, tcs := range tx.changeSet.TableChanges {
			ts := nextState.Tables[tableName]
			if ts == nil {
				continue
			}

			if tcs.Cleared {
				ts.Clear()
			}

			// Apply deletes
			for key := range tcs.Deletes {
				oldRecord, err := tx.readCommittedRecord(tableName, key)
				if err != nil {
					return err
				}
				ts.Delete(key, oldRecord)
			}

			// Apply inserts using generated pointers from background writer
			generatedPointers := result.Pointers[tableName]
			pointerIdx := 0

			// Inserts first
			for _, record := range insertsOrder[tableName] {
				ptr := generatedPointers[pointerIdx]
				pointerIdx++
				ts.Insert(record, ptr)
			}

			// Updates second
			for _, record := range updatesOrder[tableName] {
				ptr := generatedPointers[pointerIdx]
				pointerIdx++
				key := getPrimaryKey(record)
				oldRecord, err := tx.readCommittedRecord(tableName, key)
				if err != nil {
					return err
				}
				ts.Update(record, oldRecord, ptr)
			}
		}

		tx.db.publish(nextState)
	}

	return nil
}

func (tx *Transaction) readCommittedRecord(tableName string, key any) (any, error) {
	ts := tx.committedState.Tables[tableName]
	if ts == nil {
		return nil, nil
	}
	ptr, ok := ts.RecordPointers[key]
	if !ok {
		return nil, nil
	}
	storage, err := tx.db.getTableStorage(tableName)
	if err != nil {
		return nil, err
	}
	bytes, err := storage.ReadRecord(ptr)
	if err != nil {
		return nil, err
	}
	// deserialize
	newRecordVal := reflect.New(ts.EntityType)
	err = Unmarshal(bytes, newRecordVal.Interface())
	if err != nil {
		return nil, err
	}
	return newRecordVal.Elem().Interface(), nil
}

func (tx *Transaction) validateUniqueIndexes() error {
	for tableName, tcs := range tx.changeSet.TableChanges {
		if tcs.IsEmpty() {
			continue
		}

		committedTable := tx.committedState.Tables[tableName]
		if committedTable == nil {
			continue
		}

		for _, committedIndex := range committedTable.Indexes {
			if !committedIndex.Metadata.Unique {
				continue
			}

			meta := committedIndex.Metadata
			txAddedUnique := make(map[any]any)
			txRemovedUnique := make(map[any]struct{})

			if !tcs.Cleared {
				for key := range tcs.Deletes {
					oldRecord, err := tx.readCommittedRecord(tableName, key)
					if err == nil && oldRecord != nil {
						idxVal := getFieldValue(oldRecord, meta.FieldName)
						if idxVal != nil {
							txRemovedUnique[idxVal] = struct{}{}
						}
					}
				}
				for key := range tcs.Updates {
					oldRecord, err := tx.readCommittedRecord(tableName, key)
					if err == nil && oldRecord != nil {
						idxVal := getFieldValue(oldRecord, meta.FieldName)
						if idxVal != nil {
							txRemovedUnique[idxVal] = struct{}{}
						}
					}
				}
			}

			checkConflict := func(key, idxVal any) error {
				if idxVal == nil {
					return nil
				}

				if existingKey, exists := txAddedUnique[idxVal]; exists {
					if existingKey != key {
						return &ErrDuplicateIndex{
							TableName: tableName,
							IndexName: meta.IndexName,
							Value:     idxVal,
							Message:   fmt.Sprintf("duplicate value '%v' in unique index '%s' on table '%s'", idxVal, meta.IndexName, tableName),
						}
					}
					return nil
				}

				if !tcs.Cleared {
					if _, removed := txRemovedUnique[idxVal]; !removed {
						if existingKey, exists := committedIndex.UniqueMap[idxVal]; exists {
							if existingKey != key {
								return &ErrDuplicateIndex{
									TableName: tableName,
									IndexName: meta.IndexName,
									Value:     idxVal,
									Message:   fmt.Sprintf("duplicate value '%v' in unique index '%s' on table '%s'", idxVal, meta.IndexName, tableName),
								}
							}
						}
					}
				}

				txAddedUnique[idxVal] = key
				return nil
			}

			for key, record := range tcs.Inserts {
				if err := checkConflict(key, getFieldValue(record, meta.FieldName)); err != nil {
					return err
				}
			}

			for key, record := range tcs.Updates {
				if err := checkConflict(key, getFieldValue(record, meta.FieldName)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (tx *Transaction) InsertDynamic(tableName string, record any) error {
	if !tx.active {
		return ErrTransactionNotActive
	}

	idVal := getPrimaryKey(record)
	if idVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}

	cs := tx.changeSet.GetTableChanges(tableName)
	exists := false
	if _, ok := cs.Inserts[idVal]; ok {
		exists = true
	} else if _, ok := cs.Updates[idVal]; ok {
		exists = true
	} else if !cs.Cleared {
		if _, ok := cs.Deletes[idVal]; !ok {
			ts := tx.committedState.Tables[tableName]
			if ts != nil {
				if _, ok := ts.RecordPointers[idVal]; ok {
					exists = true
				}
			}
		}
	}

	if exists {
		return fmt.Errorf("record already exists in table '%s'", tableName)
	}

	cs.Inserts[idVal] = record

	// Index additions
	ts := tx.committedState.Tables[tableName]
	if ts != nil {
		for _, idx := range ts.Indexes {
			idxVal := getFieldValue(record, idx.Metadata.FieldName)
			tx.changeSet.IndexChanges.Add(tableName, idx.Metadata.IndexName, idxVal, idVal)
		}
	}

	return nil
}
