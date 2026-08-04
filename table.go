package masterkeeper

import (
	"fmt"
	"reflect"
)

type Table[ID comparable, T any] struct {
	tableName string
	database  *Database
}

func GetTable[ID comparable, T any](database *Database, tableName string) (*Table[ID, T], error) {
	if !isValidTableName(tableName) {
		return nil, ErrInvalidTableName
	}

	var sample T
	err := database.registerTableMetadata(tableName, reflect.TypeOf((*ID)(nil)).Elem(), reflect.TypeOf(sample))
	if err != nil {
		return nil, err
	}
	return &Table[ID, T]{
		tableName: tableName,
		database:  database,
	}, nil
}

func (table *Table[ID, T]) TableName() string {
	return table.tableName
}

func (table *Table[ID, T]) FindByID(transaction *Transaction, idValue ID) (T, bool, error) {
	var zero T
	if transaction != nil {
		if !transaction.active {
			return zero, false, ErrTransactionNotActive
		}

		changeSet := transaction.changeSet.GetTableChanges(table.tableName)
		if changeSet.Cleared {
			if record, found := changeSet.Inserts[idValue]; found {
				return record.(T), true, nil
			}
			return zero, false, nil
		}

		if _, deleted := changeSet.Deletes[idValue]; deleted {
			return zero, false, nil
		}

		if record, found := changeSet.Inserts[idValue]; found {
			return record.(T), true, nil
		}

		if record, found := changeSet.Updates[idValue]; found {
			return record.(T), true, nil
		}

		// Read committed
		tableState := transaction.committedState.Tables[table.tableName]
		if tableState != nil {
			recordPointer, exists := tableState.RecordPointers[idValue]
			if exists {
				record, err := table.readFromStorage(recordPointer)
				if err != nil {
					return zero, false, err
				}
				return record, true, nil
			}
		}
		return zero, false, nil
	} else {
		// Read directly from current committed state
		committed := table.database.getCommittedState()
		tableState := committed.Tables[table.tableName]
		if tableState != nil {
			recordPointer, exists := tableState.RecordPointers[idValue]
			if exists {
				record, err := table.readFromStorage(recordPointer)
				if err != nil {
					return zero, false, err
				}
				return record, true, nil
			}
		}
		return zero, false, nil
	}
}

func (table *Table[ID, T]) readFromStorage(recordPointer RecordPointer) (T, error) {
	var zero T
	tableStorage, err := table.database.getTableStorage(table.tableName)
	if err != nil {
		return zero, err
	}
	bytesValue, err := tableStorage.ReadRecord(recordPointer)
	if err != nil {
		return zero, err
	}

	targetValue := reflect.New(reflect.TypeOf(zero))
	err = Unmarshal(bytesValue, targetValue.Interface())
	if err != nil {
		return zero, err
	}
	return targetValue.Elem().Interface().(T), nil
}

func (table *Table[ID, T]) Insert(transaction *Transaction, record T) error {
	if transaction != nil {
		return table.insert(transaction, record)
	}
	return table.database.Transaction(func(txVal *Transaction) error {
		return table.insert(txVal, record)
	})
}

func (table *Table[ID, T]) insert(transaction *Transaction, record T) error {
	if !transaction.active {
		return ErrTransactionNotActive
	}

	primaryKeyVal := getPrimaryKey(record)
	if primaryKeyVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}
	primaryKey := primaryKeyVal.(ID)

	changeSet := transaction.changeSet.GetTableChanges(table.tableName)

	// Check if exists in changeset or committed
	exists := false
	if _, found := changeSet.Inserts[primaryKey]; found {
		exists = true
	} else if _, found := changeSet.Updates[primaryKey]; found {
		exists = true
	} else if !changeSet.Cleared {
		if _, deleted := changeSet.Deletes[primaryKey]; !deleted {
			tableState := transaction.committedState.Tables[table.tableName]
			if tableState != nil {
				if _, found := tableState.RecordPointers[primaryKey]; found {
					exists = true
				}
			}
		}
	}

	if exists {
		return fmt.Errorf("record with ID '%v' already exists in table '%s'", primaryKey, table.tableName)
	}

	changeSet.Inserts[primaryKey] = record

	// Index additions
	tableState := transaction.committedState.Tables[table.tableName]
	if tableState != nil {
		for _, indexState := range tableState.Indexes {
			indexValue := getFieldValue(record, indexState.Metadata.FieldName)
			transaction.changeSet.IndexChanges.Add(table.tableName, indexState.Metadata.IndexName, indexValue, primaryKey)
		}
	}

	return nil
}

func (table *Table[ID, T]) Update(transaction *Transaction, record T) error {
	if transaction != nil {
		return table.update(transaction, record)
	}
	return table.database.Transaction(func(txVal *Transaction) error {
		return table.update(txVal, record)
	})
}

func (table *Table[ID, T]) update(transaction *Transaction, record T) error {
	if !transaction.active {
		return ErrTransactionNotActive
	}

	primaryKeyVal := getPrimaryKey(record)
	if primaryKeyVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}
	primaryKey := primaryKeyVal.(ID)

	changeSet := transaction.changeSet.GetTableChanges(table.tableName)

	var oldRecord T
	var foundRecord bool

	if insertedRecord, found := changeSet.Inserts[primaryKey]; found {
		oldRecord = insertedRecord.(T)
		foundRecord = true
	} else if updatedRecord, found := changeSet.Updates[primaryKey]; found {
		oldRecord = updatedRecord.(T)
		foundRecord = true
	} else if !changeSet.Cleared {
		if _, deleted := changeSet.Deletes[primaryKey]; !deleted {
			tableState := transaction.committedState.Tables[table.tableName]
			if tableState != nil {
				recordPointer, exists := tableState.RecordPointers[primaryKey]
				if exists {
					rRecord, err := table.readFromStorage(recordPointer)
					if err != nil {
						return err
					}
					oldRecord = rRecord
					foundRecord = true
				}
			}
		}
	}

	if !foundRecord {
		return fmt.Errorf("record with ID '%v' does not exist in table '%s'", primaryKey, table.tableName)
	}

	if _, found := changeSet.Inserts[primaryKey]; found {
		changeSet.Inserts[primaryKey] = record
	} else {
		changeSet.Updates[primaryKey] = record
	}

	// Update index changes
	tableState := transaction.committedState.Tables[table.tableName]
	if tableState != nil {
		for _, indexState := range tableState.Indexes {
			oldIndexValue := getFieldValue(oldRecord, indexState.Metadata.FieldName)
			newIndexValue := getFieldValue(record, indexState.Metadata.FieldName)
			if !valuesEqual(oldIndexValue, newIndexValue) {
				if oldIndexValue != nil {
					transaction.changeSet.IndexChanges.Remove(table.tableName, indexState.Metadata.IndexName, oldIndexValue, primaryKey)
				}
				transaction.changeSet.IndexChanges.Add(table.tableName, indexState.Metadata.IndexName, newIndexValue, primaryKey)
			}
		}
	}

	return nil
}

func (table *Table[ID, T]) Upsert(transaction *Transaction, record T) error {
	if transaction != nil {
		return table.upsert(transaction, record)
	}
	return table.database.Transaction(func(txVal *Transaction) error {
		return table.upsert(txVal, record)
	})
}

func (table *Table[ID, T]) upsert(transaction *Transaction, record T) error {
	primaryKeyVal := getPrimaryKey(record)
	if primaryKeyVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}
	primaryKey := primaryKeyVal.(ID)

	changeSet := transaction.changeSet.GetTableChanges(table.tableName)
	exists := false

	if _, found := changeSet.Inserts[primaryKey]; found {
		exists = true
	} else if _, found := changeSet.Updates[primaryKey]; found {
		exists = true
	} else if !changeSet.Cleared {
		if _, deleted := changeSet.Deletes[primaryKey]; !deleted {
			tableState := transaction.committedState.Tables[table.tableName]
			if tableState != nil {
				if _, found := tableState.RecordPointers[primaryKey]; found {
					exists = true
				}
			}
		}
	}

	if exists {
		return table.update(transaction, record)
	}
	return table.insert(transaction, record)
}

func (table *Table[ID, T]) DeleteByID(transaction *Transaction, idValue ID) (bool, error) {
	if transaction != nil {
		return table.deleteByID(transaction, idValue)
	}
	var deleted bool
	err := table.database.Transaction(func(txVal *Transaction) error {
		var deleteErr error
		deleted, deleteErr = table.deleteByID(txVal, idValue)
		return deleteErr
	})
	return deleted, err
}

func (table *Table[ID, T]) deleteByID(transaction *Transaction, idValue ID) (bool, error) {
	if !transaction.active {
		return false, ErrTransactionNotActive
	}

	changeSet := transaction.changeSet.GetTableChanges(table.tableName)

	var oldRecord T
	var foundRecord bool

	if insertedRecord, found := changeSet.Inserts[idValue]; found {
		oldRecord = insertedRecord.(T)
		foundRecord = true
	} else if updatedRecord, found := changeSet.Updates[idValue]; found {
		oldRecord = updatedRecord.(T)
		foundRecord = true
	} else if !changeSet.Cleared {
		if _, deleted := changeSet.Deletes[idValue]; !deleted {
			tableState := transaction.committedState.Tables[table.tableName]
			if tableState != nil {
				recordPointer, exists := tableState.RecordPointers[idValue]
				if exists {
					rRecord, err := table.readFromStorage(recordPointer)
					if err != nil {
						return false, err
					}
					oldRecord = rRecord
					foundRecord = true
				}
			}
		}
	}

	if !foundRecord {
		return false, nil
	}

	if _, found := changeSet.Inserts[idValue]; found {
		delete(changeSet.Inserts, idValue)
	} else {
		delete(changeSet.Updates, idValue)
		changeSet.Deletes[idValue] = struct{}{}
	}

	// Index removals
	tableState := transaction.committedState.Tables[table.tableName]
	if tableState != nil {
		for _, indexState := range tableState.Indexes {
			indexValue := getFieldValue(oldRecord, indexState.Metadata.FieldName)
			if indexValue != nil {
				transaction.changeSet.IndexChanges.Remove(table.tableName, indexState.Metadata.IndexName, indexValue, idValue)
			}
		}
	}

	return true, nil
}

func (table *Table[ID, T]) Clear(transaction *Transaction) error {
	if transaction != nil {
		return table.clear(transaction)
	}
	return table.database.Transaction(func(txVal *Transaction) error {
		return table.clear(txVal)
	})
}

func (table *Table[ID, T]) clear(transaction *Transaction) error {
	if !transaction.active {
		return ErrTransactionNotActive
	}

	changeSet := transaction.changeSet.GetTableChanges(table.tableName)
	changeSet.Cleared = true
	changeSet.Inserts = make(map[any]any)
	changeSet.Updates = make(map[any]any)
	changeSet.Deletes = make(map[any]struct{})

	var remainingAdded []IndexChange
	for _, addedIndexChange := range transaction.changeSet.IndexChanges.Added {
		if addedIndexChange.TableName != table.tableName {
			remainingAdded = append(remainingAdded, addedIndexChange)
		}
	}
	transaction.changeSet.IndexChanges.Added = remainingAdded

	var remainingRemoved []IndexChange
	for _, removedIndexChange := range transaction.changeSet.IndexChanges.Removed {
		if removedIndexChange.TableName != table.tableName {
			remainingRemoved = append(remainingRemoved, removedIndexChange)
		}
	}
	transaction.changeSet.IndexChanges.Removed = remainingRemoved

	return nil
}

func (table *Table[ID, T]) Query(transaction *Transaction) *Query[T] {
	if transaction != nil {
		committedTableState := transaction.committedState.Tables[table.tableName]
		stagedTableChanges := transaction.changeSet.GetTableChanges(table.tableName)
		return NewQuery[T](table.tableName, table.database, committedTableState, stagedTableChanges)
	}
	committedTableState := table.database.getCommittedState().Tables[table.tableName]
	return NewQuery[T](table.tableName, table.database, committedTableState, nil)
}
