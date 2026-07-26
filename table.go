package masterkeeper

import (
	"fmt"
	"reflect"
)

type Table[ID comparable, T any] struct {
	tableName string
	db        *Database
}

func GetTable[ID comparable, T any](db *Database, tableName string) (*Table[ID, T], error) {
	var sample T
	err := db.registerTableMetadata(tableName, reflect.TypeOf((*ID)(nil)).Elem(), reflect.TypeOf(sample))
	if err != nil {
		return nil, err
	}
	return &Table[ID, T]{
		tableName: tableName,
		db:        db,
	}, nil
}

func (t *Table[ID, T]) TableName() string {
	return t.tableName
}

func (t *Table[ID, T]) FindByID(tx *Transaction, id ID) (T, bool, error) {
	var zero T
	if tx != nil {
		if !tx.active {
			return zero, false, ErrTransactionNotActive
		}

		cs := tx.changeSet.GetTableChanges(t.tableName)
		if cs.Cleared {
			if rec, ok := cs.Inserts[id]; ok {
				return rec.(T), true, nil
			}
			return zero, false, nil
		}

		if _, deleted := cs.Deletes[id]; deleted {
			return zero, false, nil
		}

		if rec, ok := cs.Inserts[id]; ok {
			return rec.(T), true, nil
		}

		if rec, ok := cs.Updates[id]; ok {
			return rec.(T), true, nil
		}

		// Read committed
		ts := tx.committedState.Tables[t.tableName]
		if ts != nil {
			ptr, exists := ts.RecordPointers[id]
			if exists {
				rec, err := t.readFromStorage(ptr)
				if err != nil {
					return zero, false, err
				}
				return rec, true, nil
			}
		}
		return zero, false, nil
	} else {
		// Read directly from current committed state
		committed := t.db.getCommittedState()
		ts := committed.Tables[t.tableName]
		if ts != nil {
			ptr, exists := ts.RecordPointers[id]
			if exists {
				rec, err := t.readFromStorage(ptr)
				if err != nil {
					return zero, false, err
				}
				return rec, true, nil
			}
		}
		return zero, false, nil
	}
}

func (t *Table[ID, T]) readFromStorage(ptr RecordPointer) (T, error) {
	var zero T
	storage, err := t.db.getTableStorage(t.tableName)
	if err != nil {
		return zero, err
	}
	bytes, err := storage.ReadRecord(ptr)
	if err != nil {
		return zero, err
	}

	target := reflect.New(reflect.TypeOf(zero))
	err = Unmarshal(bytes, target.Interface())
	if err != nil {
		return zero, err
	}
	return target.Elem().Interface().(T), nil
}

func (t *Table[ID, T]) Insert(tx *Transaction, record T) error {
	if tx != nil {
		return t.insert(tx, record)
	}
	return t.db.Transaction(func(tx *Transaction) error {
		return t.insert(tx, record)
	})
}

func (t *Table[ID, T]) insert(tx *Transaction, record T) error {
	if !tx.active {
		return ErrTransactionNotActive
	}

	idVal := getPrimaryKey(record)
	if idVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}
	id := idVal.(ID)

	cs := tx.changeSet.GetTableChanges(t.tableName)

	// Check if exists in changeset or committed
	exists := false
	if _, ok := cs.Inserts[id]; ok {
		exists = true
	} else if _, ok := cs.Updates[id]; ok {
		exists = true
	} else if !cs.Cleared {
		if _, ok := cs.Deletes[id]; !ok {
			ts := tx.committedState.Tables[t.tableName]
			if ts != nil {
				if _, ok := ts.RecordPointers[id]; ok {
					exists = true
				}
			}
		}
	}

	if exists {
		return fmt.Errorf("record with ID '%v' already exists in table '%s'", id, t.tableName)
	}

	cs.Inserts[id] = record

	// Index additions
	ts := tx.committedState.Tables[t.tableName]
	if ts != nil {
		for _, idx := range ts.Indexes {
			idxVal := getFieldValue(record, idx.Metadata.FieldName)
			tx.changeSet.IndexChanges.Add(t.tableName, idx.Metadata.IndexName, idxVal, id)
		}
	}

	return nil
}

func (t *Table[ID, T]) Update(tx *Transaction, record T) error {
	if tx != nil {
		return t.update(tx, record)
	}
	return t.db.Transaction(func(tx *Transaction) error {
		return t.update(tx, record)
	})
}

func (t *Table[ID, T]) update(tx *Transaction, record T) error {
	if !tx.active {
		return ErrTransactionNotActive
	}

	idVal := getPrimaryKey(record)
	if idVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}
	id := idVal.(ID)

	cs := tx.changeSet.GetTableChanges(t.tableName)

	var oldRecord T
	var found bool

	if r, ok := cs.Inserts[id]; ok {
		oldRecord = r.(T)
		found = true
	} else if r, ok := cs.Updates[id]; ok {
		oldRecord = r.(T)
		found = true
	} else if !cs.Cleared {
		if _, deleted := cs.Deletes[id]; !deleted {
			ts := tx.committedState.Tables[t.tableName]
			if ts != nil {
				ptr, exists := ts.RecordPointers[id]
				if exists {
					r, err := t.readFromStorage(ptr)
					if err != nil {
						return err
					}
					oldRecord = r
					found = true
				}
			}
		}
	}

	if !found {
		return fmt.Errorf("record with ID '%v' does not exist in table '%s'", id, t.tableName)
	}

	if _, ok := cs.Inserts[id]; ok {
		cs.Inserts[id] = record
	} else {
		cs.Updates[id] = record
	}

	// Update index changes
	ts := tx.committedState.Tables[t.tableName]
	if ts != nil {
		for _, idx := range ts.Indexes {
			oldIdxVal := getFieldValue(oldRecord, idx.Metadata.FieldName)
			newIdxVal := getFieldValue(record, idx.Metadata.FieldName)
			if !valuesEqual(oldIdxVal, newIdxVal) {
				if oldIdxVal != nil {
					tx.changeSet.IndexChanges.Remove(t.tableName, idx.Metadata.IndexName, oldIdxVal, id)
				}
				tx.changeSet.IndexChanges.Add(t.tableName, idx.Metadata.IndexName, newIdxVal, id)
			}
		}
	}

	return nil
}

func (t *Table[ID, T]) Upsert(tx *Transaction, record T) error {
	if tx != nil {
		return t.upsert(tx, record)
	}
	return t.db.Transaction(func(tx *Transaction) error {
		return t.upsert(tx, record)
	})
}

func (t *Table[ID, T]) upsert(tx *Transaction, record T) error {
	idVal := getPrimaryKey(record)
	if idVal == nil {
		return fmt.Errorf("primary key ID cannot be nil")
	}
	id := idVal.(ID)

	cs := tx.changeSet.GetTableChanges(t.tableName)
	exists := false

	if _, ok := cs.Inserts[id]; ok {
		exists = true
	} else if _, ok := cs.Updates[id]; ok {
		exists = true
	} else if !cs.Cleared {
		if _, deleted := cs.Deletes[id]; !deleted {
			ts := tx.committedState.Tables[t.tableName]
			if ts != nil {
				if _, ok := ts.RecordPointers[id]; ok {
					exists = true
				}
			}
		}
	}

	if exists {
		return t.update(tx, record)
	}
	return t.insert(tx, record)
}

func (t *Table[ID, T]) DeleteByID(tx *Transaction, id ID) (bool, error) {
	if tx != nil {
		return t.deleteByID(tx, id)
	}
	var deleted bool
	err := t.db.Transaction(func(tx *Transaction) error {
		var dErr error
		deleted, dErr = t.deleteByID(tx, id)
		return dErr
	})
	return deleted, err
}

func (t *Table[ID, T]) deleteByID(tx *Transaction, id ID) (bool, error) {
	if !tx.active {
		return false, ErrTransactionNotActive
	}

	cs := tx.changeSet.GetTableChanges(t.tableName)

	var oldRecord T
	var found bool

	if r, ok := cs.Inserts[id]; ok {
		oldRecord = r.(T)
		found = true
	} else if r, ok := cs.Updates[id]; ok {
		oldRecord = r.(T)
		found = true
	} else if !cs.Cleared {
		if _, deleted := cs.Deletes[id]; !deleted {
			ts := tx.committedState.Tables[t.tableName]
			if ts != nil {
				ptr, exists := ts.RecordPointers[id]
				if exists {
					r, err := t.readFromStorage(ptr)
					if err != nil {
						return false, err
					}
					oldRecord = r
					found = true
				}
			}
		}
	}

	if !found {
		return false, nil
	}

	if _, ok := cs.Inserts[id]; ok {
		delete(cs.Inserts, id)
	} else {
		delete(cs.Updates, id)
		cs.Deletes[id] = struct{}{}
	}

	// Index removals
	ts := tx.committedState.Tables[t.tableName]
	if ts != nil {
		for _, idx := range ts.Indexes {
			idxVal := getFieldValue(oldRecord, idx.Metadata.FieldName)
			if idxVal != nil {
				tx.changeSet.IndexChanges.Remove(t.tableName, idx.Metadata.IndexName, idxVal, id)
			}
		}
	}

	return true, nil
}

func (t *Table[ID, T]) Clear(tx *Transaction) error {
	if tx != nil {
		return t.clear(tx)
	}
	return t.db.Transaction(func(tx *Transaction) error {
		return t.clear(tx)
	})
}

func (t *Table[ID, T]) clear(tx *Transaction) error {
	if !tx.active {
		return ErrTransactionNotActive
	}

	cs := tx.changeSet.GetTableChanges(t.tableName)
	cs.Cleared = true
	cs.Inserts = make(map[any]any)
	cs.Updates = make(map[any]any)
	cs.Deletes = make(map[any]struct{})
	// Clear index changes for this table in this transaction
	// Note: in Java it is `tx.getChangeSet().getIndexChanges().clear()`.
	// We can filter out IndexChanges related to this table.
	var remainingAdded []IndexChange
	for _, added := range tx.changeSet.IndexChanges.Added {
		if added.TableName != t.tableName {
			remainingAdded = append(remainingAdded, added)
		}
	}
	tx.changeSet.IndexChanges.Added = remainingAdded

	var remainingRemoved []IndexChange
	for _, removed := range tx.changeSet.IndexChanges.Removed {
		if removed.TableName != t.tableName {
			remainingRemoved = append(remainingRemoved, removed)
		}
	}
	tx.changeSet.IndexChanges.Removed = remainingRemoved

	return nil
}

func (t *Table[ID, T]) Query(tx *Transaction) *Query[T] {
	if tx != nil {
		committed := tx.committedState.Tables[t.tableName]
		staging := tx.changeSet.GetTableChanges(t.tableName)
		return NewQuery[T](t.tableName, t.db, committed, staging)
	}
	committed := t.db.getCommittedState().Tables[t.tableName]
	return NewQuery[T](t.tableName, t.db, committed, nil)
}
