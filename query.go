package masterkeeper

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type Condition interface {
	Test(record any) bool
	FieldName() string
	ExplainDetail() string
}

type conditionImpl struct {
	fieldName string
	value     any
	testFn    func(recVal any) bool
	desc      string
}

func (c *conditionImpl) Test(record any) bool {
	recVal := getFieldValue(record, c.fieldName)
	return c.testFn(recVal)
}

func (c *conditionImpl) FieldName() string {
	return c.fieldName
}

func (c *conditionImpl) ExplainDetail() string {
	return c.desc
}

func Eq(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s == %v", fieldName, value),
		testFn: func(recVal any) bool {
			return valuesEqual(recVal, value)
		},
	}
}

func Ne(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s != %v", fieldName, value),
		testFn: func(recVal any) bool {
			return !valuesEqual(recVal, value)
		},
	}
}

func Gt(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s > %v", fieldName, value),
		testFn: func(recVal any) bool {
			return compareValues(recVal, value) > 0
		},
	}
}

func Ge(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s >= %v", fieldName, value),
		testFn: func(recVal any) bool {
			return compareValues(recVal, value) >= 0
		},
	}
}

func Lt(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s < %v", fieldName, value),
		testFn: func(recVal any) bool {
			return compareValues(recVal, value) < 0
		},
	}
}

func Le(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s <= %v", fieldName, value),
		testFn: func(recVal any) bool {
			return compareValues(recVal, value) <= 0
		},
	}
}

type SortOrder struct {
	FieldName string
	Ascending bool
}

func Asc(fieldName string) SortOrder {
	return SortOrder{FieldName: fieldName, Ascending: true}
}

func Desc(fieldName string) SortOrder {
	return SortOrder{FieldName: fieldName, Ascending: false}
}

type QueryPlan struct {
	Strategy    string
	OverlayUsed bool
}

type Query[T any] struct {
	tableName      string
	db             *Database
	committedTable *TableState
	staging        *TableChangeSet
	conditions     []Condition
	sortOrder      *SortOrder
	limit          int
	offset         int
}

func NewQuery[T any](tableName string, db *Database, committedTable *TableState, staging *TableChangeSet) *Query[T] {
	return &Query[T]{
		tableName:      tableName,
		db:             db,
		committedTable: committedTable,
		staging:        staging,
		limit:          -1,
		offset:         0,
	}
}

func (q *Query[T]) Where(cond Condition) *Query[T] {
	if cond != nil {
		q.conditions = append(q.conditions, cond)
	}
	return q
}

func (q *Query[T]) OrderBy(order SortOrder) *Query[T] {
	q.sortOrder = &order
	return q
}

func (q *Query[T]) Limit(limit int) *Query[T] {
	q.limit = limit
	return q
}

func (q *Query[T]) Offset(offset int) *Query[T] {
	q.offset = offset
	return q
}

func (q *Query[T]) List() ([]T, error) {
	var records []any
	useIndexLookup := false
	var targetIDs []any

	for _, cond := range q.conditions {
		if strings.EqualFold(cond.FieldName(), "id") {
			if eqCond, ok := cond.(*conditionImpl); ok && strings.Contains(eqCond.desc, "==") {
				targetID := eqCond.value
				targetIDs = []any{targetID}
				useIndexLookup = true
				break
			}
		}
	}

	if !useIndexLookup && q.committedTable != nil {
		for _, cond := range q.conditions {
			if eqCond, ok := cond.(*conditionImpl); ok && strings.Contains(eqCond.desc, "==") {
				fieldName := eqCond.FieldName()
				val := eqCond.value

				for _, idx := range q.committedTable.Indexes {
					if strings.EqualFold(idx.Metadata.FieldName, fieldName) {
						useIndexLookup = true
						if idx.Metadata.Unique {
							if pKey, exists := idx.UniqueMap[val]; exists {
								targetIDs = append(targetIDs, pKey)
							}
						} else {
							if pKeys, exists := idx.SecondaryMap[val]; exists {
								targetIDs = append(targetIDs, pKeys...)
							}
						}
						break
					}
				}
				if useIndexLookup {
					break
				}
			}
		}
	}

	if useIndexLookup {
		if q.staging != nil && q.staging.Cleared {
			for _, id := range targetIDs {
				if rec, ok := q.staging.Inserts[id]; ok {
					records = append(records, rec)
				}
			}
		} else {
			if q.committedTable != nil {
				storage, err := q.db.getTableStorage(q.tableName)
				if err != nil {
					return nil, err
				}

				for _, id := range targetIDs {
					if q.staging != nil {
						if _, deleted := q.staging.Deletes[id]; deleted {
							continue
						}
						if _, updated := q.staging.Updates[id]; updated {
							continue
						}
						if _, inserted := q.staging.Inserts[id]; inserted {
							continue
						}
					}

					ptr, exists := q.committedTable.RecordPointers[id]
					if exists {
						bytes, err := storage.ReadRecord(ptr)
						if err != nil {
							return nil, err
						}

						newRecordVal := reflect.New(q.committedTable.EntityType)
						if err := Unmarshal(bytes, newRecordVal.Interface()); err != nil {
							return nil, err
						}
						records = append(records, newRecordVal.Elem().Interface())
					}
				}
			}

			if q.staging != nil {
				for _, id := range targetIDs {
					if rec, ok := q.staging.Inserts[id]; ok {
						records = append(records, rec)
					}
					if rec, ok := q.staging.Updates[id]; ok {
						records = append(records, rec)
					}
				}

				for _, rec := range q.staging.Inserts {
					id := getPrimaryKey(rec)
					alreadyAdded := false
					for _, tID := range targetIDs {
						if tID == id {
							alreadyAdded = true
							break
						}
					}
					if !alreadyAdded {
						records = append(records, rec)
					}
				}
				for _, rec := range q.staging.Updates {
					id := getPrimaryKey(rec)
					alreadyAdded := false
					for _, tID := range targetIDs {
						if tID == id {
							alreadyAdded = true
							break
						}
					}
					if !alreadyAdded {
						records = append(records, rec)
					}
				}
			}
		}
	} else {
		if q.staging != nil && q.staging.Cleared {
			for _, rec := range q.staging.Inserts {
				records = append(records, rec)
			}
		} else {
			if q.committedTable != nil {
				storage, err := q.db.getTableStorage(q.tableName)
				if err != nil {
					return nil, err
				}

				for id, ptr := range q.committedTable.RecordPointers {
					if q.staging != nil {
						if _, deleted := q.staging.Deletes[id]; deleted {
							continue
						}
						if _, updated := q.staging.Updates[id]; updated {
							continue
						}
						if _, inserted := q.staging.Inserts[id]; inserted {
							continue
						}
					}

					bytes, err := storage.ReadRecord(ptr)
					if err != nil {
						return nil, err
					}

					newRecordVal := reflect.New(q.committedTable.EntityType)
					if err := Unmarshal(bytes, newRecordVal.Interface()); err != nil {
						return nil, err
					}
					records = append(records, newRecordVal.Elem().Interface())
				}
			}

			if q.staging != nil {
				for _, rec := range q.staging.Inserts {
					records = append(records, rec)
				}
				for _, rec := range q.staging.Updates {
					records = append(records, rec)
				}
			}
		}
	}

	var filtered []any
	for _, rec := range records {
		match := true
		for _, cond := range q.conditions {
			if !cond.Test(rec) {
				match = false
				break
			}
		}
		if match {
			id := getPrimaryKey(rec)
			duplicate := false
			for _, fRec := range filtered {
				if getPrimaryKey(fRec) == id {
					duplicate = true
					break
				}
			}
			if !duplicate {
				filtered = append(filtered, rec)
			}
		}
	}

	if q.sortOrder != nil {
		field := q.sortOrder.FieldName
		ascending := q.sortOrder.Ascending

		sort.Slice(filtered, func(i, j int) bool {
			valA := getFieldValue(filtered[i], field)
			valB := getFieldValue(filtered[j], field)
			cmp := compareValues(valA, valB)
			if ascending {
				return cmp < 0
			}
			return cmp > 0
		})
	}

	fromIndex := q.offset
	if fromIndex > len(filtered) {
		fromIndex = len(filtered)
	}

	toIndex := len(filtered)
	if q.limit >= 0 {
		toIndex = fromIndex + q.limit
		if toIndex > len(filtered) {
			toIndex = len(filtered)
		}
	}

	var results []T
	for i := fromIndex; i < toIndex; i++ {
		results = append(results, filtered[i].(T))
	}

	return results, nil
}

func (q *Query[T]) FindFirst() (T, bool, error) {
	var zero T
	results, err := q.Limit(1).List()
	if err != nil {
		return zero, false, err
	}
	if len(results) == 0 {
		return zero, false, nil
	}
	return results[0], true, nil
}

func (q *Query[T]) Explain() QueryPlan {
	overlayUsed := q.staging != nil && (!q.staging.IsEmpty())
	strategy := "FULL_SCAN_WITH_TRANSACTION_OVERLAY"

	for _, cond := range q.conditions {
		if strings.EqualFold(cond.FieldName(), "id") {
			strategy = "PRIMARY_KEY_LOOKUP_WITH_TRANSACTION_OVERLAY"
			break
		}

		if q.committedTable != nil {
			for _, idx := range q.committedTable.Indexes {
				if strings.EqualFold(idx.Metadata.FieldName, cond.FieldName()) {
					if idx.Metadata.Unique {
						strategy = "UNIQUE_INDEX_LOOKUP_WITH_TRANSACTION_OVERLAY"
					} else if idx.Metadata.Ordered {
						strategy = "ORDERED_INDEX_LOOKUP_WITH_TRANSACTION_OVERLAY"
					} else {
						strategy = "SECONDARY_INDEX_LOOKUP_WITH_TRANSACTION_OVERLAY"
					}
					break
				}
			}
		}
	}

	return QueryPlan{
		Strategy:    strategy,
		OverlayUsed: overlayUsed,
	}
}
