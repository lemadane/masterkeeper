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
	testFn    func(recordValue any) bool
	desc      string
}

func (condition *conditionImpl) Test(record any) bool {
	recordValue := getFieldValue(record, condition.fieldName)
	return condition.testFn(recordValue)
}

func (condition *conditionImpl) FieldName() string {
	return condition.fieldName
}

func (condition *conditionImpl) ExplainDetail() string {
	return condition.desc
}

func Eq(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s == %v", fieldName, value),
		testFn: func(recordValue any) bool {
			return valuesEqual(recordValue, value)
		},
	}
}

func Ne(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s != %v", fieldName, value),
		testFn: func(recordValue any) bool {
			return !valuesEqual(recordValue, value)
		},
	}
}

func Gt(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s > %v", fieldName, value),
		testFn: func(recordValue any) bool {
			return compareValues(recordValue, value) > 0
		},
	}
}

func Ge(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s >= %v", fieldName, value),
		testFn: func(recordValue any) bool {
			return compareValues(recordValue, value) >= 0
		},
	}
}

func Lt(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s < %v", fieldName, value),
		testFn: func(recordValue any) bool {
			return compareValues(recordValue, value) < 0
		},
	}
}

func Le(fieldName string, value any) Condition {
	return &conditionImpl{
		fieldName: fieldName,
		value:     value,
		desc:      fmt.Sprintf("%s <= %v", fieldName, value),
		testFn: func(recordValue any) bool {
			return compareValues(recordValue, value) <= 0
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
	database       *Database
	committedTable *TableState
	staging        *TableChangeSet
	conditions     []Condition
	sortOrder      *SortOrder
	limit          int
	offset         int
}

func NewQuery[T any](tableName string, database *Database, committedTable *TableState, staging *TableChangeSet) *Query[T] {
	return &Query[T]{
		tableName:      tableName,
		database:       database,
		committedTable: committedTable,
		staging:        staging,
		limit:          -1,
		offset:         0,
	}
}

func (query *Query[T]) Where(condition Condition) *Query[T] {
	if condition != nil {
		query.conditions = append(query.conditions, condition)
	}
	return query
}

func (query *Query[T]) OrderBy(order SortOrder) *Query[T] {
	query.sortOrder = &order
	return query
}

func (query *Query[T]) Limit(limit int) *Query[T] {
	query.limit = limit
	return query
}

func (query *Query[T]) Offset(offset int) *Query[T] {
	if offset < 0 {
		query.offset = 0
	} else {
		query.offset = offset
	}
	return query
}

func (query *Query[T]) List() ([]T, error) {
	var records []any
	useIndexLookup := false
	var targetIDs []any

	for _, condition := range query.conditions {
		if strings.EqualFold(condition.FieldName(), "id") {
			if equalityCondition, found := condition.(*conditionImpl); found && strings.Contains(equalityCondition.desc, "==") {
				targetIDValue := equalityCondition.value
				targetIDs = []any{targetIDValue}
				useIndexLookup = true
				break
			}
		}
	}

	if !useIndexLookup && query.committedTable != nil && (query.staging == nil || !query.staging.Cleared) {
		for _, condition := range query.conditions {
			if equalityCondition, found := condition.(*conditionImpl); found && strings.Contains(equalityCondition.desc, "==") {
				fieldName := equalityCondition.FieldName()
				targetValue := equalityCondition.value

				for _, indexState := range query.committedTable.Indexes {
					if strings.EqualFold(indexState.Metadata.FieldName, fieldName) {
						useIndexLookup = true
						if indexState.Metadata.Unique {
							if primaryKey, foundKey := indexState.UniqueMap[targetValue]; foundKey {
								targetIDs = append(targetIDs, primaryKey)
							}
						} else {
							if primaryKeys, foundKeys := indexState.SecondaryMap[targetValue]; foundKeys {
								targetIDs = append(targetIDs, primaryKeys...)
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
		if query.staging != nil && query.staging.Cleared {
			for _, idValue := range targetIDs {
				if record, found := query.staging.Inserts[idValue]; found {
					records = append(records, record)
				}
			}
		} else {
			if query.committedTable != nil {
				tableStorage, err := query.database.getTableStorage(query.tableName)
				if err != nil {
					return nil, err
				}

				for _, idValue := range targetIDs {
					if query.staging != nil {
						if _, deleted := query.staging.Deletes[idValue]; deleted {
							continue
						}
						if _, updated := query.staging.Updates[idValue]; updated {
							continue
						}
						if _, inserted := query.staging.Inserts[idValue]; inserted {
							continue
						}
					}

					recordPointer, exists := query.committedTable.RecordPointers[idValue]
					if exists {
						bytesValue, err := tableStorage.ReadRecord(recordPointer)
						if err != nil {
							return nil, err
						}

						newRecordVal := reflect.New(query.committedTable.EntityType)
						if err := Unmarshal(bytesValue, newRecordVal.Interface()); err != nil {
							return nil, err
						}
						records = append(records, newRecordVal.Elem().Interface())
					}
				}
			}

			if query.staging != nil {
				for _, idValue := range targetIDs {
					if record, found := query.staging.Inserts[idValue]; found {
						records = append(records, record)
					}
					if record, found := query.staging.Updates[idValue]; found {
						records = append(records, record)
					}
				}

				for _, record := range query.staging.Inserts {
					primaryKey := getPrimaryKey(record)
					alreadyAdded := false
					for _, targetIDValue := range targetIDs {
						if targetIDValue == primaryKey {
							alreadyAdded = true
							break
						}
					}
					if !alreadyAdded {
						records = append(records, record)
					}
				}
				for _, record := range query.staging.Updates {
					primaryKey := getPrimaryKey(record)
					alreadyAdded := false
					for _, targetIDValue := range targetIDs {
						if targetIDValue == primaryKey {
							alreadyAdded = true
							break
						}
					}
					if !alreadyAdded {
						records = append(records, record)
					}
				}
			}
		}
	} else {
		if query.staging != nil && query.staging.Cleared {
			for _, record := range query.staging.Inserts {
				records = append(records, record)
			}
		} else {
			if query.committedTable != nil {
				tableStorage, err := query.database.getTableStorage(query.tableName)
				if err != nil {
					return nil, err
				}

				for primaryKey, recordPointer := range query.committedTable.RecordPointers {
					if query.staging != nil {
						if _, deleted := query.staging.Deletes[primaryKey]; deleted {
							continue
						}
						if _, updated := query.staging.Updates[primaryKey]; updated {
							continue
						}
						if _, inserted := query.staging.Inserts[primaryKey]; inserted {
							continue
						}
					}

					bytesValue, err := tableStorage.ReadRecord(recordPointer)
					if err != nil {
						return nil, err
					}

					newRecordVal := reflect.New(query.committedTable.EntityType)
					if err := Unmarshal(bytesValue, newRecordVal.Interface()); err != nil {
						return nil, err
					}
					records = append(records, newRecordVal.Elem().Interface())
				}
			}

			if query.staging != nil {
				for _, record := range query.staging.Inserts {
					records = append(records, record)
				}
				for _, record := range query.staging.Updates {
					records = append(records, record)
				}
			}
		}
	}

	var filtered []any
	for _, record := range records {
		match := true
		for _, condition := range query.conditions {
			if !condition.Test(record) {
				match = false
				break
			}
		}
		if match {
			primaryKey := getPrimaryKey(record)
			duplicate := false
			for _, filteredRecord := range filtered {
				if getPrimaryKey(filteredRecord) == primaryKey {
					duplicate = true
					break
				}
			}
			if !duplicate {
				filtered = append(filtered, record)
			}
		}
	}

	if query.sortOrder != nil {
		field := query.sortOrder.FieldName
		ascending := query.sortOrder.Ascending

		sort.Slice(filtered, func(indexLeft, indexRight int) bool {
			valueLeft := getFieldValue(filtered[indexLeft], field)
			valueRight := getFieldValue(filtered[indexRight], field)
			comparison := compareValues(valueLeft, valueRight)
			if ascending {
				return comparison < 0
			}
			return comparison > 0
		})
	}

	fromIndex := query.offset
	if fromIndex > len(filtered) {
		fromIndex = len(filtered)
	}

	toIndex := len(filtered)
	if query.limit >= 0 {
		toIndex = fromIndex + query.limit
		if toIndex > len(filtered) {
			toIndex = len(filtered)
		}
	}

	var results []T
	for index := fromIndex; index < toIndex; index++ {
		results = append(results, filtered[index].(T))
	}

	return results, nil
}

func (query *Query[T]) FindFirst() (T, bool, error) {
	var zero T
	results, err := query.Limit(1).List()
	if err != nil {
		return zero, false, err
	}
	if len(results) == 0 {
		return zero, false, nil
	}
	return results[0], true, nil
}

func (query *Query[T]) Explain() QueryPlan {
	overlayUsed := query.staging != nil && (!query.staging.IsEmpty())
	strategy := "FULL_SCAN_WITH_TRANSACTION_OVERLAY"

	for _, condition := range query.conditions {
		if strings.EqualFold(condition.FieldName(), "id") {
			strategy = "PRIMARY_KEY_LOOKUP_WITH_TRANSACTION_OVERLAY"
			break
		}

		if query.committedTable != nil {
			for _, indexState := range query.committedTable.Indexes {
				if strings.EqualFold(indexState.Metadata.FieldName, condition.FieldName()) {
					if indexState.Metadata.Unique {
						strategy = "UNIQUE_INDEX_LOOKUP_WITH_TRANSACTION_OVERLAY"
					} else if indexState.Metadata.Ordered {
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
