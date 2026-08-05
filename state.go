package masterkeeper

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type IndexMetadata struct {
	IndexName string
	FieldName string
	Unique    bool
	Ordered   bool
}

type TableState struct {
	TableName         string
	IdType            reflect.Type
	EntityType        reflect.Type
	IndexMetadataList []IndexMetadata
}

func NewTableState(tableName string, idType reflect.Type, entityType reflect.Type, indexMetadataList []IndexMetadata) *TableState {
	return &TableState{
		TableName:         tableName,
		IdType:            idType,
		EntityType:        entityType,
		IndexMetadataList: indexMetadataList,
	}
}

func (tableState *TableState) Copy() *TableState {
	indexMetadataCopy := make([]IndexMetadata, len(tableState.IndexMetadataList))
	copy(indexMetadataCopy, tableState.IndexMetadataList)
	return &TableState{
		TableName:         tableState.TableName,
		IdType:            tableState.IdType,
		EntityType:        tableState.EntityType,
		IndexMetadataList: indexMetadataCopy,
	}
}

func (tableState *TableState) Insert(record any, recordPointer RecordPointer) {
}

func (tableState *TableState) Update(record any, oldRecord any, recordPointer RecordPointer) {
}

func (tableState *TableState) Delete(key any, oldRecord any) {
}

func (tableState *TableState) Clear() {
}

type DatabaseState struct {
	Generation int64
	Tables     map[string]*TableState
}

func NewDatabaseState(generation int64) *DatabaseState {
	return &DatabaseState{
		Generation: generation,
		Tables:     make(map[string]*TableState),
	}
}

func (databaseState *DatabaseState) Copy(nextGenerationeration int64) *DatabaseState {
	newTables := make(map[string]*TableState)
	for tableName, tableStateValue := range databaseState.Tables {
		newTables[tableName] = tableStateValue.Copy()
	}
	return &DatabaseState{
		Generation: nextGenerationeration,
		Tables:     newTables,
	}
}

// Helpers

type structFieldMarshalInfo struct {
	index int
	name  string
}

type structFieldInfo struct {
	name      string
	index     int
	isID      bool
	isIndex   bool
	isUnique  bool
	isOrdered bool
}

type structMetadata struct {
	idField       structFieldInfo
	fields        map[string]structFieldInfo
	marshalFields []structFieldMarshalInfo
}

var structCache sync.Map

func getStructMetadata(reflectType reflect.Type) *structMetadata {
	for reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}
	if reflectType.Kind() != reflect.Struct {
		return nil
	}

	if cachedMetadata, found := structCache.Load(reflectType); found {
		return cachedMetadata.(*structMetadata)
	}

	metadata := &structMetadata{
		fields: make(map[string]structFieldInfo),
	}

	for index := 0; index < reflectType.NumField(); index++ {
		structField := reflectType.Field(index)
		if structField.PkgPath != "" { // Unexported
			continue
		}

		tag := structField.Tag.Get("keeper")
		fieldInfo := structFieldInfo{
			name:  structField.Name,
			index: index,
		}

		fieldName := structField.Name
		if tag != "" {
			parts := strings.Split(tag, ",")
			if len(parts) > 0 {
				part0 := strings.TrimSpace(parts[0])
				if part0 != "id" && part0 != "index" && part0 != "unique" && part0 != "ordered" {
					fieldName = part0
				}
			}
			for _, part := range parts {
				switch strings.TrimSpace(part) {
				case "id":
					fieldInfo.isID = true
				case "index":
					fieldInfo.isIndex = true
				case "unique":
					fieldInfo.isUnique = true
				case "ordered":
					fieldInfo.isOrdered = true
				}
			}
		}

		metadata.fields[strings.ToLower(structField.Name)] = fieldInfo
		metadata.fields[strings.ToLower(fieldName)] = fieldInfo
		if fieldInfo.isID {
			metadata.idField = fieldInfo
		}

		metadata.marshalFields = append(metadata.marshalFields, structFieldMarshalInfo{
			index: index,
			name:  fieldName,
		})
	}

	if metadata.idField.name == "" {
		if fieldInfo, found := metadata.fields["id"]; found {
			fieldInfo.isID = true
			metadata.idField = fieldInfo
			metadata.fields["id"] = fieldInfo
		}
	}

	actualMetadata, _ := structCache.LoadOrStore(reflectType, metadata)
	return actualMetadata.(*structMetadata)
}

func getPrimaryKey(record any) any {
	reflectValue := reflect.ValueOf(record)
	for reflectValue.Kind() == reflect.Ptr || reflectValue.Kind() == reflect.Interface {
		if reflectValue.IsNil() {
			return nil
		}
		reflectValue = reflectValue.Elem()
	}
	if reflectValue.Kind() != reflect.Struct {
		return nil
	}

	metadata := getStructMetadata(reflectValue.Type())
	if metadata == nil || metadata.idField.name == "" {
		return nil
	}

	fieldValue := reflectValue.Field(metadata.idField.index)
	for fieldValue.Kind() == reflect.Ptr || fieldValue.Kind() == reflect.Interface {
		if fieldValue.IsNil() {
			return nil
		}
		fieldValue = fieldValue.Elem()
	}
	return fieldValue.Interface()
}

func getFieldValue(record any, fieldName string) any {
	reflectValue := reflect.ValueOf(record)
	for reflectValue.Kind() == reflect.Ptr || reflectValue.Kind() == reflect.Interface {
		if reflectValue.IsNil() {
			return nil
		}
		reflectValue = reflectValue.Elem()
	}
	if reflectValue.Kind() != reflect.Struct {
		return nil
	}

	metadata := getStructMetadata(reflectValue.Type())
	if metadata == nil {
		return nil
	}

	fieldInfo, found := metadata.fields[strings.ToLower(fieldName)]
	if !found {
		return nil
	}

	fieldValue := reflectValue.Field(fieldInfo.index)
	for fieldValue.Kind() == reflect.Ptr || fieldValue.Kind() == reflect.Interface {
		if fieldValue.IsNil() {
			return nil
		}
		fieldValue = fieldValue.Elem()
	}
	if !fieldValue.IsValid() {
		return nil
	}
	return fieldValue.Interface()
}

func asFloat(value any) (float64, bool) {
	switch val := value.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	}
	return 0, false
}

func asInteger(value any) (int64, bool) {
	switch val := value.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case int16:
		return int64(val), true
	case int8:
		return int64(val), true
	}
	return 0, false
}

func asFloatOrInteger(value any) (float64, bool) {
	if fVal, ok := asFloat(value); ok {
		return fVal, true
	}
	if iVal, ok := asInteger(value); ok {
		return float64(iVal), true
	}
	return 0, false
}

func compareValues(leftValue, rightValue any) int {
	if leftValue == nil && rightValue == nil {
		return 0
	}
	if leftValue == nil {
		return -1
	}
	if rightValue == nil {
		return 1
	}

	// 1. Try integer comparison first (to preserve full precision)
	if leftInt, isLeftInt := asInteger(leftValue); isLeftInt {
		if rightInt, isRightInt := asInteger(rightValue); isRightInt {
			if leftInt < rightInt {
				return -1
			} else if leftInt > rightInt {
				return 1
			}
			return 0
		}
	}

	// 2. Try numeric comparison (floats, or mixed float/integer)
	if leftNum, isLeftNum := asFloatOrInteger(leftValue); isLeftNum {
		if rightNum, isRightNum := asFloatOrInteger(rightValue); isRightNum {
			if leftNum < rightNum {
				return -1
			} else if leftNum > rightNum {
				return 1
			}
			return 0
		}
	}

	switch left := leftValue.(type) {
	case string:
		if right, found := rightValue.(string); found {
			if left < right {
				return -1
			} else if left > right {
				return 1
			}
			return 0
		}
	case bool:
		if right, found := rightValue.(bool); found {
			if !left && right {
				return -1
			} else if left && !right {
				return 1
			}
			return 0
		}
	case time.Time:
		if right, found := rightValue.(time.Time); found {
			if left.Before(right) {
				return -1
			} else if left.After(right) {
				return 1
			}
			return 0
		}
	}
	leftStr := fmt.Sprintf("%v", leftValue)
	rightStr := fmt.Sprintf("%v", rightValue)
	if leftStr < rightStr {
		return -1
	} else if leftStr > rightStr {
		return 1
	}
	return 0
}

func valuesEqual(leftValue, rightValue any) bool {
	return compareValues(leftValue, rightValue) == 0
}

func canonicalizeKey(value any) any {
	if value == nil {
		return nil
	}
	reflectValue := reflect.ValueOf(value)
	for reflectValue.Kind() == reflect.Ptr || reflectValue.Kind() == reflect.Interface {
		if reflectValue.IsNil() {
			return nil
		}
		reflectValue = reflectValue.Elem()
	}
	if reflectValue.Type().Comparable() {
		return reflectValue.Interface()
	}

	// Handle []byte specifically
	if reflectValue.Kind() == reflect.Slice && reflectValue.Type().Elem().Kind() == reflect.Uint8 {
		return string(reflectValue.Bytes())
	}

	// For other non-comparable types, fallback to their string representation
	return fmt.Sprintf("%v", reflectValue.Interface())
}
