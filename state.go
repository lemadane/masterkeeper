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

type IndexState struct {
	Metadata     IndexMetadata
	UniqueMap    map[any]any   // maps indexVal -> primaryKey
	SecondaryMap map[any][]any // maps indexVal -> slice of primaryKeys
	SortedKeys   []any         // kept sorted if Ordered is true
}

func NewIndexState(meta IndexMetadata) *IndexState {
	idx := &IndexState{
		Metadata: meta,
	}
	if meta.Unique {
		idx.UniqueMap = make(map[any]any)
	} else {
		idx.SecondaryMap = make(map[any][]any)
	}
	return idx
}

func (idx *IndexState) Copy() *IndexState {
	newIdx := &IndexState{
		Metadata: idx.Metadata,
	}
	if idx.Metadata.Unique {
		newIdx.UniqueMap = make(map[any]any)
		for k, v := range idx.UniqueMap {
			newIdx.UniqueMap[k] = v
		}
	} else {
		newIdx.SecondaryMap = make(map[any][]any)
		for k, v := range idx.SecondaryMap {
			sliceCopy := make([]any, len(v))
			copy(sliceCopy, v)
			newIdx.SecondaryMap[k] = sliceCopy
		}
		if idx.Metadata.Ordered {
			newIdx.SortedKeys = make([]any, len(idx.SortedKeys))
			copy(newIdx.SortedKeys, idx.SortedKeys)
		}
	}
	return newIdx
}

func (idx *IndexState) Add(indexVal any, primaryKey any) {
	if indexVal == nil {
		return
	}
	if idx.Metadata.Unique {
		idx.UniqueMap[indexVal] = primaryKey
	} else {
		keys := idx.SecondaryMap[indexVal]
		found := false
		for _, k := range keys {
			if k == primaryKey {
				found = true
				break
			}
		}
		if !found {
			idx.SecondaryMap[indexVal] = append(keys, primaryKey)
		}

		if idx.Metadata.Ordered {
			// Maintain SortedKeys
			idx.insertSortedKey(indexVal)
		}
	}
}

func (idx *IndexState) Remove(indexVal any, primaryKey any) {
	if indexVal == nil {
		return
	}
	if idx.Metadata.Unique {
		delete(idx.UniqueMap, indexVal)
	} else {
		keys := idx.SecondaryMap[indexVal]
		for i, k := range keys {
			if k == primaryKey {
				idx.SecondaryMap[indexVal] = append(keys[:i], keys[i+1:]...)
				break
			}
		}
		if len(idx.SecondaryMap[indexVal]) == 0 {
			delete(idx.SecondaryMap, indexVal)
			if idx.Metadata.Ordered {
				idx.removeSortedKey(indexVal)
			}
		}
	}
}

func (idx *IndexState) insertSortedKey(key any) {
	// Binary search to find position
	low, high := 0, len(idx.SortedKeys)-1
	pos := len(idx.SortedKeys)
	for low <= high {
		mid := (low + high) / 2
		cmp := compareValues(idx.SortedKeys[mid], key)
		if cmp == 0 {
			return // already present
		} else if cmp < 0 {
			low = mid + 1
		} else {
			pos = mid
			high = mid - 1
		}
	}
	if pos == len(idx.SortedKeys) {
		idx.SortedKeys = append(idx.SortedKeys, key)
	} else {
		idx.SortedKeys = append(idx.SortedKeys, nil)
		copy(idx.SortedKeys[pos+1:], idx.SortedKeys[pos:])
		idx.SortedKeys[pos] = key
	}
}

func (idx *IndexState) removeSortedKey(key any) {
	low, high := 0, len(idx.SortedKeys)-1
	for low <= high {
		mid := (low + high) / 2
		cmp := compareValues(idx.SortedKeys[mid], key)
		if cmp == 0 {
			idx.SortedKeys = append(idx.SortedKeys[:mid], idx.SortedKeys[mid+1:]...)
			return
		} else if cmp < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
}

func (idx *IndexState) Clear() {
	if idx.UniqueMap != nil {
		idx.UniqueMap = make(map[any]any)
	}
	if idx.SecondaryMap != nil {
		idx.SecondaryMap = make(map[any][]any)
	}
	idx.SortedKeys = nil
}

type TableState struct {
	TableName         string
	IdType            reflect.Type
	EntityType        reflect.Type
	RecordPointers    map[any]RecordPointer
	Indexes           map[string]*IndexState
	IndexMetadataList []IndexMetadata
}

func NewTableState(tableName string, idType reflect.Type, entityType reflect.Type, indexMetadataList []IndexMetadata) *TableState {
	ts := &TableState{
		TableName:         tableName,
		IdType:            idType,
		EntityType:        entityType,
		RecordPointers:    make(map[any]RecordPointer),
		Indexes:           make(map[string]*IndexState),
		IndexMetadataList: indexMetadataList,
	}
	for _, meta := range indexMetadataList {
		ts.Indexes[meta.IndexName] = NewIndexState(meta)
	}
	return ts
}

func (ts *TableState) Copy() *TableState {
	newPointers := make(map[any]RecordPointer)
	for k, v := range ts.RecordPointers {
		newPointers[k] = v
	}
	newIndexes := make(map[string]*IndexState)
	for k, v := range ts.Indexes {
		newIndexes[k] = v.Copy()
	}
	return &TableState{
		TableName:         ts.TableName,
		IdType:            ts.IdType,
		EntityType:        ts.EntityType,
		RecordPointers:    newPointers,
		Indexes:           newIndexes,
		IndexMetadataList: ts.IndexMetadataList,
	}
}

func (ts *TableState) Insert(record any, ptr RecordPointer) {
	id := getPrimaryKey(record)
	ts.RecordPointers[id] = ptr
	for _, idx := range ts.Indexes {
		idxVal := getFieldValue(record, idx.Metadata.FieldName)
		idx.Add(idxVal, id)
	}
}

func (ts *TableState) Update(record any, oldRecord any, ptr RecordPointer) {
	id := getPrimaryKey(record)
	ts.RecordPointers[id] = ptr
	for _, idx := range ts.Indexes {
		var oldVal any
		if oldRecord != nil {
			oldVal = getFieldValue(oldRecord, idx.Metadata.FieldName)
		}
		newVal := getFieldValue(record, idx.Metadata.FieldName)
		if !valuesEqual(oldVal, newVal) {
			if oldVal != nil {
				idx.Remove(oldVal, id)
			}
			idx.Add(newVal, id)
		}
	}
}

func (ts *TableState) Delete(key any, oldRecord any) {
	delete(ts.RecordPointers, key)
	if oldRecord != nil {
		for _, idx := range ts.Indexes {
			idxVal := getFieldValue(oldRecord, idx.Metadata.FieldName)
			if idxVal != nil {
				idx.Remove(idxVal, key)
			}
		}
	}
}

func (ts *TableState) Clear() {
	ts.RecordPointers = make(map[any]RecordPointer)
	for _, idx := range ts.Indexes {
		idx.Clear()
	}
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

func (ds *DatabaseState) Copy(nextGen int64) *DatabaseState {
	newTables := make(map[string]*TableState)
	for k, v := range ds.Tables {
		newTables[k] = v.Copy()
	}
	return &DatabaseState{
		Generation: nextGen,
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

func getStructMetadata(t reflect.Type) *structMetadata {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	if val, ok := structCache.Load(t); ok {
		return val.(*structMetadata)
	}

	meta := &structMetadata{
		fields: make(map[string]structFieldInfo),
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // Unexported
			continue
		}

		tag := f.Tag.Get("keeper")
		info := structFieldInfo{
			name:  f.Name,
			index: i,
		}

		fieldName := f.Name
		if tag != "" {
			parts := strings.Split(tag, ",")
			if len(parts) > 0 {
				p0 := strings.TrimSpace(parts[0])
				if p0 != "id" && p0 != "index" && p0 != "unique" && p0 != "ordered" {
					fieldName = p0
				}
			}
			for _, p := range parts {
				switch strings.TrimSpace(p) {
				case "id":
					info.isID = true
				case "index":
					info.isIndex = true
				case "unique":
					info.isUnique = true
				case "ordered":
					info.isOrdered = true
				}
			}
		}

		meta.fields[strings.ToLower(f.Name)] = info
		meta.fields[strings.ToLower(fieldName)] = info
		if info.isID {
			meta.idField = info
		}

		meta.marshalFields = append(meta.marshalFields, structFieldMarshalInfo{
			index: i,
			name:  fieldName,
		})
	}

	if meta.idField.name == "" {
		if info, ok := meta.fields["id"]; ok {
			info.isID = true
			meta.idField = info
			meta.fields["id"] = info
		}
	}

	actual, _ := structCache.LoadOrStore(t, meta)
	return actual.(*structMetadata)
}

func getPrimaryKey(record any) any {
	val := reflect.ValueOf(record)
	for val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}

	meta := getStructMetadata(val.Type())
	if meta == nil || meta.idField.name == "" {
		return nil
	}

	fieldVal := val.Field(meta.idField.index)
	for fieldVal.Kind() == reflect.Ptr || fieldVal.Kind() == reflect.Interface {
		if fieldVal.IsNil() {
			return nil
		}
		fieldVal = fieldVal.Elem()
	}
	return fieldVal.Interface()
}

func getFieldValue(record any, fieldName string) any {
	val := reflect.ValueOf(record)
	for val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}

	meta := getStructMetadata(val.Type())
	if meta == nil {
		return nil
	}

	info, ok := meta.fields[strings.ToLower(fieldName)]
	if !ok {
		return nil
	}

	fieldVal := val.Field(info.index)
	for fieldVal.Kind() == reflect.Ptr || fieldVal.Kind() == reflect.Interface {
		if fieldVal.IsNil() {
			return nil
		}
		fieldVal = fieldVal.Elem()
	}
	if !fieldVal.IsValid() {
		return nil
	}
	return fieldVal.Interface()
}

func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int32:
		if vb, ok := b.(int32); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case bool:
		if vb, ok := b.(bool); ok {
			if !va && vb {
				return -1
			} else if va && !vb {
				return 1
			}
			return 0
		}
	case time.Time:
		if vb, ok := b.(time.Time); ok {
			if va.Before(vb) {
				return -1
			} else if va.After(vb) {
				return 1
			}
			return 0
		}
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

func valuesEqual(a, b any) bool {
	return compareValues(a, b) == 0
}
