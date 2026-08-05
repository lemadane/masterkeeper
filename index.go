package masterkeeper

import (
	"bytes"
	"reflect"
)

type PersistentIndex interface {
	Find(key []byte, generation uint64) (RecordPointer, bool, error)

	Insert(
		key []byte,
		pointer RecordPointer,
		generation uint64,
	) (newRoot PageID, errorVal error)

	Delete(
		key []byte,
		generation uint64,
	) (newRoot PageID, errorVal error)

	Range(
		start []byte,
		end []byte,
		generation uint64,
		visit func([]byte, RecordPointer) bool,
	) error

	Sync() error
	Close() error
}

func serializeKey(key any) ([]byte, error) {
	if key == nil {
		return nil, nil
	}
	var buffer bytes.Buffer
	if writeError := writeValue(&buffer, reflect.ValueOf(key)); writeError != nil {
		return nil, writeError
	}
	return buffer.Bytes(), nil
}

func deserializeKey(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return readValue(bytes.NewReader(data))
}
