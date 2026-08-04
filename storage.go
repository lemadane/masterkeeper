package masterkeeper

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type RecordPointer struct {
	Offset int64
	Size   int32
}

type TableStorage struct {
	mu          sync.Mutex
	file        *os.File
	tablePath   string
	currentSize int64
}

func NewTableStorage(directory string, tableName string) (*TableStorage, error) {
	if !isValidTableName(tableName) {
		return nil, InvalidTableNameError
	}

	if error := os.MkdirAll(directory, 0755); error != nil {
		return nil, fmt.Errorf("failed to create directory: %w", error)
	}

	tablePath := filepath.Join(directory, tableName+".db")
	file, error := os.OpenFile(tablePath, os.O_CREATE|os.O_RDWR, 0644)
	if error != nil {
		return nil, fmt.Errorf("failed to open table file %s: %w", tablePath, error)
	}

	fileInfo, error := file.Stat()
	if error != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat table file %s: %w", tablePath, error)
	}

	return &TableStorage{
		file:        file,
		tablePath:   tablePath,
		currentSize: fileInfo.Size(),
	}, nil
}

func (tableStorage *TableStorage) AppendRecord(bytesValue []byte) (RecordPointer, error) {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()

	if tableStorage.file == nil {
		return RecordPointer{}, DatabaseClosedError
	}

	offset := tableStorage.currentSize
	written, error := tableStorage.file.WriteAt(bytesValue, offset)
	if error != nil {
		return RecordPointer{}, error
	}

	recordPointer := RecordPointer{
		Offset: offset,
		Size:   int32(written),
	}
	tableStorage.currentSize += int64(written)
	return recordPointer, nil
}

func (tableStorage *TableStorage) AppendRecords(recordsSlice [][]byte) ([]RecordPointer, error) {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()

	if tableStorage.file == nil {
		return nil, DatabaseClosedError
	}

	var totalSize int
	for _, recordBytes := range recordsSlice {
		totalSize += len(recordBytes)
	}

	buffer := make([]byte, 0, totalSize)
	var recordPointers []RecordPointer
	offset := tableStorage.currentSize

	for _, recordBytes := range recordsSlice {
		buffer = append(buffer, recordBytes...)
		recordPointers = append(recordPointers, RecordPointer{
			Offset: offset,
			Size:   int32(len(recordBytes)),
		})
		offset += int64(len(recordBytes))
	}

	if len(buffer) > 0 {
		_, error := tableStorage.file.WriteAt(buffer, tableStorage.currentSize)
		if error != nil {
			return nil, error
		}
	}

	tableStorage.currentSize = offset
	return recordPointers, nil
}

func (tableStorage *TableStorage) ReadRecord(recordPointer RecordPointer) ([]byte, error) {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()

	if tableStorage.file == nil {
		return nil, DatabaseClosedError
	}

	buffer := make([]byte, recordPointer.Size)
	_, error := tableStorage.file.ReadAt(buffer, recordPointer.Offset)
	if error != nil {
		if error == io.EOF {
			return nil, fmt.Errorf("unexpected EOF reading record at offset %d, size %d", recordPointer.Offset, recordPointer.Size)
		}
		return nil, error
	}
	return buffer, nil
}

func (tableStorage *TableStorage) Close() error {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()
	if tableStorage.file != nil {
		error := tableStorage.file.Close()
		tableStorage.file = nil
		return error
	}
	return nil
}

func (tableStorage *TableStorage) Compact(activePointers map[any]RecordPointer) error {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()

	if tableStorage.file == nil {
		return DatabaseClosedError
	}

	compactPath := tableStorage.tablePath + ".compact"
	_ = os.Remove(compactPath)

	compactedFile, error := os.OpenFile(compactPath, os.O_CREATE|os.O_RDWR, 0644)
	if error != nil {
		return fmt.Errorf("failed to open compact file: %w", error)
	}
	defer func() {
		if compactedFile != nil {
			compactedFile.Close()
			_ = os.Remove(compactPath)
		}
	}()

	var writeOffset int64
	newPointers := make(map[any]RecordPointer)

	for key, oldRecordPointer := range activePointers {
		// Read record
		buffer := make([]byte, oldRecordPointer.Size)
		if _, error := tableStorage.file.ReadAt(buffer, oldRecordPointer.Offset); error != nil {
			return fmt.Errorf("compact failed to read record: %w", error)
		}

		// Write to compact file
		if _, error := compactedFile.WriteAt(buffer, writeOffset); error != nil {
			return fmt.Errorf("compact failed to write record: %w", error)
		}

		newPointers[key] = RecordPointer{
			Offset: writeOffset,
			Size:   oldRecordPointer.Size,
		}
		writeOffset += int64(oldRecordPointer.Size)
	}

	// Sync compact file
	if error := compactedFile.Sync(); error != nil {
		return fmt.Errorf("compact failed to sync: %w", error)
	}

	// Close files and swap
	compactedFile.Close()
	compactedFile = nil // prevent defer cleanup from deleting the swapped file

	if error := tableStorage.file.Close(); error != nil {
		return fmt.Errorf("failed to close table file for swap: %w", error)
	}
	tableStorage.file = nil

	if error := os.Rename(compactPath, tableStorage.tablePath); error != nil {
		return fmt.Errorf("failed to swap table file: %w", error)
	}

	// Reopen
	file, error := os.OpenFile(tableStorage.tablePath, os.O_CREATE|os.O_RDWR, 0644)
	if error != nil {
		return fmt.Errorf("failed to reopen table file after swap: %w", error)
	}
	tableStorage.file = file
	tableStorage.currentSize = writeOffset

	// Update activePointers in place
	for pointerKey := range activePointers {
		delete(activePointers, pointerKey)
	}
	for pointerKey, newRecordPointer := range newPointers {
		activePointers[pointerKey] = newRecordPointer
	}

	return nil
}

func (tableStorage *TableStorage) Reset() error {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()
	if tableStorage.file == nil {
		return DatabaseClosedError
	}

	if error := tableStorage.file.Truncate(0); error != nil {
		return error
	}
	if _, error := tableStorage.file.Seek(0, io.SeekStart); error != nil {
		return error
	}
	tableStorage.currentSize = 0
	return tableStorage.file.Sync()
}
