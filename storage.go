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
		return nil, ErrInvalidTableName
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	tablePath := filepath.Join(directory, tableName+".db")
	file, err := os.OpenFile(tablePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open table file %s: %w", tablePath, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat table file %s: %w", tablePath, err)
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

	offset := tableStorage.currentSize
	written, err := tableStorage.file.WriteAt(bytesValue, offset)
	if err != nil {
		return RecordPointer{}, err
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
		_, err := tableStorage.file.WriteAt(buffer, tableStorage.currentSize)
		if err != nil {
			return nil, err
		}
	}

	tableStorage.currentSize = offset
	return recordPointers, nil
}

func (tableStorage *TableStorage) ReadRecord(recordPointer RecordPointer) ([]byte, error) {
	buffer := make([]byte, recordPointer.Size)
	_, err := tableStorage.file.ReadAt(buffer, recordPointer.Offset)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("unexpected EOF reading record at offset %d, size %d", recordPointer.Offset, recordPointer.Size)
		}
		return nil, err
	}
	return buffer, nil
}

func (tableStorage *TableStorage) Close() error {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()
	if tableStorage.file != nil {
		err := tableStorage.file.Close()
		tableStorage.file = nil
		return err
	}
	return nil
}

func (tableStorage *TableStorage) Compact(activePointers map[any]RecordPointer) error {
	tableStorage.mu.Lock()
	defer tableStorage.mu.Unlock()

	compactPath := tableStorage.tablePath + ".compact"
	_ = os.Remove(compactPath)

	compactedFile, err := os.OpenFile(compactPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open compact file: %w", err)
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
		if _, err := tableStorage.file.ReadAt(buffer, oldRecordPointer.Offset); err != nil {
			return fmt.Errorf("compact failed to read record: %w", err)
		}

		// Write to compact file
		if _, err := compactedFile.WriteAt(buffer, writeOffset); err != nil {
			return fmt.Errorf("compact failed to write record: %w", err)
		}

		newPointers[key] = RecordPointer{
			Offset: writeOffset,
			Size:   oldRecordPointer.Size,
		}
		writeOffset += int64(oldRecordPointer.Size)
	}

	// Sync compact file
	if err := compactedFile.Sync(); err != nil {
		return fmt.Errorf("compact failed to sync: %w", err)
	}

	// Close files and swap
	compactedFile.Close()
	compactedFile = nil // prevent defer cleanup from deleting the swapped file

	if err := tableStorage.file.Close(); err != nil {
		return fmt.Errorf("failed to close table file for swap: %w", err)
	}
	tableStorage.file = nil

	if err := os.Rename(compactPath, tableStorage.tablePath); err != nil {
		return fmt.Errorf("failed to swap table file: %w", err)
	}

	// Reopen
	file, err := os.OpenFile(tableStorage.tablePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen table file after swap: %w", err)
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
		return ErrClosed
	}

	if err := tableStorage.file.Truncate(0); err != nil {
		return err
	}
	if _, err := tableStorage.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	tableStorage.currentSize = 0
	return tableStorage.file.Sync()
}
