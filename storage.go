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
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	tablePath := filepath.Join(directory, tableName+".db")
	file, err := os.OpenFile(tablePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open table file %s: %w", tablePath, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat table file %s: %w", tablePath, err)
	}

	return &TableStorage{
		file:        file,
		tablePath:   tablePath,
		currentSize: info.Size(),
	}, nil
}

func (s *TableStorage) AppendRecord(bytes []byte) (RecordPointer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.currentSize
	written, err := s.file.WriteAt(bytes, offset)
	if err != nil {
		return RecordPointer{}, err
	}

	ptr := RecordPointer{
		Offset: offset,
		Size:   int32(written),
	}
	s.currentSize += int64(written)
	return ptr, nil
}

func (s *TableStorage) AppendRecords(records [][]byte) ([]RecordPointer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var totalSize int
	for _, r := range records {
		totalSize += len(r)
	}

	buf := make([]byte, 0, totalSize)
	var ptrs []RecordPointer
	offset := s.currentSize

	for _, r := range records {
		buf = append(buf, r...)
		ptrs = append(ptrs, RecordPointer{
			Offset: offset,
			Size:   int32(len(r)),
		})
		offset += int64(len(r))
	}

	if len(buf) > 0 {
		_, err := s.file.WriteAt(buf, s.currentSize)
		if err != nil {
			return nil, err
		}
	}

	s.currentSize = offset
	return ptrs, nil
}

func (s *TableStorage) ReadRecord(ptr RecordPointer) ([]byte, error) {
	buf := make([]byte, ptr.Size)
	_, err := s.file.ReadAt(buf, ptr.Offset)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("unexpected EOF reading record at offset %d, size %d", ptr.Offset, ptr.Size)
		}
		return nil, err
	}
	return buf, nil
}

func (s *TableStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}

func (s *TableStorage) Compact(activePointers map[any]RecordPointer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	compactPath := s.tablePath + ".compact"
	_ = os.Remove(compactPath)

	compactFile, err := os.OpenFile(compactPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open compact file: %w", err)
	}
	defer func() {
		if compactFile != nil {
			compactFile.Close()
			_ = os.Remove(compactPath)
		}
	}()

	var writeOffset int64
	newPointers := make(map[any]RecordPointer)

	for key, oldPtr := range activePointers {
		// Read record
		buf := make([]byte, oldPtr.Size)
		if _, err := s.file.ReadAt(buf, oldPtr.Offset); err != nil {
			return fmt.Errorf("compact failed to read record: %w", err)
		}

		// Write to compact file
		if _, err := compactFile.WriteAt(buf, writeOffset); err != nil {
			return fmt.Errorf("compact failed to write record: %w", err)
		}

		newPointers[key] = RecordPointer{
			Offset: writeOffset,
			Size:   oldPtr.Size,
		}
		writeOffset += int64(oldPtr.Size)
	}

	// Sync compact file
	if err := compactFile.Sync(); err != nil {
		return fmt.Errorf("compact failed to sync: %w", err)
	}

	// Close files and swap
	compactFile.Close()
	compactFile = nil // prevent defer cleanup from deleting the swapped file

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close table file for swap: %w", err)
	}
	s.file = nil

	if err := os.Rename(compactPath, s.tablePath); err != nil {
		return fmt.Errorf("failed to swap table file: %w", err)
	}

	// Reopen
	file, err := os.OpenFile(s.tablePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen table file after swap: %w", err)
	}
	s.file = file
	s.currentSize = writeOffset

	// Update activePointers in place
	for k := range activePointers {
		delete(activePointers, k)
	}
	for k, v := range newPointers {
		activePointers[k] = v
	}

	return nil
}
