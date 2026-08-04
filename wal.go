package masterkeeper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const WalMagic = 0x524d574c

type WalOperation byte

const (
	OpBeginTransaction    WalOperation = 0
	OpInsert              WalOperation = 1
	OpUpsert              WalOperation = 2
	OpUpdate              WalOperation = 3
	OpDelete              WalOperation = 4
	OpClearTable          WalOperation = 5
	OpCreateTable         WalOperation = 6
	OpDropTable           WalOperation = 7
	OpCreateIndex         WalOperation = 8
	OpDropIndex           WalOperation = 9
	OpCommitTransaction   WalOperation = 10
	OpRollbackTransaction WalOperation = 11
)

type WalRecord struct {
	Type          WalOperation
	TransactionID int64
	Generation    int64
	Payload       []byte
}

type DurabilityMode int

const (
	DurabilitySync    DurabilityMode = 0
	DurabilityBatched DurabilityMode = 1
	DurabilityAsync   DurabilityMode = 2
)

type WriteTask struct {
	TxID         int64
	Generation   int64
	WalRecords   []WalRecord
	TableAppends map[string][][]byte // tableName -> slice of serialized records
	Done         chan WriteResult
}

type WriteResult struct {
	Pointers map[string][]RecordPointer
	Err      error
}

type WalManager struct {
	mu           sync.Mutex
	walFile      *os.File
	walPath      string
	durability   DurabilityMode
	dbDir        string
	writeQueue   chan *WriteTask
	closeChan    chan struct{}
	wg           sync.WaitGroup
	tableStorage func(string) (*TableStorage, error)
}

func NewWalManager(directory string, durability DurabilityMode, tableStorageResolver func(string) (*TableStorage, error)) (*WalManager, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	walPath := filepath.Join(directory, "wal.log")
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Seek to end
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek WAL: %w", err)
	}

	walManager := &WalManager{
		walFile:      file,
		walPath:      walPath,
		durability:   durability,
		dbDir:        directory,
		writeQueue:   make(chan *WriteTask, 1000),
		closeChan:    make(chan struct{}),
		tableStorage: tableStorageResolver,
	}

	walManager.wg.Add(1)
	go walManager.backgroundWriterLoop()

	return walManager, nil
}

func (walManager *WalManager) Submit(task *WriteTask) {
	select {
	case <-walManager.closeChan:
		task.Done <- WriteResult{Err: ErrClosed}
	default:
		select {
		case <-walManager.closeChan:
			task.Done <- WriteResult{Err: ErrClosed}
		case walManager.writeQueue <- task:
		}
	}
}

func (walManager *WalManager) Close() error {
	select {
	case <-walManager.closeChan:
		return nil // already closed
	default:
	}

	close(walManager.closeChan)
	walManager.wg.Wait()

	walManager.mu.Lock()
	defer walManager.mu.Unlock()
	if walManager.walFile != nil {
		_ = walManager.walFile.Sync()
		err := walManager.walFile.Close()
		walManager.walFile = nil
		return err
	}
	return nil
}

func (walManager *WalManager) Truncate() error {
	walManager.mu.Lock()
	defer walManager.mu.Unlock()
	if walManager.walFile == nil {
		return ErrClosed
	}

	if err := walManager.walFile.Truncate(0); err != nil {
		return err
	}
	if _, err := walManager.walFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return walManager.walFile.Sync()
}

func (walManager *WalManager) AppendRollbackMarker(transactionID int64, generation int64, reason string) error {
	walManager.mu.Lock()
	defer walManager.mu.Unlock()
	if walManager.walFile == nil {
		return ErrClosed
	}

	record := WalRecord{
		Type:          OpRollbackTransaction,
		TransactionID: transactionID,
		Generation:    generation,
		Payload:       []byte(reason),
	}

	var buffer bytes.Buffer
	if err := walManager.writeRecordToBuffer(&buffer, record); err != nil {
		return err
	}

	if _, err := walManager.walFile.Write(buffer.Bytes()); err != nil {
		return err
	}

	if walManager.durability == DurabilitySync {
		return walManager.walFile.Sync()
	}
	return nil
}

func (walManager *WalManager) writeRecordToBuffer(writer io.Writer, record WalRecord) error {
	// CRC computed over Type (1 byte) + TxID (8 bytes) + Gen (8 bytes) + Payload (N bytes)
	crcTable := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(crcTable)

	typeByte := byte(record.Type)
	_, _ = hash.Write([]byte{typeByte})

	var temp [16]byte
	binary.BigEndian.PutUint64(temp[0:8], uint64(record.TransactionID))
	binary.BigEndian.PutUint64(temp[8:16], uint64(record.Generation))
	_, _ = hash.Write(temp[:])

	if len(record.Payload) > 0 {
		_, _ = hash.Write(record.Payload)
	}
	checksum := hash.Sum32()

	if err := binary.Write(writer, binary.BigEndian, int32(WalMagic)); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, typeByte); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, record.TransactionID); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, record.Generation); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, int32(len(record.Payload))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, int32(checksum)); err != nil {
		return err
	}
	if len(record.Payload) > 0 {
		_, err := writer.Write(record.Payload)
		return err
	}
	return nil
}

func (walManager *WalManager) ReadAllRecords() ([]WalRecord, error) {
	walManager.mu.Lock()
	defer walManager.mu.Unlock()

	if _, err := walManager.walFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []WalRecord
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	for {
		var magic int32
		err := binary.Read(walManager.walFile, binary.BigEndian, &magic)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if magic != WalMagic {
			// Check if we are at the end (truncated write)
			currentOffset, _ := walManager.walFile.Seek(0, io.SeekCurrent)
			fileSize, _ := walManager.walFile.Seek(0, io.SeekEnd)
			if currentOffset >= fileSize-4 {
				break
			}
			return nil, fmt.Errorf("corrupt WAL: magic bytes mismatch")
		}

		var typeByteValue byte
		var transactionID int64
		var generation int64
		var payloadLength int32
		var checksumValue int32

		if err := binary.Read(walManager.walFile, binary.BigEndian, &typeByteValue); err != nil {
			break
		}
		if err := binary.Read(walManager.walFile, binary.BigEndian, &transactionID); err != nil {
			break
		}
		if err := binary.Read(walManager.walFile, binary.BigEndian, &generation); err != nil {
			break
		}
		if err := binary.Read(walManager.walFile, binary.BigEndian, &payloadLength); err != nil {
			break
		}
		if err := binary.Read(walManager.walFile, binary.BigEndian, &checksumValue); err != nil {
			break
		}

		payload := make([]byte, payloadLength)
		if _, err := io.ReadFull(walManager.walFile, payload); err != nil {
			// Truncated payload at end of file - safe to ignore
			break
		}

		// Verify Checksum
		hash := crc32.New(crcTable)
		_, _ = hash.Write([]byte{typeByteValue})
		var temp [16]byte
		binary.BigEndian.PutUint64(temp[0:8], uint64(transactionID))
		binary.BigEndian.PutUint64(temp[8:16], uint64(generation))
		_, _ = hash.Write(temp[:])
		if len(payload) > 0 {
			_, _ = hash.Write(payload)
		}

		if hash.Sum32() != uint32(checksumValue) {
			// If not at the end of the file, this is middle corruption
			currentOffset, _ := walManager.walFile.Seek(0, io.SeekCurrent)
			fileSize, _ := walManager.walFile.Seek(0, io.SeekEnd)
			if currentOffset >= fileSize {
				break
			}
			return nil, fmt.Errorf("corrupt WAL: checksum mismatch")
		}

		records = append(records, WalRecord{
			Type:          WalOperation(typeByteValue),
			TransactionID: transactionID,
			Generation:    generation,
			Payload:       payload,
		})
	}

	return records, nil
}

func (walManager *WalManager) backgroundWriterLoop() {
	defer walManager.wg.Done()

	for {
		select {
		case <-walManager.closeChan:
			// Process any remaining tasks
			for len(walManager.writeQueue) > 0 {
				writeTask := <-walManager.writeQueue
				walManager.processTasks([]*WriteTask{writeTask})
			}
			return
		case writeTask := <-walManager.writeQueue:
			var tasks []*WriteTask
			tasks = append(tasks, writeTask)

			// If BATCHED, gather tasks for up to 10ms
			if walManager.durability == DurabilityBatched {
				deadlineTimer := time.NewTimer(10 * time.Millisecond)
			gatherLoop:
				for len(tasks) < 100 {
					select {
					case nextWriteTask := <-walManager.writeQueue:
						tasks = append(tasks, nextWriteTask)
					case <-deadlineTimer.C:
						break gatherLoop
					case <-walManager.closeChan:
						break gatherLoop
					}
				}
				deadlineTimer.Stop()
			}

			walManager.processTasks(tasks)
		}
	}
}

func (walManager *WalManager) processTasks(tasks []*WriteTask) {
	if len(tasks) == 0 {
		return
	}

	walManager.mu.Lock()
	defer walManager.mu.Unlock()

	var buffer bytes.Buffer
	var lastErr error
	modifiedTables := make(map[string]*TableStorage)

	// Step 1: Write all WAL records to buffer
	for _, writeTask := range tasks {
		// BEGIN
		beginRecord := WalRecord{
			Type:          OpBeginTransaction,
			TransactionID: writeTask.TxID,
			Generation:    writeTask.Generation,
		}
		if err := walManager.writeRecordToBuffer(&buffer, beginRecord); err != nil {
			lastErr = err
			break
		}

		// Mutations
		for _, record := range writeTask.WalRecords {
			if err := walManager.writeRecordToBuffer(&buffer, record); err != nil {
				lastErr = err
				break
			}
		}

		// COMMIT
		commitRecord := WalRecord{
			Type:          OpCommitTransaction,
			TransactionID: writeTask.TxID,
			Generation:    writeTask.Generation,
		}
		if err := walManager.writeRecordToBuffer(&buffer, commitRecord); err != nil {
			lastErr = err
			break
		}
	}

	// Step 2: Write WAL buffer to file
	if lastErr == nil && buffer.Len() > 0 {
		if _, err := walManager.walFile.Write(buffer.Bytes()); err != nil {
			lastErr = err
		}
	}

	// Step 3: Write table storage appends
	taskResults := make([]WriteResult, len(tasks))
	for index, writeTask := range tasks {
		if lastErr != nil {
			taskResults[index] = WriteResult{Err: lastErr}
			continue
		}

		pointersMap := make(map[string][]RecordPointer)
		var taskErr error

		for tableName, appends := range writeTask.TableAppends {
			tableStorageVal, err := walManager.tableStorage(tableName)
			if err != nil {
				taskErr = err
				break
			}
			modifiedTables[tableName] = tableStorageVal

			recordPointers, err := tableStorageVal.AppendRecords(appends)
			if err != nil {
				taskErr = err
				break
			}
			pointersMap[tableName] = recordPointers
		}

		if taskErr != nil {
			lastErr = taskErr
			taskResults[index] = WriteResult{Err: taskErr}
		} else {
			taskResults[index] = WriteResult{Pointers: pointersMap}
		}
	}

	// Step 4: Sync files if SYNC or BATCHED
	if lastErr == nil && (walManager.durability == DurabilitySync || walManager.durability == DurabilityBatched) {
		if err := walManager.walFile.Sync(); err != nil {
			lastErr = err
		}
		for _, tableStorageVal := range modifiedTables {
			tableStorageVal.mu.Lock()
			if tableStorageVal.file != nil {
				if err := tableStorageVal.file.Sync(); err != nil {
					lastErr = err
				}
			}
			tableStorageVal.mu.Unlock()
		}
	}

	// Step 5: Complete task done channels
	for index, writeTask := range tasks {
		if lastErr != nil {
			writeTask.Done <- WriteResult{Err: lastErr}
		} else {
			writeTask.Done <- taskResults[index]
		}
	}
}
