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
	Error      error
}

type WalManager struct {
	mu           sync.Mutex
	walFile      *os.File
	walPath      string
	durability   DurabilityMode
	dbDirectory        string
	writeQueue   chan *WriteTask
	closeChan    chan struct{}
	wg           sync.WaitGroup
	tableStorage func(string) (*TableStorage, error)
}

func NewWalManager(directory string, durability DurabilityMode, tableStorageResolver func(string) (*TableStorage, error)) (*WalManager, error) {
	if error := os.MkdirAll(directory, 0755); error != nil {
		return nil, fmt.Errorf("failed to create directory: %w", error)
	}

	walPath := filepath.Join(directory, "wal.log")
	file, error := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0644)
	if error != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", error)
	}

	// Seek to end
	if _, error := file.Seek(0, io.SeekEnd); error != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek WAL: %w", error)
	}

	walManager := &WalManager{
		walFile:      file,
		walPath:      walPath,
		durability:   durability,
		dbDirectory:        directory,
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
		task.Done <- WriteResult{Error: DatabaseClosedError}
	default:
		select {
		case <-walManager.closeChan:
			task.Done <- WriteResult{Error: DatabaseClosedError}
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
		error := walManager.walFile.Close()
		walManager.walFile = nil
		return error
	}
	return nil
}

func (walManager *WalManager) Truncate() error {
	walManager.mu.Lock()
	defer walManager.mu.Unlock()
	if walManager.walFile == nil {
		return DatabaseClosedError
	}

	if error := walManager.walFile.Truncate(0); error != nil {
		return error
	}
	if _, error := walManager.walFile.Seek(0, io.SeekStart); error != nil {
		return error
	}
	return walManager.walFile.Sync()
}

func (walManager *WalManager) AppendRollbackMarker(transactionID int64, generation int64, reason string) error {
	walManager.mu.Lock()
	defer walManager.mu.Unlock()
	if walManager.walFile == nil {
		return DatabaseClosedError
	}

	record := WalRecord{
		Type:          OpRollbackTransaction,
		TransactionID: transactionID,
		Generation:    generation,
		Payload:       []byte(reason),
	}

	var buffer bytes.Buffer
	if error := walManager.writeRecordToBuffer(&buffer, record); error != nil {
		return error
	}

	if _, error := walManager.walFile.Write(buffer.Bytes()); error != nil {
		return error
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

	if error := binary.Write(writer, binary.BigEndian, int32(WalMagic)); error != nil {
		return error
	}
	if error := binary.Write(writer, binary.BigEndian, typeByte); error != nil {
		return error
	}
	if error := binary.Write(writer, binary.BigEndian, record.TransactionID); error != nil {
		return error
	}
	if error := binary.Write(writer, binary.BigEndian, record.Generation); error != nil {
		return error
	}
	if error := binary.Write(writer, binary.BigEndian, int32(len(record.Payload))); error != nil {
		return error
	}
	if error := binary.Write(writer, binary.BigEndian, int32(checksum)); error != nil {
		return error
	}
	if len(record.Payload) > 0 {
		_, error := writer.Write(record.Payload)
		return error
	}
	return nil
}

func (walManager *WalManager) ReadAllRecords() ([]WalRecord, error) {
	walManager.mu.Lock()
	defer walManager.mu.Unlock()

	if _, error := walManager.walFile.Seek(0, io.SeekStart); error != nil {
		return nil, error
	}

	var records []WalRecord
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	for {
		var magic int32
		error := binary.Read(walManager.walFile, binary.BigEndian, &magic)
		if error == io.EOF {
			break
		}
		if error != nil {
			return nil, error
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

		if error := binary.Read(walManager.walFile, binary.BigEndian, &typeByteValue); error != nil {
			break
		}
		if typeByteValue > 11 {
			return nil, fmt.Errorf("corrupt WAL: unknown operation type 0x%X", typeByteValue)
		}
		if error := binary.Read(walManager.walFile, binary.BigEndian, &transactionID); error != nil {
			break
		}
		if error := binary.Read(walManager.walFile, binary.BigEndian, &generation); error != nil {
			break
		}
		if error := binary.Read(walManager.walFile, binary.BigEndian, &payloadLength); error != nil {
			break
		}
		if error := binary.Read(walManager.walFile, binary.BigEndian, &checksumValue); error != nil {
			break
		}

		if payloadLength < 0 || payloadLength > 1024*1024*64 {
			return nil, fmt.Errorf("corrupt WAL: invalid payload length %d", payloadLength)
		}
		currentOffset, _ := walManager.walFile.Seek(0, io.SeekCurrent)
		fileInfo, err := walManager.walFile.Stat()
		if err == nil {
			remainingFileBytes := fileInfo.Size() - currentOffset
			if int64(payloadLength) > remainingFileBytes {
				// Torn write at the end of the file: break to stop reading, but don't return error
				break
			}
		}

		payload := make([]byte, payloadLength)
		if _, error := io.ReadFull(walManager.walFile, payload); error != nil {
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

	var mutationBuffer bytes.Buffer
	var lastError error
	modifiedTables := make(map[string]*TableStorage)

	// Step 1: Write all transaction mutations (BEGIN + WalRecords) to WAL buffer
	for _, writeTask := range tasks {
		beginRecord := WalRecord{
			Type:          OpBeginTransaction,
			TransactionID: writeTask.TxID,
			Generation:    writeTask.Generation,
		}
		if error := walManager.writeRecordToBuffer(&mutationBuffer, beginRecord); error != nil {
			lastError = error
			break
		}

		for _, record := range writeTask.WalRecords {
			if error := walManager.writeRecordToBuffer(&mutationBuffer, record); error != nil {
				lastError = error
				break
			}
		}
	}

	// Step 2: Write mutations WAL buffer to file and sync WAL
	if lastError == nil && mutationBuffer.Len() > 0 {
		if _, error := walManager.walFile.Write(mutationBuffer.Bytes()); error != nil {
			lastError = error
		} else if walManager.durability == DurabilitySync || walManager.durability == DurabilityBatched {
			if error := walManager.walFile.Sync(); error != nil {
				lastError = error
			}
		}
	}

	// Step 3: Write table storage appends
	taskResults := make([]WriteResult, len(tasks))
	for index, writeTask := range tasks {
		if lastError != nil {
			taskResults[index] = WriteResult{Error: lastError}
			continue
		}

		pointersMap := make(map[string][]RecordPointer)
		var taskError error

		for tableName, appends := range writeTask.TableAppends {
			tableStorageValue, error := walManager.tableStorage(tableName)
			if error != nil {
				taskError = error
				break
			}
			modifiedTables[tableName] = tableStorageValue

			recordPointers, error := tableStorageValue.AppendRecords(appends)
			if error != nil {
				taskError = error
				break
			}
			pointersMap[tableName] = recordPointers
		}

		if taskError != nil {
			taskResults[index] = WriteResult{Error: taskError}
		} else {
			taskResults[index] = WriteResult{Pointers: pointersMap}
		}
	}

	// Step 4: Sync table files
	if lastError == nil && (walManager.durability == DurabilitySync || walManager.durability == DurabilityBatched) {
		for _, tableStorageValue := range modifiedTables {
			tableStorageValue.mu.Lock()
			if tableStorageValue.file != nil {
				if error := tableStorageValue.file.Sync(); error != nil {
					lastError = error
				}
			}
			tableStorageValue.mu.Unlock()
		}
	}

	// Step 5: Write COMMIT or ROLLBACK markers to WAL buffer
	var commitBuffer bytes.Buffer
	for index, writeTask := range tasks {
		if lastError == nil && taskResults[index].Error == nil {
			commitRecord := WalRecord{
				Type:          OpCommitTransaction,
				TransactionID: writeTask.TxID,
				Generation:    writeTask.Generation,
			}
			if error := walManager.writeRecordToBuffer(&commitBuffer, commitRecord); error != nil {
				lastError = error
			}
		} else {
			rollbackRecord := WalRecord{
				Type:          OpRollbackTransaction,
				TransactionID: writeTask.TxID,
				Generation:    writeTask.Generation,
			}
			_ = walManager.writeRecordToBuffer(&commitBuffer, rollbackRecord)
		}
	}

	// Step 6: Write commit markers to WAL file and sync WAL
	if commitBuffer.Len() > 0 {
		if _, error := walManager.walFile.Write(commitBuffer.Bytes()); error != nil {
			lastError = error
		} else if walManager.durability == DurabilitySync || walManager.durability == DurabilityBatched {
			if error := walManager.walFile.Sync(); error != nil {
				lastError = error
			}
		}
	}

	// Step 7: Complete task done channels
	for index, writeTask := range tasks {
		if lastError != nil {
			writeTask.Done <- WriteResult{Error: lastError}
		} else {
			writeTask.Done <- taskResults[index]
		}
	}
}
