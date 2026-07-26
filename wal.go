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

	wm := &WalManager{
		walFile:      file,
		walPath:      walPath,
		durability:   durability,
		dbDir:        directory,
		writeQueue:   make(chan *WriteTask, 1000),
		closeChan:    make(chan struct{}),
		tableStorage: tableStorageResolver,
	}

	wm.wg.Add(1)
	go wm.backgroundWriterLoop()

	return wm, nil
}

func (wm *WalManager) Submit(task *WriteTask) {
	wm.writeQueue <- task
}

func (wm *WalManager) Close() error {
	select {
	case <-wm.closeChan:
		return nil // already closed
	default:
	}

	close(wm.closeChan)
	wm.wg.Wait()

	wm.mu.Lock()
	defer wm.mu.Unlock()
	if wm.walFile != nil {
		_ = wm.walFile.Sync()
		err := wm.walFile.Close()
		wm.walFile = nil
		return err
	}
	return nil
}

func (wm *WalManager) Truncate() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if wm.walFile == nil {
		return ErrClosed
	}

	if err := wm.walFile.Truncate(0); err != nil {
		return err
	}
	if _, err := wm.walFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return wm.walFile.Sync()
}

func (wm *WalManager) AppendRollbackMarker(txID int64, gen int64, reason string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if wm.walFile == nil {
		return ErrClosed
	}

	rec := WalRecord{
		Type:          OpRollbackTransaction,
		TransactionID: txID,
		Generation:    gen,
		Payload:       []byte(reason),
	}

	var buf bytes.Buffer
	if err := wm.writeRecordToBuffer(&buf, rec); err != nil {
		return err
	}

	if _, err := wm.walFile.Write(buf.Bytes()); err != nil {
		return err
	}

	if wm.durability == DurabilitySync {
		return wm.walFile.Sync()
	}
	return nil
}

func (wm *WalManager) writeRecordToBuffer(w io.Writer, rec WalRecord) error {
	// CRC computed over Type (1 byte) + TxID (8 bytes) + Gen (8 bytes) + Payload (N bytes)
	crcTable := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(crcTable)

	typeByte := byte(rec.Type)
	_, _ = hash.Write([]byte{typeByte})

	var temp [16]byte
	binary.BigEndian.PutUint64(temp[0:8], uint64(rec.TransactionID))
	binary.BigEndian.PutUint64(temp[8:16], uint64(rec.Generation))
	_, _ = hash.Write(temp[:])

	if len(rec.Payload) > 0 {
		_, _ = hash.Write(rec.Payload)
	}
	checksum := hash.Sum32()

	if err := binary.Write(w, binary.BigEndian, int32(WalMagic)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, typeByte); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, rec.TransactionID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, rec.Generation); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, int32(len(rec.Payload))); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, int32(checksum)); err != nil {
		return err
	}
	if len(rec.Payload) > 0 {
		_, err := w.Write(rec.Payload)
		return err
	}
	return nil
}

func (wm *WalManager) ReadAllRecords() ([]WalRecord, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, err := wm.walFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []WalRecord
	crcTable := crc32.MakeTable(crc32.Castagnoli)

	for {
		var magic int32
		err := binary.Read(wm.walFile, binary.BigEndian, &magic)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if magic != WalMagic {
			// Check if we are at the end (truncated write)
			curr, _ := wm.walFile.Seek(0, io.SeekCurrent)
			size, _ := wm.walFile.Seek(0, io.SeekEnd)
			if curr >= size-4 {
				break
			}
			return nil, fmt.Errorf("corrupt WAL: magic bytes mismatch")
		}

		var typeByte byte
		var txID int64
		var gen int64
		var payloadLen int32
		var checksum int32

		if err := binary.Read(wm.walFile, binary.BigEndian, &typeByte); err != nil {
			break
		}
		if err := binary.Read(wm.walFile, binary.BigEndian, &txID); err != nil {
			break
		}
		if err := binary.Read(wm.walFile, binary.BigEndian, &gen); err != nil {
			break
		}
		if err := binary.Read(wm.walFile, binary.BigEndian, &payloadLen); err != nil {
			break
		}
		if err := binary.Read(wm.walFile, binary.BigEndian, &checksum); err != nil {
			break
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(wm.walFile, payload); err != nil {
			// Truncated payload at end of file - safe to ignore
			break
		}

		// Verify Checksum
		hash := crc32.New(crcTable)
		_, _ = hash.Write([]byte{typeByte})
		var temp [16]byte
		binary.BigEndian.PutUint64(temp[0:8], uint64(txID))
		binary.BigEndian.PutUint64(temp[8:16], uint64(gen))
		_, _ = hash.Write(temp[:])
		if len(payload) > 0 {
			_, _ = hash.Write(payload)
		}

		if hash.Sum32() != uint32(checksum) {
			// If not at the end of the file, this is middle corruption
			curr, _ := wm.walFile.Seek(0, io.SeekCurrent)
			size, _ := wm.walFile.Seek(0, io.SeekEnd)
			if curr >= size {
				break
			}
			return nil, fmt.Errorf("corrupt WAL: checksum mismatch")
		}

		records = append(records, WalRecord{
			Type:          WalOperation(typeByte),
			TransactionID: txID,
			Generation:    gen,
			Payload:       payload,
		})
	}

	return records, nil
}

func (wm *WalManager) backgroundWriterLoop() {
	defer wm.wg.Done()

	for {
		select {
		case <-wm.closeChan:
			// Process any remaining tasks
			for len(wm.writeQueue) > 0 {
				task := <-wm.writeQueue
				wm.processTasks([]*WriteTask{task})
			}
			return
		case task := <-wm.writeQueue:
			var tasks []*WriteTask
			tasks = append(tasks, task)

			// If BATCHED, gather tasks for up to 10ms
			if wm.durability == DurabilityBatched {
				deadline := time.NewTimer(10 * time.Millisecond)
			gatherLoop:
				for len(tasks) < 100 {
					select {
					case nextTask := <-wm.writeQueue:
						tasks = append(tasks, nextTask)
					case <-deadline.C:
						break gatherLoop
					case <-wm.closeChan:
						break gatherLoop
					}
				}
				deadline.Stop()
			}

			wm.processTasks(tasks)
		}
	}
}

func (wm *WalManager) processTasks(tasks []*WriteTask) {
	if len(tasks) == 0 {
		return
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()

	var buf bytes.Buffer
	var lastErr error
	modifiedTables := make(map[string]*TableStorage)

	// Step 1: Write all WAL records to buffer
	for _, task := range tasks {
		// BEGIN
		beginRec := WalRecord{
			Type:          OpBeginTransaction,
			TransactionID: task.TxID,
			Generation:    task.Generation,
		}
		if err := wm.writeRecordToBuffer(&buf, beginRec); err != nil {
			lastErr = err
			break
		}

		// Mutations
		for _, rec := range task.WalRecords {
			if err := wm.writeRecordToBuffer(&buf, rec); err != nil {
				lastErr = err
				break
			}
		}

		// COMMIT
		commitRec := WalRecord{
			Type:          OpCommitTransaction,
			TransactionID: task.TxID,
			Generation:    task.Generation,
		}
		if err := wm.writeRecordToBuffer(&buf, commitRec); err != nil {
			lastErr = err
			break
		}
	}

	// Step 2: Write WAL buffer to file
	if lastErr == nil && buf.Len() > 0 {
		if _, err := wm.walFile.Write(buf.Bytes()); err != nil {
			lastErr = err
		}
	}

	// Step 3: Write table storage appends
	taskResults := make([]WriteResult, len(tasks))
	for idx, task := range tasks {
		if lastErr != nil {
			taskResults[idx] = WriteResult{Err: lastErr}
			continue
		}

		ptrsMap := make(map[string][]RecordPointer)
		var taskErr error

		for tableName, appends := range task.TableAppends {
			storage, err := wm.tableStorage(tableName)
			if err != nil {
				taskErr = err
				break
			}
			modifiedTables[tableName] = storage

			ptrs, err := storage.AppendRecords(appends)
			if err != nil {
				taskErr = err
				break
			}
			ptrsMap[tableName] = ptrs
		}

		if taskErr != nil {
			lastErr = taskErr
			taskResults[idx] = WriteResult{Err: taskErr}
		} else {
			taskResults[idx] = WriteResult{Pointers: ptrsMap}
		}
	}

	// Step 4: Sync files if SYNC or BATCHED
	if lastErr == nil && (wm.durability == DurabilitySync || wm.durability == DurabilityBatched) {
		if err := wm.walFile.Sync(); err != nil {
			lastErr = err
		}
		for _, storage := range modifiedTables {
			storage.mu.Lock()
			if storage.file != nil {
				if err := storage.file.Sync(); err != nil {
					lastErr = err
				}
			}
			storage.mu.Unlock()
		}
	}

	// Step 5: Complete task done channels
	for idx, task := range tasks {
		if lastErr != nil {
			task.Done <- WriteResult{Err: lastErr}
		} else {
			task.Done <- taskResults[idx]
		}
	}
}
