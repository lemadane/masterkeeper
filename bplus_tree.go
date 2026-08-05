package masterkeeper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
)

const IndexPageSize = 16 * 1024 // 16 KiB
type PageID uint64

type PageHeader struct {
	Magic        uint32
	Version      uint16
	PageType     uint8
	Flags        uint8
	PageID       PageID
	Generation   uint64
	CellCount    uint16
	FreeStart    uint16
	FreeEnd      uint16
	RightSibling PageID
	Checksum     uint32
}

const (
	PageTypeInternal = 1
	PageTypeLeaf     = 2
	PageTypeOverflow = 3
	PageTypeCatalog  = 4
)

var InvalidPageSizeError = errors.New("invalid page size")

func pageOffset(pageID PageID) int64 {
	return int64(pageID) * IndexPageSize
}

func readPage(file *os.File, pageID PageID) ([]byte, error) {
	page := make([]byte, IndexPageSize)

	bytesRead, readError := file.ReadAt(page, pageOffset(pageID))
	if readError != nil && readError != io.EOF {
		return nil, readError
	}
	if bytesRead != len(page) {
		return nil, io.ErrUnexpectedEOF
	}

	if validationError := validateChecksum(page); validationError != nil {
		return nil, validationError
	}

	return page, nil
}

func writePage(file *os.File, pageID PageID, page []byte) error {
	if len(page) != IndexPageSize {
		return InvalidPageSizeError
	}

	updateChecksum(page)

	bytesWritten, writeError := file.WriteAt(page, pageOffset(pageID))
	if writeError != nil {
		return writeError
	}
	if bytesWritten != len(page) {
		return io.ErrShortWrite
	}

	return nil
}

func writeHeader(buffer []byte, header PageHeader) {
	binary.BigEndian.PutUint32(buffer[0:4], header.Magic)
	binary.BigEndian.PutUint16(buffer[4:6], header.Version)
	buffer[6] = header.PageType
	buffer[7] = header.Flags
	binary.BigEndian.PutUint64(buffer[8:16], uint64(header.PageID))
	binary.BigEndian.PutUint64(buffer[16:24], header.Generation)
	binary.BigEndian.PutUint16(buffer[24:26], header.CellCount)
	binary.BigEndian.PutUint16(buffer[26:28], header.FreeStart)
	binary.BigEndian.PutUint16(buffer[28:30], header.FreeEnd)
	binary.BigEndian.PutUint64(buffer[30:38], uint64(header.RightSibling))
	binary.BigEndian.PutUint32(buffer[38:42], header.Checksum)
	for index := 42; index < 48; index++ {
		buffer[index] = 0
	}
}

func readHeader(buffer []byte) PageHeader {
	return PageHeader{
		Magic:        binary.BigEndian.Uint32(buffer[0:4]),
		Version:      binary.BigEndian.Uint16(buffer[4:6]),
		PageType:     buffer[6],
		Flags:        buffer[7],
		PageID:       PageID(binary.BigEndian.Uint64(buffer[8:16])),
		Generation:   binary.BigEndian.Uint64(buffer[16:24]),
		CellCount:    binary.BigEndian.Uint16(buffer[24:26]),
		FreeStart:    binary.BigEndian.Uint16(buffer[26:28]),
		FreeEnd:      binary.BigEndian.Uint16(buffer[28:30]),
		RightSibling: PageID(binary.BigEndian.Uint64(buffer[30:38])),
		Checksum:     binary.BigEndian.Uint32(buffer[38:42]),
	}
}

func updateChecksum(page []byte) {
	binary.BigEndian.PutUint32(page[38:42], 0)
	checksumTable := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(page, checksumTable)
	binary.BigEndian.PutUint32(page[38:42], checksum)
}

func validateChecksum(page []byte) error {
	storedChecksum := binary.BigEndian.Uint32(page[38:42])
	binary.BigEndian.PutUint32(page[38:42], 0)
	checksumTable := crc32.MakeTable(crc32.Castagnoli)
	computedChecksum := crc32.Checksum(page, checksumTable)
	binary.BigEndian.PutUint32(page[38:42], storedChecksum)
	if computedChecksum != storedChecksum {
		return errors.New("page checksum verification failed")
	}
	return nil
}

type Superblock struct {
	Magic             uint32
	Version           uint16
	PageSize          uint32
	Generation        uint64
	CatalogRootPageID PageID
	NextPageID        PageID
	Checksum          uint32
}

func serializeSuperblock(superblock Superblock) ([]byte, error) {
	page := make([]byte, IndexPageSize)
	binary.BigEndian.PutUint32(page[0:4], superblock.Magic)
	binary.BigEndian.PutUint16(page[4:6], superblock.Version)
	binary.BigEndian.PutUint32(page[6:10], superblock.PageSize)
	binary.BigEndian.PutUint64(page[10:18], superblock.Generation)
	binary.BigEndian.PutUint64(page[18:26], uint64(superblock.CatalogRootPageID))
	binary.BigEndian.PutUint64(page[26:34], uint64(superblock.NextPageID))

	return page, nil
}

func deserializeSuperblock(page []byte) (Superblock, error) {
	if len(page) != IndexPageSize {
		return Superblock{}, InvalidPageSizeError
	}

	superblock := Superblock{
		Magic:             binary.BigEndian.Uint32(page[0:4]),
		Version:           binary.BigEndian.Uint16(page[4:6]),
		PageSize:          binary.BigEndian.Uint32(page[6:10]),
		Generation:        binary.BigEndian.Uint64(page[10:18]),
		CatalogRootPageID: PageID(binary.BigEndian.Uint64(page[18:26])),
		NextPageID:        PageID(binary.BigEndian.Uint64(page[26:34])),
		Checksum:          binary.BigEndian.Uint32(page[38:42]),
	}

	if superblock.Magic != 0x4d4b5342 {
		return Superblock{}, errors.New("invalid superblock magic number")
	}

	return superblock, nil
}

type IndexRoot struct {
	Name       string
	RootPageID PageID
	Generation uint64
	Unique     bool
	Ordered    bool
}

func serializeCatalog(roots []IndexRoot, freeList []PageID, pageID PageID, generation uint64) ([]byte, error) {
	page := make([]byte, IndexPageSize)
	header := PageHeader{
		Magic:      0x4d4b4341, // MKCA
		Version:    1,
		PageType:   PageTypeCatalog,
		PageID:     pageID,
		Generation: generation,
		CellCount:  uint16(len(roots)),
	}
	writeHeader(page[0:48], header)

	offset := 48
	for _, root := range roots {
		nameBytes := []byte(root.Name)
		nameLength := len(nameBytes)
		if offset+2+nameLength+8+8+1+1 > IndexPageSize-4 {
			return nil, errors.New("catalog page overflow")
		}

		binary.BigEndian.PutUint16(page[offset:offset+2], uint16(nameLength))
		copy(page[offset+2:offset+2+nameLength], nameBytes)
		offset += 2 + nameLength

		binary.BigEndian.PutUint64(page[offset:offset+8], uint64(root.RootPageID))
		offset += 8

		binary.BigEndian.PutUint64(page[offset:offset+8], root.Generation)
		offset += 8

		if root.Unique {
			page[offset] = 1
		} else {
			page[offset] = 0
		}
		offset++

		if root.Ordered {
			page[offset] = 1
		} else {
			page[offset] = 0
		}
		offset++
	}

	maxFreeIDs := (IndexPageSize - 4 - offset - 4) / 8
	if len(freeList) > maxFreeIDs {
		freeList = freeList[:maxFreeIDs]
	}

	binary.BigEndian.PutUint32(page[offset:offset+4], uint32(len(freeList)))
	offset += 4

	for _, freeID := range freeList {
		binary.BigEndian.PutUint64(page[offset:offset+8], uint64(freeID))
		offset += 8
	}

	updateChecksum(page)
	return page, nil
}

func deserializeCatalog(page []byte) ([]IndexRoot, []PageID, error) {
	if validationError := validateChecksum(page); validationError != nil {
		return nil, nil, validationError
	}

	header := readHeader(page[0:48])
	if header.Magic != 0x4d4b4341 {
		return nil, nil, errors.New("invalid catalog page magic")
	}

	var roots []IndexRoot
	offset := 48
	for index := 0; index < int(header.CellCount); index++ {
		nameLength := int(binary.BigEndian.Uint16(page[offset : offset+2]))
		offset += 2

		name := string(page[offset : offset+nameLength])
		offset += nameLength

		rootPageID := PageID(binary.BigEndian.Uint64(page[offset : offset+8]))
		offset += 8

		generation := binary.BigEndian.Uint64(page[offset : offset+8])
		offset += 8

		unique := page[offset] == 1
		offset++

		ordered := page[offset] == 1
		offset++

		roots = append(roots, IndexRoot{
			Name:       name,
			RootPageID: rootPageID,
			Generation: generation,
			Unique:     unique,
			Ordered:    ordered,
		})
	}

	var freeList []PageID
	if offset+4 <= IndexPageSize-4 {
		freeCount := int(binary.BigEndian.Uint32(page[offset : offset+4]))
		offset += 4
		if offset+8*freeCount <= IndexPageSize-4 {
			for index := 0; index < freeCount; index++ {
				freeID := binary.BigEndian.Uint64(page[offset : offset+8])
				freeList = append(freeList, PageID(freeID))
				offset += 8
			}
		}
	}

	return roots, freeList, nil
}

type BPlusNode struct {
	PageID       PageID
	PageType     uint8
	Generation   uint64
	Keys         [][]byte
	Pointers     []RecordPointer
	Children     []PageID
	RightSibling PageID
}

func (node *BPlusNode) Serialize() ([]byte, error) {
	page := make([]byte, IndexPageSize)

	header := PageHeader{
		Magic:        0x4d4b4958, // MKIX
		Version:      1,
		PageType:     node.PageType,
		PageID:       node.PageID,
		Generation:   node.Generation,
		CellCount:    uint16(len(node.Keys)),
		RightSibling: node.RightSibling,
	}

	headerSize := uint16(48)
	freeStart := headerSize + 2*header.CellCount
	freeEnd := uint16(IndexPageSize)

	offsets := make([]uint16, len(node.Keys))
	for index := 0; index < len(node.Keys); index++ {
		var cellData []byte
		if node.PageType == PageTypeLeaf {
			keyLength := len(node.Keys[index])
			cellData = make([]byte, 2+keyLength+8+4)
			binary.BigEndian.PutUint16(cellData[0:2], uint16(keyLength))
			copy(cellData[2:2+keyLength], node.Keys[index])
			binary.BigEndian.PutUint64(cellData[2+keyLength:2+keyLength+8], uint64(node.Pointers[index].Offset))
			binary.BigEndian.PutUint32(cellData[2+keyLength+8:2+keyLength+12], uint32(node.Pointers[index].Size))
		} else {
			keyLength := len(node.Keys[index])
			cellData = make([]byte, 8+2+keyLength)
			binary.BigEndian.PutUint64(cellData[0:8], uint64(node.Children[index]))
			binary.BigEndian.PutUint16(cellData[8:10], uint16(keyLength))
			copy(cellData[10:10+keyLength], node.Keys[index])
		}

		if freeEnd-freeStart < uint16(len(cellData)) {
			return nil, errors.New("page overflow during node serialization")
		}

		freeEnd -= uint16(len(cellData))
		copy(page[freeEnd:], cellData)
		offsets[index] = freeEnd
	}

	header.FreeStart = freeStart
	header.FreeEnd = freeEnd

	if node.PageType == PageTypeInternal {
		if len(node.Children) > 0 {
			header.RightSibling = node.Children[len(node.Children)-1]
		}
	}

	writeHeader(page[0:48], header)

	for index, offsetValue := range offsets {
		binary.BigEndian.PutUint16(page[headerSize+2*uint16(index):headerSize+2*uint16(index)+2], offsetValue)
	}

	updateChecksum(page)
	return page, nil
}

func DeserializeNode(page []byte) (*BPlusNode, error) {
	if validationError := validateChecksum(page); validationError != nil {
		return nil, validationError
	}

	header := readHeader(page[0:48])
	if header.Magic != 0x4d4b4958 {
		return nil, errors.New("invalid page magic number")
	}

	node := &BPlusNode{
		PageID:       header.PageID,
		PageType:     header.PageType,
		Generation:   header.Generation,
		RightSibling: header.RightSibling,
		Keys:         make([][]byte, header.CellCount),
	}

	headerSize := uint16(48)
	offsets := make([]uint16, header.CellCount)
	for index := 0; index < int(header.CellCount); index++ {
		offsets[index] = binary.BigEndian.Uint16(page[headerSize+2*uint16(index):headerSize+2*uint16(index)+2])
	}

	if header.PageType == PageTypeLeaf {
		node.Pointers = make([]RecordPointer, header.CellCount)
		for index, offsetValue := range offsets {
			keyLength := binary.BigEndian.Uint16(page[offsetValue : offsetValue+2])
			key := make([]byte, keyLength)
			copy(key, page[offsetValue+2:offsetValue+2+keyLength])
			node.Keys[index] = key

			pointerOffset := binary.BigEndian.Uint64(page[offsetValue+2+keyLength : offsetValue+2+keyLength+8])
			pointerSize := binary.BigEndian.Uint32(page[offsetValue+2+keyLength+8 : offsetValue+2+keyLength+12])
			node.Pointers[index] = RecordPointer{
				Offset: int64(pointerOffset),
				Size:   int32(pointerSize),
			}
		}
	} else {
		node.Children = make([]PageID, int(header.CellCount)+1)
		for index, offsetValue := range offsets {
			childID := binary.BigEndian.Uint64(page[offsetValue : offsetValue+8])
			node.Children[index] = PageID(childID)

			keyLength := binary.BigEndian.Uint16(page[offsetValue+8 : offsetValue+10])
			key := make([]byte, keyLength)
			copy(key, page[offsetValue+10:offsetValue+10+keyLength])
			node.Keys[index] = key
		}
		if len(node.Children) > 0 {
			node.Children[len(node.Children)-1] = header.RightSibling
		}
	}

	return node, nil
}

func compareCompositeKeys(keyFirst, keySecond []byte) (int, bool) {
	if len(keyFirst) < 4 || len(keySecond) < 4 {
		return 0, false
	}
	lenFirst := int(binary.BigEndian.Uint16(keyFirst[0:2]))
	if len(keyFirst) < 2+lenFirst+2 {
		return 0, false
	}
	lenSecond := int(binary.BigEndian.Uint16(keySecond[0:2]))
	if len(keySecond) < 2+lenSecond+2 {
		return 0, false
	}

	secFirst := keyFirst[2 : 2+lenFirst]
	secSecond := keySecond[2 : 2+lenSecond]

	comparison := compareByteKeys(secFirst, secSecond)
	if comparison != 0 {
		return comparison, true
	}

	offsetFirst := 2 + lenFirst
	primLenFirst := int(binary.BigEndian.Uint16(keyFirst[offsetFirst : offsetFirst+2]))
	if len(keyFirst) < offsetFirst+2+primLenFirst {
		return 0, false
	}
	primFirst := keyFirst[offsetFirst+2 : offsetFirst+2+primLenFirst]

	offsetSecond := 2 + lenSecond
	primLenSecond := int(binary.BigEndian.Uint16(keySecond[offsetSecond : offsetSecond+2]))
	if len(keySecond) < offsetSecond+2+primLenSecond {
		return 0, false
	}
	primSecond := keySecond[offsetSecond+2 : offsetSecond+2+primLenSecond]

	return compareByteKeys(primFirst, primSecond), true
}

func compareByteKeys(keyFirst, keySecond []byte) int {
	if len(keyFirst) == 0 && len(keySecond) == 0 {
		return 0
	}
	if len(keyFirst) == 0 {
		return -1
	}
	if len(keySecond) == 0 {
		return 1
	}

	isCompositeFirst := (len(keyFirst) > 1 && keyFirst[0] == 0) || keyFirst[0] > 7
	isCompositeSecond := (len(keySecond) > 1 && keySecond[0] == 0) || keySecond[0] > 7
	if isCompositeFirst && isCompositeSecond {
		if result, ok := compareCompositeKeys(keyFirst, keySecond); ok {
			return result
		}
	}

	if keyFirst[0] == 1 && keySecond[0] == 1 {
		if len(keyFirst) >= 5 && len(keySecond) >= 5 {
			return bytes.Compare(keyFirst[5:], keySecond[5:])
		}
	}

	if keyFirst[0] == 4 && keySecond[0] == 4 {
		if len(keyFirst) == 5 && len(keySecond) == 5 {
			valFirst := int32(binary.BigEndian.Uint32(keyFirst[1:5]))
			valSecond := int32(binary.BigEndian.Uint32(keySecond[1:5]))
			if valFirst < valSecond {
				return -1
			} else if valFirst > valSecond {
				return 1
			}
			return 0
		}
	}

	if keyFirst[0] == 5 && keySecond[0] == 5 {
		if len(keyFirst) == 9 && len(keySecond) == 9 {
			valFirst := int64(binary.BigEndian.Uint64(keyFirst[1:9]))
			valSecond := int64(binary.BigEndian.Uint64(keySecond[1:9]))
			if valFirst < valSecond {
				return -1
			} else if valFirst > valSecond {
				return 1
			}
			return 0
		}
	}

	valueFirst, errorFirst := deserializeKey(keyFirst)
	valueSecond, errorSecond := deserializeKey(keySecond)
	if errorFirst != nil || errorSecond != nil {
		return bytes.Compare(keyFirst, keySecond)
	}
	return compareValues(valueFirst, valueSecond)
}

type DiskBPlusTree struct {
	file           *os.File
	nextPageID     PageID
	generation     uint64
	catalog        map[string]IndexRoot
	catalogRoot    PageID
	nodeCache      map[PageID]*BPlusNode
	dirtyNodes     map[PageID]bool
	freeList       []PageID
	orphanedList   []PageID
	nodeCacheMutex sync.Mutex
	catalogMutex   sync.RWMutex
	mutex          sync.RWMutex
}

func NewDiskBPlusTree(file *os.File) (*DiskBPlusTree, error) {
	fileInfo, statError := file.Stat()
	if statError != nil {
		return nil, statError
	}

	tree := &DiskBPlusTree{
		file:    file,
		catalog: make(map[string]IndexRoot),
	}

	if fileInfo.Size() == 0 {
		catalogPageID := PageID(2)
		catalogBytes, serializeError := serializeCatalog(nil, nil, catalogPageID, 1)
		if serializeError != nil {
			return nil, serializeError
		}
		if writeError := writePage(file, catalogPageID, catalogBytes); writeError != nil {
			return nil, writeError
		}

		superblock := Superblock{
			Magic:             0x4d4b5342,
			Version:           1,
			PageSize:          uint32(IndexPageSize),
			Generation:        1,
			CatalogRootPageID: catalogPageID,
			NextPageID:        3,
		}
		superblockBytes, serializeError := serializeSuperblock(superblock)
		if serializeError != nil {
			return nil, serializeError
		}
		if writeError := writePage(file, 0, superblockBytes); writeError != nil {
			return nil, writeError
		}

		emptyPage := make([]byte, IndexPageSize)
		if writeError := writePage(file, 1, emptyPage); writeError != nil {
			return nil, writeError
		}

		tree.catalogRoot = catalogPageID
		tree.nextPageID = 3
		tree.generation = 1
	} else {
		var superblockA, superblockB Superblock
		var errorA, errorB error

		dataA, readErrorA := readPage(file, 0)
		if readErrorA == nil {
			superblockA, errorA = deserializeSuperblock(dataA)
		} else {
			errorA = readErrorA
		}

		dataB, readErrorB := readPage(file, 1)
		if readErrorB == nil {
			superblockB, errorB = deserializeSuperblock(dataB)
		} else {
			errorB = readErrorB
		}

		var activeSuperblock Superblock
		if errorA != nil && errorB != nil {
			return nil, errors.New("both superblocks are corrupt or invalid")
		} else if errorA != nil {
			activeSuperblock = superblockB
		} else if errorB != nil {
			activeSuperblock = superblockA
		} else {
			if superblockA.Generation >= superblockB.Generation {
				activeSuperblock = superblockA
			} else {
				activeSuperblock = superblockB
			}
		}

		catalogBytes, readError := readPage(file, activeSuperblock.CatalogRootPageID)
		if readError != nil {
			return nil, readError
		}

		roots, freeList, deserializeError := deserializeCatalog(catalogBytes)
		if deserializeError != nil {
			return nil, deserializeError
		}

		for _, root := range roots {
			tree.catalog[root.Name] = root
		}
		tree.freeList = freeList
		tree.catalogRoot = activeSuperblock.CatalogRootPageID
		tree.nextPageID = activeSuperblock.NextPageID
		tree.generation = activeSuperblock.Generation
	}

	return tree, nil
}

func (tree *DiskBPlusTree) getNode(pageID PageID) (*BPlusNode, error) {
	tree.nodeCacheMutex.Lock()
	if tree.nodeCache == nil {
		tree.nodeCache = make(map[PageID]*BPlusNode)
	}
	if node, found := tree.nodeCache[pageID]; found {
		tree.nodeCacheMutex.Unlock()
		return node, nil
	}
	tree.nodeCacheMutex.Unlock()

	data, readError := readPage(tree.file, pageID)
	if readError != nil {
		return nil, readError
	}
	node, deserializeError := DeserializeNode(data)
	if deserializeError != nil {
		return nil, deserializeError
	}

	tree.nodeCacheMutex.Lock()
	tree.nodeCache[pageID] = node
	tree.nodeCacheMutex.Unlock()
	return node, nil
}

func (tree *DiskBPlusTree) markDirty(pageID PageID) {
	tree.nodeCacheMutex.Lock()
	if tree.dirtyNodes == nil {
		tree.dirtyNodes = make(map[PageID]bool)
	}
	tree.dirtyNodes[pageID] = true
	tree.nodeCacheMutex.Unlock()
}

func (tree *DiskBPlusTree) registerNewNode(node *BPlusNode) {
	tree.nodeCacheMutex.Lock()
	if tree.nodeCache == nil {
		tree.nodeCache = make(map[PageID]*BPlusNode)
	}
	tree.nodeCache[node.PageID] = node
	if tree.dirtyNodes == nil {
		tree.dirtyNodes = make(map[PageID]bool)
	}
	tree.dirtyNodes[node.PageID] = true
	tree.nodeCacheMutex.Unlock()
}

func (tree *DiskBPlusTree) markOrphaned(pageID PageID) {
	if pageID < 3 {
		return
	}
	tree.orphanedList = append(tree.orphanedList, pageID)
}

func (tree *DiskBPlusTree) allocatePageID() PageID {
	if len(tree.freeList) > 0 {
		pageID := tree.freeList[len(tree.freeList)-1]
		tree.freeList = tree.freeList[:len(tree.freeList)-1]
		return pageID
	}
	pageID := tree.nextPageID
	tree.nextPageID++
	return pageID
}

func (tree *DiskBPlusTree) writeNewStateLocked(generation uint64) error {
	tree.catalogMutex.RLock()
	var roots []IndexRoot
	for _, root := range tree.catalog {
		roots = append(roots, root)
	}
	tree.catalogMutex.RUnlock()

	tree.markOrphaned(tree.catalogRoot)

	tree.freeList = append(tree.freeList, tree.orphanedList...)
	tree.orphanedList = nil

	// 1. Serialize and write all dirty B+ tree nodes first
	tree.nodeCacheMutex.Lock()
	dirtyNodeIDs := make([]PageID, 0, len(tree.dirtyNodes))
	for pageID := range tree.dirtyNodes {
		dirtyNodeIDs = append(dirtyNodeIDs, pageID)
	}
	nodesToWrite := make([]*BPlusNode, 0, len(dirtyNodeIDs))
	for _, pageID := range dirtyNodeIDs {
		nodesToWrite = append(nodesToWrite, tree.nodeCache[pageID])
	}
	tree.dirtyNodes = nil
	tree.nodeCacheMutex.Unlock()

	for index, pageID := range dirtyNodeIDs {
		node := nodesToWrite[index]
		if node != nil {
			serialized, serializeErrorNode := node.Serialize()
			if serializeErrorNode != nil {
				return serializeErrorNode
			}
			if writeError := writePage(tree.file, pageID, serialized); writeError != nil {
				return writeError
			}
		}
	}

	// 2. Allocate, serialize, and write the catalog page second
	catalogPageID := tree.allocatePageID()
	catalogBytes, serializeError := serializeCatalog(roots, tree.freeList, catalogPageID, generation)
	if serializeError != nil {
		return serializeError
	}

	if writeError := writePage(tree.file, catalogPageID, catalogBytes); writeError != nil {
		return writeError
	}

	// 3. Select active superblock slot and write the superblock last
	var targetPageID PageID
	var oldSuperblockA, oldSuperblockB Superblock
	var errorA, errorB error

	dataA, readErrorA := readPage(tree.file, 0)
	if readErrorA == nil {
		oldSuperblockA, errorA = deserializeSuperblock(dataA)
	} else {
		errorA = readErrorA
	}

	dataB, readErrorB := readPage(tree.file, 1)
	if readErrorB == nil {
		oldSuperblockB, errorB = deserializeSuperblock(dataB)
	} else {
		errorB = readErrorB
	}

	if errorA != nil {
		targetPageID = 0
	} else if errorB != nil {
		targetPageID = 1
	} else {
		if oldSuperblockA.Generation <= oldSuperblockB.Generation {
			targetPageID = 0
		} else {
			targetPageID = 1
		}
	}

	newSuperblock := Superblock{
		Magic:             0x4d4b5342,
		Version:           1,
		PageSize:          uint32(IndexPageSize),
		Generation:        generation,
		CatalogRootPageID: catalogPageID,
		NextPageID:        tree.nextPageID,
	}

	superblockBytes, serializeErrorSB := serializeSuperblock(newSuperblock)
	if serializeErrorSB != nil {
		return serializeErrorSB
	}

	if writeError := writePage(tree.file, targetPageID, superblockBytes); writeError != nil {
		return writeError
	}

	tree.catalogRoot = catalogPageID
	tree.generation = generation
	return nil
}

func (tree *DiskBPlusTree) FindNamed(indexName string, key []byte, generation uint64) (RecordPointer, bool, error) {
	tree.catalogMutex.RLock()
	rootEntry, found := tree.catalog[indexName]
	tree.catalogMutex.RUnlock()
	if !found {
		return RecordPointer{}, false, nil
	}

	currentPageID := rootEntry.RootPageID
	for {
		node, getNodeError := tree.getNode(currentPageID)
		if getNodeError != nil {
			return RecordPointer{}, false, getNodeError
		}

		if node.PageType == PageTypeLeaf {
			index := sort.Search(len(node.Keys), func(indexVal int) bool {
				return compareByteKeys(node.Keys[indexVal], key) >= 0
			})
			if index < len(node.Keys) && bytes.Equal(node.Keys[index], key) {
				return node.Pointers[index], true, nil
			}
			return RecordPointer{}, false, nil
		} else {
			index := sort.Search(len(node.Keys), func(indexVal int) bool {
				return compareByteKeys(node.Keys[indexVal], key) > 0
			})
			currentPageID = node.Children[index]
		}
	}
}

func (tree *DiskBPlusTree) InsertNamed(indexName string, key []byte, pointer RecordPointer, generation uint64) (PageID, error) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()

	if tree.dirtyNodes == nil {
		tree.dirtyNodes = make(map[PageID]bool)
	}
	if tree.nodeCache == nil {
		tree.nodeCache = make(map[PageID]*BPlusNode)
	}
	tree.orphanedList = nil

	newRootID, insertError := tree.insertNamedNoCommitLocked(indexName, key, pointer, generation)
	if insertError != nil {
		tree.orphanedList = nil
		return 0, insertError
	}

	if writeError := tree.writeNewStateLocked(generation); writeError != nil {
		tree.orphanedList = nil
		return 0, writeError
	}

	return newRootID, nil
}

func (tree *DiskBPlusTree) InsertNamedNoCommit(indexName string, key []byte, pointer RecordPointer, generation uint64) (PageID, error) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()

	if tree.dirtyNodes == nil {
		tree.dirtyNodes = make(map[PageID]bool)
	}
	if tree.nodeCache == nil {
		tree.nodeCache = make(map[PageID]*BPlusNode)
	}

	return tree.insertNamedNoCommitLocked(indexName, key, pointer, generation)
}

func (tree *DiskBPlusTree) insertNamedNoCommitLocked(indexName string, key []byte, pointer RecordPointer, generation uint64) (PageID, error) {
	tree.catalogMutex.Lock()
	rootEntry, found := tree.catalog[indexName]
	tree.catalogMutex.Unlock()

	var rootPageID PageID
	if !found {
		rootPageID = tree.allocatePageID()
		emptyNode := &BPlusNode{
			PageID:     rootPageID,
			PageType:   PageTypeLeaf,
			Generation: generation,
		}
		tree.registerNewNode(emptyNode)
		
		tree.catalogMutex.Lock()
		tree.catalog[indexName] = IndexRoot{
			Name:       indexName,
			RootPageID: rootPageID,
			Generation: generation,
		}
		tree.catalogMutex.Unlock()
	} else {
		rootPageID = rootEntry.RootPageID
	}

	newChildID, didSplit, separatorKey, rightChildID, insertError := tree.insertRecursive(rootPageID, key, pointer, generation)
	if insertError != nil {
		return 0, insertError
	}

	var newRootID PageID
	if didSplit {
		newRoot := &BPlusNode{
			PageID:     tree.allocatePageID(),
			PageType:   PageTypeInternal,
			Generation: generation,
			Keys:       [][]byte{separatorKey},
			Children:   []PageID{newChildID, rightChildID},
		}
		tree.registerNewNode(newRoot)
		newRootID = newRoot.PageID
	} else {
		newRootID = newChildID
	}

	tree.catalogMutex.Lock()
	tree.catalog[indexName] = IndexRoot{
		Name:       indexName,
		RootPageID: newRootID,
		Generation: generation,
	}
	tree.catalogMutex.Unlock()

	return newRootID, nil
}

func (tree *DiskBPlusTree) DeleteNamed(indexName string, key []byte, generation uint64) (PageID, error) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()

	if tree.dirtyNodes == nil {
		tree.dirtyNodes = make(map[PageID]bool)
	}
	if tree.nodeCache == nil {
		tree.nodeCache = make(map[PageID]*BPlusNode)
	}
	tree.orphanedList = nil

	newRootID, deleteError := tree.deleteNamedNoCommitLocked(indexName, key, generation)
	if deleteError != nil {
		tree.orphanedList = nil
		return 0, deleteError
	}

	if writeError := tree.writeNewStateLocked(generation); writeError != nil {
		tree.orphanedList = nil
		return 0, writeError
	}

	return newRootID, nil
}

func (tree *DiskBPlusTree) DeleteNamedNoCommit(indexName string, key []byte, generation uint64) (PageID, error) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()

	if tree.dirtyNodes == nil {
		tree.dirtyNodes = make(map[PageID]bool)
	}
	if tree.nodeCache == nil {
		tree.nodeCache = make(map[PageID]*BPlusNode)
	}

	return tree.deleteNamedNoCommitLocked(indexName, key, generation)
}

func (tree *DiskBPlusTree) deleteNamedNoCommitLocked(indexName string, key []byte, generation uint64) (PageID, error) {
	tree.catalogMutex.Lock()
	rootEntry, found := tree.catalog[indexName]
	tree.catalogMutex.Unlock()
	if !found {
		return 0, nil
	}

	newRootID, found, deleteError := tree.deleteRecursive(rootEntry.RootPageID, key, generation)
	if deleteError != nil {
		return 0, deleteError
	}
	if found {
		node, getNodeError := tree.getNode(newRootID)
		if getNodeError == nil && node.PageType == PageTypeInternal && len(node.Keys) == 0 && len(node.Children) == 1 {
			oldRootID := newRootID
			newRootID = node.Children[0]
			tree.markOrphaned(oldRootID)
		}

		tree.catalogMutex.Lock()
		tree.catalog[indexName] = IndexRoot{
			Name:       indexName,
			RootPageID: newRootID,
			Generation: generation,
		}
		tree.catalogMutex.Unlock()
	}
	return newRootID, nil
}

func (tree *DiskBPlusTree) RangeNamed(indexName string, start []byte, end []byte, generation uint64, visit func([]byte, RecordPointer) bool) error {
	tree.catalogMutex.RLock()
	rootEntry, found := tree.catalog[indexName]
	tree.catalogMutex.RUnlock()
	if !found {
		return nil
	}

	currentPageID := rootEntry.RootPageID
	for {
		node, getNodeError := tree.getNode(currentPageID)
		if getNodeError != nil {
			return getNodeError
		}

		if node.PageType == PageTypeLeaf {
			break
		} else {
			index := sort.Search(len(node.Keys), func(indexVal int) bool {
				return compareByteKeys(node.Keys[indexVal], start) > 0
			})
			currentPageID = node.Children[index]
		}
	}

	for currentPageID != 0 {
		node, getNodeError := tree.getNode(currentPageID)
		if getNodeError != nil {
			return getNodeError
		}

		for index := 0; index < len(node.Keys); index++ {
			key := node.Keys[index]
			if len(start) > 0 && compareByteKeys(key, start) < 0 {
				continue
			}
			if len(end) > 0 && compareByteKeys(key, end) > 0 {
				return nil
			}
			if !visit(key, node.Pointers[index]) {
				return nil
			}
		}

		currentPageID = node.RightSibling
	}

	return nil
}

func (tree *DiskBPlusTree) insertRecursive(currentPageID PageID, key []byte, pointer RecordPointer, generation uint64) (PageID, bool, []byte, PageID, error) {
	node, getNodeError := tree.getNode(currentPageID)
	if getNodeError != nil {
		return 0, false, nil, 0, getNodeError
	}

	var newNode *BPlusNode
	if node.Generation == generation {
		newNode = node
		tree.markDirty(newNode.PageID)
	} else {
		newNode = &BPlusNode{
			PageID:       tree.allocatePageID(),
			PageType:     node.PageType,
			Generation:   generation,
			RightSibling: node.RightSibling,
		}
		tree.registerNewNode(newNode)
		tree.markOrphaned(node.PageID)
	}

	if node.PageType == PageTypeLeaf {
		if newNode != node {
			newNode.Keys = append([][]byte(nil), node.Keys...)
			newNode.Pointers = append([]RecordPointer(nil), node.Pointers...)
		}

		index := sort.Search(len(newNode.Keys), func(indexVal int) bool {
			return compareByteKeys(newNode.Keys[indexVal], key) >= 0
		})

		if index < len(newNode.Keys) && bytes.Equal(newNode.Keys[index], key) {
			newNode.Pointers[index] = pointer
		} else {
			newNode.Keys = append(newNode.Keys, nil)
			copy(newNode.Keys[index+1:], newNode.Keys[index:])
			newNode.Keys[index] = key

			newNode.Pointers = append(newNode.Pointers, RecordPointer{})
			copy(newNode.Pointers[index+1:], newNode.Pointers[index:])
			newNode.Pointers[index] = pointer
		}

		_, serializeError := newNode.Serialize()
		if serializeError == nil {
			return newNode.PageID, false, nil, 0, nil
		}

		midpoint := len(newNode.Keys) / 2
		rightNode := &BPlusNode{
			PageID:       tree.allocatePageID(),
			PageType:     PageTypeLeaf,
			Generation:   generation,
			Keys:         append([][]byte(nil), newNode.Keys[midpoint:]...),
			Pointers:     append([]RecordPointer(nil), newNode.Pointers[midpoint:]...),
			RightSibling: newNode.RightSibling,
		}
		tree.registerNewNode(rightNode)

		newNode.Keys = newNode.Keys[:midpoint]
		newNode.Pointers = newNode.Pointers[:midpoint]
		newNode.RightSibling = rightNode.PageID
		tree.markDirty(newNode.PageID)

		separatorKey := rightNode.Keys[0]
		return newNode.PageID, true, separatorKey, rightNode.PageID, nil
	} else {
		index := sort.Search(len(node.Keys), func(indexVal int) bool {
			return compareByteKeys(node.Keys[indexVal], key) > 0
		})

		childID := node.Children[index]
		newChildID, didSplit, separatorKey, rightChildID, recursiveError := tree.insertRecursive(childID, key, pointer, generation)
		if recursiveError != nil {
			return 0, false, nil, 0, recursiveError
		}

		if newNode != node {
			newNode.Keys = append([][]byte(nil), node.Keys...)
			newNode.Children = append([]PageID(nil), node.Children...)
		}

		newNode.Children[index] = newChildID

		if didSplit {
			newNode.Keys = append(newNode.Keys, nil)
			copy(newNode.Keys[index+1:], newNode.Keys[index:])
			newNode.Keys[index] = separatorKey

			newNode.Children = append(newNode.Children, 0)
			copy(newNode.Children[index+2:], newNode.Children[index+1:])
			newNode.Children[index+1] = rightChildID
		}

		_, serializeError := newNode.Serialize()
		if serializeError == nil {
			return newNode.PageID, false, nil, 0, nil
		}

		midpoint := len(newNode.Keys) / 2
		parentKey := newNode.Keys[midpoint]

		rightNode := &BPlusNode{
			PageID:     tree.allocatePageID(),
			PageType:   PageTypeInternal,
			Generation: generation,
			Keys:       append([][]byte(nil), newNode.Keys[midpoint+1:]...),
			Children:   append([]PageID(nil), newNode.Children[midpoint+1:]...),
		}
		tree.registerNewNode(rightNode)

		newNode.Keys = newNode.Keys[:midpoint]
		newNode.Children = newNode.Children[:midpoint+1]
		tree.markDirty(newNode.PageID)

		return newNode.PageID, true, parentKey, rightNode.PageID, nil
	}
}

func (tree *DiskBPlusTree) deleteRecursive(currentPageID PageID, key []byte, generation uint64) (PageID, bool, error) {
	node, getNodeError := tree.getNode(currentPageID)
	if getNodeError != nil {
		return 0, false, getNodeError
	}

	var newNode *BPlusNode
	if node.Generation == generation {
		newNode = node
		tree.markDirty(newNode.PageID)
	} else {
		newNode = &BPlusNode{
			PageID:       tree.allocatePageID(),
			PageType:     node.PageType,
			Generation:   generation,
			RightSibling: node.RightSibling,
		}
		tree.registerNewNode(newNode)
		tree.markOrphaned(node.PageID)
	}

	if node.PageType == PageTypeLeaf {
		if newNode != node {
			newNode.Keys = append([][]byte(nil), node.Keys...)
			newNode.Pointers = append([]RecordPointer(nil), node.Pointers...)
		}

		index := sort.Search(len(newNode.Keys), func(indexVal int) bool {
			return compareByteKeys(newNode.Keys[indexVal], key) >= 0
		})

		if index < len(newNode.Keys) && bytes.Equal(newNode.Keys[index], key) {
			newNode.Keys = append(newNode.Keys[:index], newNode.Keys[index+1:]...)
			newNode.Pointers = append(newNode.Pointers[:index], newNode.Pointers[index+1:]...)

			return newNode.PageID, true, nil
		}

		return currentPageID, false, nil
	} else {
		index := sort.Search(len(node.Keys), func(indexVal int) bool {
			return compareByteKeys(node.Keys[indexVal], key) > 0
		})

		childID := node.Children[index]
		newChildID, found, recursiveError := tree.deleteRecursive(childID, key, generation)
		if recursiveError != nil {
			return 0, false, recursiveError
		}

		if !found {
			return currentPageID, false, nil
		}

		if newNode != node {
			newNode.Keys = append([][]byte(nil), node.Keys...)
			newNode.Children = append([]PageID(nil), node.Children...)
		}

		newNode.Children[index] = newChildID

		if handleUnderflowError := tree.handleUnderflow(newNode, index, generation); handleUnderflowError != nil {
			return 0, false, handleUnderflowError
		}

		return newNode.PageID, true, nil
	}
}

func (tree *DiskBPlusTree) handleUnderflow(parent *BPlusNode, childIndex int, generation uint64) error {
	childID := parent.Children[childIndex]
	child, getNodeError := tree.getNode(childID)
	if getNodeError != nil {
		return getNodeError
	}

	if len(child.Keys) >= 2 {
		return nil
	}

	if childIndex > 0 {
		leftID := parent.Children[childIndex-1]
		leftSibling, getNodeErrorLeft := tree.getNode(leftID)
		if getNodeErrorLeft == nil {
			if mergeOrRedistributeError := tree.mergeOrRedistribute(parent, childIndex-1, leftSibling, child, generation); mergeOrRedistributeError == nil {
				return nil
			}
		}
	}

	if childIndex < len(parent.Children)-1 {
		rightID := parent.Children[childIndex+1]
		rightSibling, getNodeErrorRight := tree.getNode(rightID)
		if getNodeErrorRight == nil {
			if mergeOrRedistributeError := tree.mergeOrRedistribute(parent, childIndex, child, rightSibling, generation); mergeOrRedistributeError == nil {
				return nil
			}
		}
	}

	return nil
}

func (tree *DiskBPlusTree) mergeOrRedistribute(parent *BPlusNode, leftIndex int, left *BPlusNode, right *BPlusNode, generation uint64) error {
	var newLeft, newRight *BPlusNode
	if left.Generation == generation {
		newLeft = left
		tree.markDirty(newLeft.PageID)
	} else {
		newLeft = &BPlusNode{
			PageID:       tree.allocatePageID(),
			PageType:     left.PageType,
			Generation:   generation,
			RightSibling: left.RightSibling,
		}
		newLeft.Keys = append([][]byte(nil), left.Keys...)
		if left.PageType == PageTypeLeaf {
			newLeft.Pointers = append([]RecordPointer(nil), left.Pointers...)
		} else {
			newLeft.Children = append([]PageID(nil), left.Children...)
		}
		tree.registerNewNode(newLeft)
		tree.markOrphaned(left.PageID)
	}

	if right.Generation == generation {
		newRight = right
		tree.markDirty(newRight.PageID)
	} else {
		newRight = &BPlusNode{
			PageID:       tree.allocatePageID(),
			PageType:     right.PageType,
			Generation:   generation,
			RightSibling: right.RightSibling,
		}
		newRight.Keys = append([][]byte(nil), right.Keys...)
		if right.PageType == PageTypeLeaf {
			newRight.Pointers = append([]RecordPointer(nil), right.Pointers...)
		} else {
			newRight.Children = append([]PageID(nil), right.Children...)
		}
		tree.registerNewNode(newRight)
		tree.markOrphaned(right.PageID)
	}

	parent.Children[leftIndex] = newLeft.PageID
	parent.Children[leftIndex+1] = newRight.PageID

	if left.PageType == PageTypeLeaf {
		if len(newLeft.Keys)+len(newRight.Keys) <= 20 {
			newLeft.Keys = append(newLeft.Keys, newRight.Keys...)
			newLeft.Pointers = append(newLeft.Pointers, newRight.Pointers...)
			newLeft.RightSibling = newRight.RightSibling

			parent.Keys = append(parent.Keys[:leftIndex], parent.Keys[leftIndex+1:]...)
			parent.Children = append(parent.Children[:leftIndex+1], parent.Children[leftIndex+2:]...)

			tree.markOrphaned(newRight.PageID)
			return nil
		}

		newLeft.Keys = append(newLeft.Keys, newRight.Keys[0])
		newLeft.Pointers = append(newLeft.Pointers, newRight.Pointers[0])

		newRight.Keys = append([][]byte(nil), newRight.Keys[1:]...)
		newRight.Pointers = append([]RecordPointer(nil), newRight.Pointers[1:]...)

		parent.Keys[leftIndex] = newRight.Keys[0]
		return nil
	} else {
		if len(newLeft.Keys)+len(newRight.Keys)+1 <= 20 {
			newLeft.Keys = append(newLeft.Keys, parent.Keys[leftIndex])
			newLeft.Keys = append(newLeft.Keys, newRight.Keys...)
			newLeft.Children = append(newLeft.Children, newRight.Children...)

			parent.Keys = append(parent.Keys[:leftIndex], parent.Keys[leftIndex+1:]...)
			parent.Children = append(parent.Children[:leftIndex+1], parent.Children[leftIndex+2:]...)

			tree.markOrphaned(newRight.PageID)
			return nil
		}

		newLeft.Keys = append(newLeft.Keys, parent.Keys[leftIndex])
		newLeft.Children = append(newLeft.Children, newRight.Children[0])

		parent.Keys[leftIndex] = newRight.Keys[0]

		newRight.Keys = append([][]byte(nil), newRight.Keys[1:]...)
		newRight.Children = append([]PageID(nil), newRight.Children[1:]...)
		return nil
	}
}

func (tree *DiskBPlusTree) Sync() error {
	return tree.file.Sync()
}

func (tree *DiskBPlusTree) Close() error {
	return tree.file.Close()
}

type NamedIndex struct {
	Tree      *DiskBPlusTree
	IndexName string
}

var _ PersistentIndex = (*NamedIndex)(nil)

func (namedIndex *NamedIndex) Find(key []byte, generation uint64) (RecordPointer, bool, error) {
	return namedIndex.Tree.FindNamed(namedIndex.IndexName, key, generation)
}

func (namedIndex *NamedIndex) Insert(key []byte, pointer RecordPointer, generation uint64) (PageID, error) {
	return namedIndex.Tree.InsertNamed(namedIndex.IndexName, key, pointer, generation)
}

func (namedIndex *NamedIndex) InsertNoCommit(key []byte, pointer RecordPointer, generation uint64) (PageID, error) {
	return namedIndex.Tree.InsertNamedNoCommit(namedIndex.IndexName, key, pointer, generation)
}

func (namedIndex *NamedIndex) Delete(key []byte, generation uint64) (PageID, error) {
	return namedIndex.Tree.DeleteNamed(namedIndex.IndexName, key, generation)
}

func (namedIndex *NamedIndex) DeleteNoCommit(key []byte, generation uint64) (PageID, error) {
	return namedIndex.Tree.DeleteNamedNoCommit(namedIndex.IndexName, key, generation)
}

func (namedIndex *NamedIndex) Commit(generation uint64) error {
	namedIndex.Tree.mutex.Lock()
	defer namedIndex.Tree.mutex.Unlock()
	return namedIndex.Tree.writeNewStateLocked(generation)
}

func (namedIndex *NamedIndex) Range(start []byte, end []byte, generation uint64, visit func([]byte, RecordPointer) bool) error {
	return namedIndex.Tree.RangeNamed(namedIndex.IndexName, start, end, generation, visit)
}

func (namedIndex *NamedIndex) Sync() error {
	return namedIndex.Tree.Sync()
}

func (namedIndex *NamedIndex) Close() error {
	return nil
}
