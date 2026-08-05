package masterkeeper_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	keeper "github.com/lemadane/masterkeeper"
)

func TestPageSerialization(test *testing.T) {
	temporaryFile, createError := os.CreateTemp("", "keeper-page-test-*")
	if createError != nil {
		test.Fatalf("failed to create temp file: %v", createError)
	}
	defer os.Remove(temporaryFile.Name())
	defer temporaryFile.Close()

	node := &keeper.BPlusNode{
		PageID:     1,
		PageType:   keeper.PageTypeLeaf,
		Generation: 10,
		Keys:       [][]byte{[]byte("key1"), []byte("key2")},
		Pointers: []keeper.RecordPointer{
			{Offset: 100, Size: 50},
			{Offset: 200, Size: 60},
		},
	}

	data, serializeError := node.Serialize()
	if serializeError != nil {
		test.Fatalf("serialize failed: %v", serializeError)
	}

	if len(data) != keeper.IndexPageSize {
		test.Fatalf("expected page size %d, got %d", keeper.IndexPageSize, len(data))
	}

	node2, deserializeError := keeper.DeserializeNode(data)
	if deserializeError != nil {
		test.Fatalf("deserialize failed: %v", deserializeError)
	}

	if node2.PageID != node.PageID || node2.PageType != node.PageType || node2.Generation != node.Generation {
		test.Fatalf("node metadata mismatch")
	}

	if len(node2.Keys) != 2 {
		test.Fatalf("expected 2 keys, got %d", len(node2.Keys))
	}

	if string(node2.Keys[0]) != "key1" || string(node2.Keys[1]) != "key2" {
		test.Fatalf("keys mismatch")
	}

	if node2.Pointers[0].Offset != 100 || node2.Pointers[1].Size != 60 {
		test.Fatalf("pointers mismatch")
	}
}

func TestDiskBPlusTreeBasic(test *testing.T) {
	temporaryFile, createError := os.CreateTemp("", "keeper-btree-test-*")
	if createError != nil {
		test.Fatalf("failed to create temp file: %v", createError)
	}
	defer os.Remove(temporaryFile.Name())
	defer temporaryFile.Close()

	tree, treeError := keeper.NewDiskBPlusTree(temporaryFile)
	if treeError != nil {
		test.Fatalf("failed to create bplus tree: %v", treeError)
	}

	namedIndex := &keeper.NamedIndex{
		Tree:      tree,
		IndexName: "test",
	}

	_, found, findError := namedIndex.Find([]byte("test"), 0)
	if findError != nil {
		test.Fatalf("find error: %v", findError)
	}
	if found {
		test.Fatalf("expected not found")
	}

	pointerValue := keeper.RecordPointer{Offset: 10, Size: 20}
	_, insertError := namedIndex.Insert([]byte("key1"), pointerValue, 1)
	if insertError != nil {
		test.Fatalf("insert error: %v", insertError)
	}

	resultPointer, found, findError2 := namedIndex.Find([]byte("key1"), 1)
	if findError2 != nil {
		test.Fatalf("find error: %v", findError2)
	}
	if !found || resultPointer.Offset != 10 || resultPointer.Size != 20 {
		test.Fatalf("expected found with correct pointer, got found=%t, pointer=%+v", found, resultPointer)
	}
}

func TestDiskBPlusTreeSplitsAndStress(test *testing.T) {
	temporaryFile, createError := os.CreateTemp("", "keeper-btree-split-test-*")
	if createError != nil {
		test.Fatalf("failed to create temp file: %v", createError)
	}
	defer os.Remove(temporaryFile.Name())
	defer temporaryFile.Close()

	tree, treeError := keeper.NewDiskBPlusTree(temporaryFile)
	if treeError != nil {
		test.Fatalf("failed to create bplus tree: %v", treeError)
	}

	namedIndex := &keeper.NamedIndex{
		Tree:      tree,
		IndexName: "test",
	}

	numKeys := 2000
	inserted := make(map[string]keeper.RecordPointer)

	randomGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	for index := 0; index < numKeys; index++ {
		keyString := fmt.Sprintf("user-%05d", index)
		pointerValue := keeper.RecordPointer{
			Offset: int64(randomGen.Int63n(1000000)),
			Size:   int32(randomGen.Intn(5000) + 1),
		}
		_, insertError := namedIndex.Insert([]byte(keyString), pointerValue, 1)
		if insertError != nil {
			test.Fatalf("failed to insert %s at index %d: %v", keyString, index, insertError)
		}
		inserted[keyString] = pointerValue
	}

	for keyString, expectedPointer := range inserted {
		pointerValue, found, findError := namedIndex.Find([]byte(keyString), 1)
		if findError != nil {
			test.Fatalf("find failed for %s: %v", keyString, findError)
		}
		if !found {
			test.Fatalf("key %s was not found", keyString)
		}
		if pointerValue.Offset != expectedPointer.Offset || pointerValue.Size != expectedPointer.Size {
			test.Fatalf("pointer mismatch for key %s: expected %+v, got %+v", keyString, expectedPointer, pointerValue)
		}
	}

	var scannedKeys []string
	rangeError := namedIndex.Range([]byte("user-00100"), []byte("user-00105"), 1, func(key []byte, pointerValue keeper.RecordPointer) bool {
		scannedKeys = append(scannedKeys, string(key))
		expectedPointer := inserted[string(key)]
		if pointerValue.Offset != expectedPointer.Offset || pointerValue.Size != expectedPointer.Size {
			test.Errorf("Range pointer mismatch for key %s", string(key))
		}
		return true
	})
	if rangeError != nil {
		test.Fatalf("Range query failed: %v", rangeError)
	}

	expectedScanned := []string{
		"user-00100",
		"user-00101",
		"user-00102",
		"user-00103",
		"user-00104",
		"user-00105",
	}

	if len(scannedKeys) != len(expectedScanned) {
		test.Fatalf("expected scanned length %d, got %d", len(expectedScanned), len(scannedKeys))
	}
	for i, key := range scannedKeys {
		if key != expectedScanned[i] {
			test.Errorf("expected scanned key %s, got %s", expectedScanned[i], key)
		}
	}
}

func TestAlternatingSuperblocksFallback(test *testing.T) {
	temporaryFile, createError := os.CreateTemp("", "keeper-btree-fallback-test-*")
	if createError != nil {
		test.Fatalf("failed to create temp file: %v", createError)
	}
	defer os.Remove(temporaryFile.Name())
	defer temporaryFile.Close()

	// 1. Initialize and perform multiple writes to alternate superblocks
	tree, treeError := keeper.NewDiskBPlusTree(temporaryFile)
	if treeError != nil {
		test.Fatalf("failed to create bplus tree: %v", treeError)
	}
	namedIndex := &keeper.NamedIndex{
		Tree:      tree,
		IndexName: "test",
	}

	pointer1 := keeper.RecordPointer{Offset: 50, Size: 100}
	if _, insertError := namedIndex.Insert([]byte("key1"), pointer1, 2); insertError != nil {
		test.Fatalf("failed to insert key1: %v", insertError)
	}

	pointer2 := keeper.RecordPointer{Offset: 150, Size: 200}
	if _, insertError := namedIndex.Insert([]byte("key2"), pointer2, 3); insertError != nil {
		test.Fatalf("failed to insert key2: %v", insertError)
	}

	// Close the tree
	tree.Close()

	// 2. Corrupt Superblock B (Page 1) by writing random garbage to it
	corruptFile, openError := os.OpenFile(temporaryFile.Name(), os.O_RDWR, 0644)
	if openError != nil {
		test.Fatalf("failed to open file for corruption: %v", openError)
	}
	garbage := make([]byte, keeper.IndexPageSize)
	for index := range garbage {
		garbage[index] = 0xAA
	}
	// Write garbage to Page 1
	if _, writeError := corruptFile.WriteAt(garbage, keeper.IndexPageSize); writeError != nil {
		corruptFile.Close()
		test.Fatalf("failed to write garbage: %v", writeError)
	}
	corruptFile.Close()

	// 3. Reopen the tree and verify fallback to Superblock A (Page 0)
	reopenedFile, reopenError := os.OpenFile(temporaryFile.Name(), os.O_RDWR, 0644)
	if reopenError != nil {
		test.Fatalf("failed to reopen file: %v", reopenError)
	}
	tree2, treeError2 := keeper.NewDiskBPlusTree(reopenedFile)
	if treeError2 != nil {
		reopenedFile.Close()
		test.Fatalf("failed to reopen tree: %v", treeError2)
	}
	defer tree2.Close()

	namedIndex2 := &keeper.NamedIndex{
		Tree:      tree2,
		IndexName: "test",
	}

	// Verify key1 can be retrieved successfully (since it was written to A)
	resultPointer, found1, findError1 := namedIndex2.Find([]byte("key1"), 2)
	if findError1 != nil {
		test.Fatalf("failed to search key1: %v", findError1)
	}
	if !found1 || resultPointer.Offset != 50 || resultPointer.Size != 100 {
		test.Fatalf("failed to retrieve key1 or mismatch pointer: found=%t, pointer=%+v", found1, resultPointer)
	}
}

func TestFreeListRecycling(test *testing.T) {
	temporaryFile, createError := os.CreateTemp("", "keeper-btree-recycle-test-*")
	if createError != nil {
		test.Fatalf("failed to create temp file: %v", createError)
	}
	defer os.Remove(temporaryFile.Name())
	defer temporaryFile.Close()

	tree, treeError := keeper.NewDiskBPlusTree(temporaryFile)
	if treeError != nil {
		test.Fatalf("failed to create tree: %v", treeError)
	}

	namedIndex := &keeper.NamedIndex{
		Tree:      tree,
		IndexName: "test",
	}

	pointerValue := keeper.RecordPointer{Offset: 1, Size: 10}
	for index := 0; index < 200; index++ {
		keyString := fmt.Sprintf("key-%d", index)
		if _, insertError := namedIndex.Insert([]byte(keyString), pointerValue, 2); insertError != nil {
			test.Fatalf("failed to insert: %v", insertError)
		}
	}

	fileInfo, statError := temporaryFile.Stat()
	if statError != nil {
		test.Fatalf("failed to stat: %v", statError)
	}
	sizeBeforeRecycle := fileInfo.Size()

	// Delete all keys to add pages to free list
	for index := 0; index < 200; index++ {
		keyString := fmt.Sprintf("key-%d", index)
		if _, deleteError := namedIndex.Delete([]byte(keyString), 3); deleteError != nil {
			test.Fatalf("failed to delete: %v", deleteError)
		}
	}

	// Insert keys again. Since they should recycle pages from free list,
	// the file size should not exceed sizeBeforeRecycle by more than a small amount.
	for index := 0; index < 200; index++ {
		keyString := fmt.Sprintf("key-%d", index)
		if _, insertError := namedIndex.Insert([]byte(keyString), pointerValue, 4); insertError != nil {
			test.Fatalf("failed to insert again: %v", insertError)
		}
	}

	fileInfoAfter, statErrorAfter := temporaryFile.Stat()
	if statErrorAfter != nil {
		test.Fatalf("failed to stat after: %v", statErrorAfter)
	}
	sizeAfterRecycle := fileInfoAfter.Size()

	if sizeAfterRecycle > sizeBeforeRecycle+32768 {
		test.Fatalf("expected page recycling to keep file size bounded, before=%d, after=%d", sizeBeforeRecycle, sizeAfterRecycle)
	}
}

func TestNodeMergingAndRedistribution(test *testing.T) {
	temporaryFile, createError := os.CreateTemp("", "keeper-btree-merge-test-*")
	if createError != nil {
		test.Fatalf("failed to create temp file: %v", createError)
	}
	defer os.Remove(temporaryFile.Name())
	defer temporaryFile.Close()

	tree, treeError := keeper.NewDiskBPlusTree(temporaryFile)
	if treeError != nil {
		test.Fatalf("failed to create tree: %v", treeError)
	}

	namedIndex := &keeper.NamedIndex{
		Tree:      tree,
		IndexName: "test",
	}

	pointerValue := keeper.RecordPointer{Offset: 1, Size: 10}

	for index := 0; index < 50; index++ {
		keyString := fmt.Sprintf("key-%03d", index)
		if _, insertError := namedIndex.Insert([]byte(keyString), pointerValue, 2); insertError != nil {
			test.Fatalf("failed to insert: %v", insertError)
		}
	}

	for index := 0; index < 50; index++ {
		keyString := fmt.Sprintf("key-%03d", index)
		if _, deleteError := namedIndex.Delete([]byte(keyString), 3); deleteError != nil {
			test.Fatalf("failed to delete: %v", deleteError)
		}
	}

	for index := 0; index < 50; index++ {
		keyString := fmt.Sprintf("key-%03d", index)
		_, found, findError := namedIndex.Find([]byte(keyString), 3)
		if findError != nil {
			test.Fatalf("find failed: %v", findError)
		}
		if found {
			test.Fatalf("expected key %s to be deleted", keyString)
		}
	}
}

func TestSecondaryIndexRangeScans(test *testing.T) {
	temporaryFile, createError := os.CreateTemp("", "keeper-btree-secondary-test-*")
	if createError != nil {
		test.Fatalf("failed to create temp file: %v", createError)
	}
	defer os.Remove(temporaryFile.Name())
	defer temporaryFile.Close()

	tree, treeError := keeper.NewDiskBPlusTree(temporaryFile)
	if treeError != nil {
		test.Fatalf("failed to create tree: %v", treeError)
	}

	namedIndex := &keeper.NamedIndex{
		Tree:      tree,
		IndexName: "Age",
	}

	pointer1 := keeper.RecordPointer{Offset: 100, Size: 10}
	pointer2 := keeper.RecordPointer{Offset: 200, Size: 20}
	pointer3 := keeper.RecordPointer{Offset: 300, Size: 30}

	compositeKey1, serializeError1 := keeper.SerializeCompositeKey(25, "c1")
	if serializeError1 != nil {
		test.Fatalf("failed to serialize composite key 1: %v", serializeError1)
	}
	compositeKey2, serializeError2 := keeper.SerializeCompositeKey(30, "c2")
	if serializeError2 != nil {
		test.Fatalf("failed to serialize composite key 2: %v", serializeError2)
	}
	compositeKey3, serializeError3 := keeper.SerializeCompositeKey(25, "c3")
	if serializeError3 != nil {
		test.Fatalf("failed to serialize composite key 3: %v", serializeError3)
	}

	if _, insertError := namedIndex.Insert(compositeKey1, pointer1, 2); insertError != nil {
		test.Fatalf("failed to insert 1: %v", insertError)
	}
	if _, insertError := namedIndex.Insert(compositeKey2, pointer2, 2); insertError != nil {
		test.Fatalf("failed to insert 2: %v", insertError)
	}
	if _, insertError := namedIndex.Insert(compositeKey3, pointer3, 2); insertError != nil {
		test.Fatalf("failed to insert 3: %v", insertError)
	}

	startKey, serializeErrorStart := keeper.SerializeCompositeKey(25, "")
	if serializeErrorStart != nil {
		test.Fatalf("failed to serialize start key: %v", serializeErrorStart)
	}

	var visitedAges []int
	var visitedIDs []string
	rangeError := namedIndex.Range(startKey, nil, 2, func(compositeKeyBytes []byte, pointerValue keeper.RecordPointer) bool {
		ageVal, idVal, deserializeError := keeper.DeserializeCompositeKey(compositeKeyBytes)
		if deserializeError != nil {
			test.Fatalf("failed to deserialize key: %v", deserializeError)
		}
		visitedAges = append(visitedAges, int(ageVal.(int64)))
		visitedIDs = append(visitedIDs, idVal.(string))
		return true
	})

	if rangeError != nil {
		test.Fatalf("range error: %v", rangeError)
	}

	if len(visitedAges) != 3 {
		test.Fatalf("expected 3 records, got %d", len(visitedAges))
	}
	if visitedAges[0] != 25 || visitedIDs[0] != "c1" {
		test.Fatalf("expected c1 first, got %s", visitedIDs[0])
	}
	if visitedAges[1] != 25 || visitedIDs[1] != "c3" {
		test.Fatalf("expected c3 second, got %s", visitedIDs[1])
	}
	if visitedAges[2] != 30 || visitedIDs[2] != "c2" {
		test.Fatalf("expected c2 third, got %s", visitedIDs[2])
	}
}
