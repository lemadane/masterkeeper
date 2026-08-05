package masterkeeper_test

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	keeper "github.com/lemadane/masterkeeper"
)

type BenchmarkUser struct {
	ID    string `keeper:"id"`
	Name  string
	Email string
}

func TestBenchmarkMemoryOverhead(test *testing.T) {
	temporaryDirectory, createError := os.MkdirTemp("/tmp", "keeper-mem-bench-*")
	if createError != nil {
		test.Fatalf("failed to create temporary directory: %v", createError)
	}
	defer os.RemoveAll(temporaryDirectory)

	options := keeper.DefaultOptions()
	options.RegisterTypes(BenchmarkUser{})

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	database, openError := keeper.Open(temporaryDirectory, options)
	if openError != nil {
		test.Fatalf("failed to open database: %v", openError)
	}
	defer database.Close()

	table, tableError := keeper.GetTable[string, BenchmarkUser](database, "users")
	if tableError != nil {
		test.Fatalf("failed to get table: %v", tableError)
	}

	const recordCount = 10000

	transactionError := database.Transaction(func(transaction *keeper.Transaction) error {
		for index := 0; index < recordCount; index++ {
			user := BenchmarkUser{
				ID:    fmt.Sprintf("user-%07d", index),
				Name:  "NameStringOfReasonableLength",
				Email: fmt.Sprintf("email-%07d@example.com", index),
			}
			if insertError := table.Insert(transaction, user); insertError != nil {
				return insertError
			}
		}
		return nil
	})
	if transactionError != nil {
		test.Fatalf("transaction failed: %v", transactionError)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	heapUsed := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
	if heapUsed < 0 {
		heapUsed = 0
	}

	bytesPerRecord := float64(heapUsed) / float64(recordCount)

	test.Logf("=== Memory Benchmark Results ===")
	test.Logf("Inserted records: %d", recordCount)
	test.Logf("HeapAlloc increase: %d bytes (%.2f MB)", heapUsed, float64(heapUsed)/(1024*1024))
	test.Logf("Estimated memory overhead per record: %.2f bytes/record", bytesPerRecord)
}
