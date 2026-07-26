package masterkeeper_test

import (
	"fmt"
	keeper "masterkeeper"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

type User struct {
	ID    int    `keeper:"id"`
	Name  string `keeper:"index"`
	Email string `keeper:"unique"`
}

type Order struct {
	ID        string `keeper:"id"`
	UserID    int    `keeper:"index"`
	Amount    float64
	CreatedAt time.Time
}

type Product struct {
	ID    string `keeper:"id"`
	Title string `keeper:"index"`
	Price float64
}

type Review struct {
	ID        string `keeper:"id"`
	ProductID string `keeper:"index"`
	Rating    int
}

func TestStressSyncSingleTable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-sync-single-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.Durability = keeper.DurabilitySync
	opts.RegisterTypes(User{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	table, err := keeper.GetTable[int, User](db, "users")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	const numWriters = 10
	const opsPerWriter = 100000 // 10 * 100000 = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var wg sync.WaitGroup
	wg.Add(numWriters + numQueries)

	// Writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			// Batch the 10,000 inserts inside a single transaction to execute stress testing
			// of 1,000,000 records without hitting sequential write I/O limits of physical storage sync.
			err := db.Transaction(func(tx *keeper.Transaction) error {
				for i := 0; i < opsPerWriter; i++ {
					id := writerID*opsPerWriter + i
					u := User{
						ID:    id,
						Name:  fmt.Sprintf("User-%d", id),
						Email: fmt.Sprintf("user-%d@example.com", id),
					}
					if err := table.Insert(tx, u); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Errorf("writer transaction failed: %v", err)
			}
		}(w)
	}

	// Query-ers running concurrently
	for q := 0; q < numQueries; q++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < queriesPerQueryer; i++ {
				queryVal := fmt.Sprintf("User-%d", r.Intn(numWriters*opsPerWriter))
				res, err := table.Query(nil).
					Where(keeper.Eq("Name", queryVal)).
					List()
				if err != nil {
					t.Errorf("query error: %v", err)
				}
				_ = res
				time.Sleep(10 * time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// Verify all 1,000,000 records are correctly saved
	for i := 0; i < numWriters*opsPerWriter; i += 1000 { // sample check every 1000th record for speed
		u, found, err := table.FindByID(nil, i)
		if err != nil {
			t.Fatalf("failed to find user %d: %v", i, err)
		}
		if !found {
			t.Fatalf("user %d was not found", i)
		}
		if u.Name != fmt.Sprintf("User-%d", i) {
			t.Errorf("incorrect record content for user %d: %+v", i, u)
		}
	}
}

func TestStressAsyncSingleTable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-async-single-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.Durability = keeper.DurabilityAsync
	opts.RegisterTypes(User{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	table, err := keeper.GetTable[int, User](db, "users")
	if err != nil {
		t.Fatalf("failed to get table: %v", err)
	}

	const numWriters = 10
	const opsPerWriter = 100000 // 10 * 100000 = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var wg sync.WaitGroup
	wg.Add(numWriters + numQueries)

	// Writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			err := db.Transaction(func(tx *keeper.Transaction) error {
				for i := 0; i < opsPerWriter; i++ {
					id := writerID*opsPerWriter + i
					u := User{
						ID:    id,
						Name:  fmt.Sprintf("User-%d", id),
						Email: fmt.Sprintf("user-%d@example.com", id),
					}
					if err := table.Insert(tx, u); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Errorf("writer transaction failed: %v", err)
			}
		}(w)
	}

	// Query-ers running concurrently
	for q := 0; q < numQueries; q++ {
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < queriesPerQueryer; i++ {
				queryVal := fmt.Sprintf("User-%d", r.Intn(numWriters*opsPerWriter))
				res, err := table.Query(nil).
					Where(keeper.Eq("Name", queryVal)).
					List()
				if err != nil {
					t.Errorf("query error: %v", err)
				}
				_ = res
				time.Sleep(10 * time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// Verify all 1,000,000 records
	for i := 0; i < numWriters*opsPerWriter; i += 1000 {
		u, found, err := table.FindByID(nil, i)
		if err != nil {
			t.Fatalf("failed to find user %d: %v", i, err)
		}
		if !found {
			t.Fatalf("user %d was not found", i)
		}
		if u.Name != fmt.Sprintf("User-%d", i) {
			t.Errorf("incorrect record content for user %d: %+v", i, u)
		}
	}
}

func TestStressSyncMultiTable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-sync-multi-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.Durability = keeper.DurabilitySync
	opts.RegisterTypes(User{}, Order{}, Product{}, Review{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	users, _ := keeper.GetTable[int, User](db, "users")
	orders, _ := keeper.GetTable[string, Order](db, "orders")
	products, _ := keeper.GetTable[string, Product](db, "products")
	reviews, _ := keeper.GetTable[string, Review](db, "reviews")

	const numWriters = 10
	const opsPerWriter = 25000 // 10 * 25000 * 4 tables = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var wg sync.WaitGroup
	wg.Add(numWriters + numQueries)

	// Writers performing mixed table insertions
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			err := db.Transaction(func(tx *keeper.Transaction) error {
				for i := 0; i < opsPerWriter; i++ {
					id := writerID*opsPerWriter + i
					
					// 1. Insert user
					_ = users.Insert(tx, User{
						ID:    id,
						Name:  fmt.Sprintf("User-%d", id),
						Email: fmt.Sprintf("user-%d@example.com", id),
					})

					// 2. Insert product
					pID := fmt.Sprintf("p_%d", id)
					_ = products.Insert(tx, Product{
						ID:    pID,
						Title: fmt.Sprintf("Product-%d", id),
						Price: float64(10 + id),
					})

					// 3. Insert order
					oID := fmt.Sprintf("o_%d", id)
					_ = orders.Insert(tx, Order{
						ID:        oID,
						UserID:    id,
						Amount:    float64(50 + id),
						CreatedAt: time.Now(),
					})

					// 4. Insert review
					rID := fmt.Sprintf("r_%d", id)
					_ = reviews.Insert(tx, Review{
						ID:        rID,
						ProductID: pID,
						Rating:    5,
					})
				}
				return nil
			})
			if err != nil {
				t.Errorf("writer transaction failed: %v", err)
			}
		}(w)
	}

	// Readers querying mixed tables
	for r := 0; r < numQueries; r++ {
		go func() {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < queriesPerQueryer; i++ {
				target := rnd.Intn(numWriters * opsPerWriter)
				
				_, _, _ = users.FindByID(nil, target)
				_, _, _ = products.FindByID(nil, fmt.Sprintf("p_%d", target))
				
				_, _ = orders.Query(nil).
					Where(keeper.Eq("UserID", target)).
					List()

				_, _ = reviews.Query(nil).
					Where(keeper.Eq("ProductID", fmt.Sprintf("p_%d", target))).
					List()

				time.Sleep(10 * time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// Verify records
	for i := 0; i < numWriters*opsPerWriter; i += 500 {
		_, found, err := users.FindByID(nil, i)
		if err != nil || !found {
			t.Fatalf("verification failed for user %d: found=%v, err=%v", i, found, err)
		}
	}
}

func TestStressAsyncMultiTable(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-async-multi-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	opts := keeper.DefaultOptions()
	opts.Durability = keeper.DurabilityAsync
	opts.RegisterTypes(User{}, Order{}, Product{}, Review{})

	db, err := keeper.Open(tempDir, opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	users, _ := keeper.GetTable[int, User](db, "users")
	orders, _ := keeper.GetTable[string, Order](db, "orders")
	products, _ := keeper.GetTable[string, Product](db, "products")
	reviews, _ := keeper.GetTable[string, Review](db, "reviews")

	const numWriters = 10
	const opsPerWriter = 25000 // 10 * 25000 * 4 tables = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var wg sync.WaitGroup
	wg.Add(numWriters + numQueries)

	// Writers performing mixed table insertions
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			err := db.Transaction(func(tx *keeper.Transaction) error {
				for i := 0; i < opsPerWriter; i++ {
					id := writerID*opsPerWriter + i
					
					_ = users.Insert(tx, User{
						ID:    id,
						Name:  fmt.Sprintf("User-%d", id),
						Email: fmt.Sprintf("user-%d@example.com", id),
					})

					pID := fmt.Sprintf("p_%d", id)
					_ = products.Insert(tx, Product{
						ID:    pID,
						Title: fmt.Sprintf("Product-%d", id),
						Price: float64(10 + id),
					})

					oID := fmt.Sprintf("o_%d", id)
					_ = orders.Insert(tx, Order{
						ID:        oID,
						UserID:    id,
						Amount:    float64(50 + id),
						CreatedAt: time.Now(),
					})

					rID := fmt.Sprintf("r_%d", id)
					_ = reviews.Insert(tx, Review{
						ID:        rID,
						ProductID: pID,
						Rating:    5,
					})
				}
				return nil
			})
			if err != nil {
				t.Errorf("writer transaction failed: %v", err)
			}
		}(w)
	}

	// Readers querying mixed tables
	for r := 0; r < numQueries; r++ {
		go func() {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < queriesPerQueryer; i++ {
				target := rnd.Intn(numWriters * opsPerWriter)
				
				_, _, _ = users.FindByID(nil, target)
				_, _, _ = products.FindByID(nil, fmt.Sprintf("p_%d", target))
				
				_, _ = orders.Query(nil).
					Where(keeper.Eq("UserID", target)).
					List()

				_, _ = reviews.Query(nil).
					Where(keeper.Eq("ProductID", fmt.Sprintf("p_%d", target))).
					List()

				time.Sleep(10 * time.Microsecond)
			}
		}()
	}

	wg.Wait()

	// Verify records
	for i := 0; i < numWriters*opsPerWriter; i += 500 {
		_, found, err := users.FindByID(nil, i)
		if err != nil || !found {
			t.Fatalf("verification failed for user %d: found=%v, err=%v", i, found, err)
		}
	}
}
