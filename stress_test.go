package masterkeeper_test

import (
	"fmt"
	keeper "github.com/lemadane/masterkeeper"
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

func TestStressSyncSingleTable(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-sync-single-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.Durability = keeper.DurabilitySync
	options.RegisterTypes(User{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	userTable, err := keeper.GetTable[int, User](database, "users")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	const numWriters = 10
	const opsPerWriter = 100000 // 10 * 100000 = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var waitGroup sync.WaitGroup
	waitGroup.Add(numWriters + numQueries)

	// Writers
	for writerIndex := 0; writerIndex < numWriters; writerIndex++ {
		go func(writerID int) {
			defer waitGroup.Done()
			// Batch the 10,000 inserts inside a single transaction to execute stress testing
			// of 1,000,000 records without hitting sequential write I/O limits of physical storage sync.
			err := database.Transaction(func(transaction *keeper.Transaction) error {
				for index := 0; index < opsPerWriter; index++ {
					userID := writerID*opsPerWriter + index
					user := User{
						ID:    userID,
						Name:  fmt.Sprintf("User-%d", userID),
						Email: fmt.Sprintf("user-%d@example.com", userID),
					}
					if err := userTable.Insert(transaction, user); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				testingT.Errorf("writer transaction failed: %v", err)
			}
		}(writerIndex)
	}

	// Query-ers running concurrently
	for queryIndex := 0; queryIndex < numQueries; queryIndex++ {
		go func() {
			defer waitGroup.Done()
			randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for index := 0; index < queriesPerQueryer; index++ {
				queryVal := fmt.Sprintf("User-%d", randomGenerator.Intn(numWriters*opsPerWriter))
				results, err := userTable.Query(nil).
					Where(keeper.Eq("Name", queryVal)).
					List()
				if err != nil {
					testingT.Errorf("query error: %v", err)
				}
				_ = results
				time.Sleep(10 * time.Microsecond)
			}
		}()
	}

	waitGroup.Wait()

	// Verify all 1,000,000 records are correctly saved
	for index := 0; index < numWriters*opsPerWriter; index += 1000 { // sample check every 1000th record for speed
		user, found, err := userTable.FindByID(nil, index)
		if err != nil {
			testingT.Fatalf("failed to find user %d: %v", index, err)
		}
		if !found {
			testingT.Fatalf("user %d was not found", index)
		}
		if user.Name != fmt.Sprintf("User-%d", index) {
			testingT.Errorf("incorrect record content for user %d: %+v", index, user)
		}
	}
}

func TestStressAsyncSingleTable(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-async-single-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.Durability = keeper.DurabilityAsync
	options.RegisterTypes(User{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	userTable, err := keeper.GetTable[int, User](database, "users")
	if err != nil {
		testingT.Fatalf("failed to get table: %v", err)
	}

	const numWriters = 10
	const opsPerWriter = 100000 // 10 * 100000 = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var waitGroup sync.WaitGroup
	waitGroup.Add(numWriters + numQueries)

	// Writers
	for writerIndex := 0; writerIndex < numWriters; writerIndex++ {
		go func(writerID int) {
			defer waitGroup.Done()
			err := database.Transaction(func(transaction *keeper.Transaction) error {
				for index := 0; index < opsPerWriter; index++ {
					userID := writerID*opsPerWriter + index
					user := User{
						ID:    userID,
						Name:  fmt.Sprintf("User-%d", userID),
						Email: fmt.Sprintf("user-%d@example.com", userID),
					}
					if err := userTable.Insert(transaction, user); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				testingT.Errorf("writer transaction failed: %v", err)
			}
		}(writerIndex)
	}

	// Query-ers running concurrently
	for queryIndex := 0; queryIndex < numQueries; queryIndex++ {
		go func() {
			defer waitGroup.Done()
			randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for index := 0; index < queriesPerQueryer; index++ {
				queryVal := fmt.Sprintf("User-%d", randomGenerator.Intn(numWriters*opsPerWriter))
				results, err := userTable.Query(nil).
					Where(keeper.Eq("Name", queryVal)).
					List()
				if err != nil {
					testingT.Errorf("query error: %v", err)
				}
				_ = results
				time.Sleep(10 * time.Microsecond)
			}
		}()
	}

	waitGroup.Wait()

	// Verify all 1,000,000 records
	for index := 0; index < numWriters*opsPerWriter; index += 1000 {
		user, found, err := userTable.FindByID(nil, index)
		if err != nil {
			testingT.Fatalf("failed to find user %d: %v", index, err)
		}
		if !found {
			testingT.Fatalf("user %d was not found", index)
		}
		if user.Name != fmt.Sprintf("User-%d", index) {
			testingT.Errorf("incorrect record content for user %d: %+v", index, user)
		}
	}
}

func TestStressSyncMultiTable(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-sync-multi-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.Durability = keeper.DurabilitySync
	options.RegisterTypes(User{}, Order{}, Product{}, Review{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	users, _ := keeper.GetTable[int, User](database, "users")
	orders, _ := keeper.GetTable[string, Order](database, "orders")
	products, _ := keeper.GetTable[string, Product](database, "products")
	reviews, _ := keeper.GetTable[string, Review](database, "reviews")

	const numWriters = 10
	const opsPerWriter = 25000 // 10 * 25000 * 4 tables = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var waitGroup sync.WaitGroup
	waitGroup.Add(numWriters + numQueries)

	// Writers performing mixed table insertions
	for writerIndex := 0; writerIndex < numWriters; writerIndex++ {
		go func(writerID int) {
			defer waitGroup.Done()
			err := database.Transaction(func(transaction *keeper.Transaction) error {
				for index := 0; index < opsPerWriter; index++ {
					userID := writerID*opsPerWriter + index
					
					// 1. Insert user
					_ = users.Insert(transaction, User{
						ID:    userID,
						Name:  fmt.Sprintf("User-%d", userID),
						Email: fmt.Sprintf("user-%d@example.com", userID),
					})

					// 2. Insert product
					productID := fmt.Sprintf("p_%d", userID)
					_ = products.Insert(transaction, Product{
						ID:    productID,
						Title: fmt.Sprintf("Product-%d", userID),
						Price: float64(10 + userID),
					})

					// 3. Insert order
					orderID := fmt.Sprintf("o_%d", userID)
					_ = orders.Insert(transaction, Order{
						ID:        orderID,
						UserID:    userID,
						Amount:    float64(50 + userID),
						CreatedAt: time.Now(),
					})

					// 4. Insert review
					reviewID := fmt.Sprintf("r_%d", userID)
					_ = reviews.Insert(transaction, Review{
						ID:        reviewID,
						ProductID: productID,
						Rating:    5,
					})
				}
				return nil
			})
			if err != nil {
				testingT.Errorf("writer transaction failed: %v", err)
			}
		}(writerIndex)
	}

	// Readers querying mixed tables
	for readerIndex := 0; readerIndex < numQueries; readerIndex++ {
		go func() {
			defer waitGroup.Done()
			randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for index := 0; index < queriesPerQueryer; index++ {
				target := randomGenerator.Intn(numWriters * opsPerWriter)
				
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

	waitGroup.Wait()

	// Verify records
	for index := 0; index < numWriters*opsPerWriter; index += 500 {
		_, found, err := users.FindByID(nil, index)
		if err != nil || !found {
			testingT.Fatalf("verification failed for user %d: found=%v, err=%v", index, found, err)
		}
	}
}

func TestStressAsyncMultiTable(testingT *testing.T) {
	tempDir, err := os.MkdirTemp("", "keeper-stress-async-multi-*")
	if err != nil {
		testingT.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	options := keeper.DefaultOptions()
	options.Durability = keeper.DurabilityAsync
	options.RegisterTypes(User{}, Order{}, Product{}, Review{})

	database, err := keeper.Open(tempDir, options)
	if err != nil {
		testingT.Fatalf("failed to open database: %v", err)
	}
	defer database.Close()

	users, _ := keeper.GetTable[int, User](database, "users")
	orders, _ := keeper.GetTable[string, Order](database, "orders")
	products, _ := keeper.GetTable[string, Product](database, "products")
	reviews, _ := keeper.GetTable[string, Review](database, "reviews")

	const numWriters = 10
	const opsPerWriter = 25000 // 10 * 25000 * 4 tables = 1,000,000 structs saved
	const numQueries = 10
	const queriesPerQueryer = 5

	var waitGroup sync.WaitGroup
	waitGroup.Add(numWriters + numQueries)

	// Writers performing mixed table insertions
	for writerIndex := 0; writerIndex < numWriters; writerIndex++ {
		go func(writerID int) {
			defer waitGroup.Done()
			err := database.Transaction(func(transaction *keeper.Transaction) error {
				for index := 0; index < opsPerWriter; index++ {
					userID := writerID*opsPerWriter + index
					
					_ = users.Insert(transaction, User{
						ID:    userID,
						Name:  fmt.Sprintf("User-%d", userID),
						Email: fmt.Sprintf("user-%d@example.com", userID),
					})

					productID := fmt.Sprintf("p_%d", userID)
					_ = products.Insert(transaction, Product{
						ID:    productID,
						Title: fmt.Sprintf("Product-%d", userID),
						Price: float64(10 + userID),
					})

					orderID := fmt.Sprintf("o_%d", userID)
					_ = orders.Insert(transaction, Order{
						ID:        orderID,
						UserID:    userID,
						Amount:    float64(50 + userID),
						CreatedAt: time.Now(),
					})

					reviewID := fmt.Sprintf("r_%d", userID)
					_ = reviews.Insert(transaction, Review{
						ID:        reviewID,
						ProductID: productID,
						Rating:    5,
					})
				}
				return nil
			})
			if err != nil {
				testingT.Errorf("writer transaction failed: %v", err)
			}
		}(writerIndex)
	}

	// Readers querying mixed tables
	for readerIndex := 0; readerIndex < numQueries; readerIndex++ {
		go func() {
			defer waitGroup.Done()
			randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
			for index := 0; index < queriesPerQueryer; index++ {
				target := randomGenerator.Intn(numWriters * opsPerWriter)
				
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

	waitGroup.Wait()

	// Verify records
	for index := 0; index < numWriters*opsPerWriter; index += 500 {
		_, found, err := users.FindByID(nil, index)
		if err != nil || !found {
			testingT.Fatalf("verification failed for user %d: found=%v, err=%v", index, found, err)
		}
	}
}
