package masterkeeper_test

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	keeper "github.com/lemadane/masterkeeper"
)

func TestSQLiteMigrationIntegration(testingT *testing.T) {
	// 1. Setup masterkeeper database
	options := keeper.DefaultOptions()
	options.RegisterTypes(MigrationUser{})

	dbDir := testingT.TempDir()
	database, err := keeper.Open(dbDir, options)
	if err != nil {
		testingT.Fatalf("failed to open masterkeeper: %v", err)
	}

	userTable, err := keeper.GetTable[string, MigrationUser](database, "users")
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to get table: %v", err)
	}

	// 2. Insert test data in masterkeeper
	testUser1 := MigrationUser{
		ID:      "usr_100",
		Name:    "Bob Smith",
		Email:   "bob@example.com",
		Age:     42,
		Score:   88.2,
		Active:  true,
		Created: time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
		Blob:    []byte("sqlite integration test"),
		Metadata: MigrationNestedStruct{
			Tag:   "moderator",
			Score: 100,
		},
	}

	testUser2 := MigrationUser{
		ID:      "usr_200",
		Name:    "Charlie Brown",
		Email:   "charlie@example.com",
		Age:     28,
		Score:   73.4,
		Active:  false,
		Created: time.Date(2026, 7, 26, 13, 0, 0, 0, time.UTC),
		Blob:    []byte("another sqlite user"),
		Metadata: MigrationNestedStruct{
			Tag:   "user",
			Score: 10,
		},
	}

	err = database.Transaction(func(transaction *keeper.Transaction) error {
		if err := userTable.Insert(transaction, testUser1); err != nil {
			return err
		}
		if err := userTable.Insert(transaction, testUser2); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to insert data: %v", err)
	}

	// 3. Open SQLite Database
	sqlitePath := filepath.Join(testingT.TempDir(), "test.db")
	sqlDatabase, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		database.Close()
		testingT.Fatalf("failed to open SQLite connection: %v", err)
	}
	defer sqlDatabase.Close()

	// 4. Run Migration
	err = keeper.Migrate(database, sqlDatabase, keeper.DialectSQLite)
	if err != nil {
		database.Close()
		testingT.Fatalf("migration to SQLite failed: %v", err)
	}

	// Close masterkeeper
	database.Close()

	// 5. Query and Assert data from SQLite
	rows, err := sqlDatabase.Query(`SELECT "ID", "Name", "Email", "Age", "Score", "Active", "Created", "Blob", "Metadata" FROM "users" ORDER BY "ID" ASC`)
	if err != nil {
		testingT.Fatalf("failed to query sqlite database: %v", err)
	}
	defer rows.Close()

	var migratedUsers []MigrationUser
	for rows.Next() {
		var user MigrationUser
		var createdString string
		var blob []byte
		var metadataJSON string
		var activeInteger int

		err := rows.Scan(
			&user.ID,
			&user.Name,
			&user.Email,
			&user.Age,
			&user.Score,
			&activeInteger,
			&createdString,
			&blob,
			&metadataJSON,
		)
		if err != nil {
			testingT.Fatalf("failed to scan sqlite row: %v", err)
		}

		user.Active = activeInteger != 0
		user.Blob = blob

		// Parse created time
		user.Created, err = time.Parse(time.RFC3339Nano, createdString)
		if err != nil {
			testingT.Fatalf("failed to parse created time: %v", err)
		}

		// Parse nested struct
		err = json.Unmarshal([]byte(metadataJSON), &user.Metadata)
		if err != nil {
			testingT.Fatalf("failed to unmarshal nested struct metadata: %v", err)
		}

		migratedUsers = append(migratedUsers, user)
	}

	if len(migratedUsers) != 2 {
		testingT.Fatalf("expected 2 users in sqlite, got %d", len(migratedUsers))
	}

	// Assert User 1
	if migratedUsers[0].ID != testUser1.ID ||
		migratedUsers[0].Name != testUser1.Name ||
		migratedUsers[0].Email != testUser1.Email ||
		migratedUsers[0].Age != testUser1.Age ||
		migratedUsers[0].Score != testUser1.Score ||
		migratedUsers[0].Active != testUser1.Active ||
		!migratedUsers[0].Created.Equal(testUser1.Created) ||
		string(migratedUsers[0].Blob) != string(testUser1.Blob) ||
		migratedUsers[0].Metadata.Tag != testUser1.Metadata.Tag ||
		migratedUsers[0].Metadata.Score != testUser1.Metadata.Score {
		testingT.Errorf("mismatch on user 1. Got: %+v, Want: %+v", migratedUsers[0], testUser1)
	}

	// Assert User 2
	if migratedUsers[1].ID != testUser2.ID ||
		migratedUsers[1].Name != testUser2.Name ||
		migratedUsers[1].Email != testUser2.Email ||
		migratedUsers[1].Age != testUser2.Age ||
		migratedUsers[1].Score != testUser2.Score ||
		migratedUsers[1].Active != testUser2.Active ||
		!migratedUsers[1].Created.Equal(testUser2.Created) ||
		string(migratedUsers[1].Blob) != string(testUser2.Blob) ||
		migratedUsers[1].Metadata.Tag != testUser2.Metadata.Tag ||
		migratedUsers[1].Metadata.Score != testUser2.Metadata.Score {
		testingT.Errorf("mismatch on user 2. Got: %+v, Want: %+v", migratedUsers[1], testUser2)
	}
}
