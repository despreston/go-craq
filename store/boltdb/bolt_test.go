package boltdb

import (
	"os"
	"testing"

	"github.com/despreston/go-craq/store/storetest"
)

func TestStorer(t *testing.T) {
	// Truncate the db file if it exists so the tests start fresh.
	fd, err := os.OpenFile("test.db", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Failed to open database file\n %#v", err.Error())
	}

	defer func() {
		fd.Close()
		os.Remove("test.db")
	}()

	storetest.Run(t, func(name string, test storetest.Test) {
		// Clear the database file before each test.
		fd.Truncate(0)

		db := New("test.db", "test-bucket")
		if err := db.Connect(); err != nil {
			t.Fatalf("Unexpected error connecting to test database\n  %#v", err.Error())
		}

		defer db.DB.Close()
		test(t, db)
	})

}
