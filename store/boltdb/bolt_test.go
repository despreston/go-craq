package boltdb

import (
	"os"
	"testing"

	"github.com/despreston/go-craq/store/storetest"
)

func TestStorer(t *testing.T) {
	storetest.Run(t, func(name string, test storetest.Test) {
		db := New("test.db", "test-bucket")
		err := db.Connect()
		if err != nil {
			t.Fatalf("Unexpected error connecting to test database\n  %#v", err)
		}
		defer func() {
			db.DB.Close()
			os.Remove("test.db")
		}()
		test(t, db)
	})
}
