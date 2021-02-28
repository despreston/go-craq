package mongodb

import (
	"context"
	"testing"

	"github.com/despreston/go-craq/store/storetest"
)

func TestStorer(t *testing.T) {
	db, err := New("craq-test")
	if err != nil {
		t.Fatalf("failed to start test database\n  %#v", err)
	}

	if err := db.Connect(context.TODO()); err != nil {
		t.Fatalf("failed to connect to database\n  %#v", err)
	}

	defer db.Disconnect(context.TODO())

	storetest.Run(t, func(name string, test storetest.Test) {
		defer db.DropCollection(context.TODO())
		test(t, db)
	})
}
