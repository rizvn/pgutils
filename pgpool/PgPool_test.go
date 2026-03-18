package pgpool_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/rizvn/pgutil/pgpool"
	"github.com/rizvn/pgutil/testutil"
)

func TestPgPoolConnection(t *testing.T) {
	// Start Postgres test container
	ctr, dsn, err := testutil.StartPgTestContainer()
	if err != nil {
		t.Fatal(err)
	}

	port, err := ctr.MappedPort(context.Background(), "5432")
	if err != nil {
		t.Fatal(fmt.Sprintf("failed to get mapped port: %v", err))
	}

	defer func() { _ = ctr.Terminate(context.Background()) }()

	dbPool, err := pgpool.NewPgPool("localhost", port.Port(), "app_admin", "app_admin", "app_db", "")
	if err != nil {
		t.Fatalf("failed to create pg pool: %v", err)
	}
	dbPool.DSN = dsn

	t.Run("Test connection", func(t *testing.T) {
		err := dbPool.Connect()
		if err != nil {
			t.Fatal(err)
		}
	})
}
