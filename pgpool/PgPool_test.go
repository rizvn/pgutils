package pgpool_test

import (
	"context"
	"testing"

	"github.com/rizvn/pgutils/pgpool"
	"github.com/rizvn/pgutils/testutil"
)

func TestPgPoolConnection(t *testing.T) {
	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	dbPool := &pgpool.PgPool{}
	dbPool.DSN = dsn

	err := dbPool.Init()
	if err != nil {
		t.Fatalf("failed to initialize db pool: %v", err)
	}

	t.Run("Test connection", func(t *testing.T) {
		value := 0
		err := dbPool.DbPool.QueryRow("SELECT 1").Scan(&value)
		if err != nil {
			t.Fatalf("failed to connect to database: %v", err)
		} // simple query to ensure connection is working

	})
}
