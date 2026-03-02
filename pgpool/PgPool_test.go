package pgpool_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/rizvn/pgutils/pgpool"
	"github.com/rizvn/pgutils/testutil"
)

func TestPgPoolConnection(t *testing.T) {
	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()

	port, err := ctr.MappedPort(context.Background(), "5432")
	if err != nil {
		panic(fmt.Sprintf("failed to get mapped port: %v", err))
	}

	defer func() { _ = ctr.Terminate(context.Background()) }()

	dbPool, err := pgpool.NewPgPool("localhost", port.Port(), "app_admin", "app_admin", "app_db", "")
	if err != nil {
		t.Fatalf("failed to create pg pool: %v", err)
	}
	dbPool.DSN = dsn

	t.Run("Test connection", func(t *testing.T) {
		value := 0
		err := dbPool.DbPool.QueryRow("SELECT 1").Scan(&value)
		if err != nil {
			t.Fatalf("failed to connect to database: %v", err)
		} // simple query to ensure connection is working

	})
}
