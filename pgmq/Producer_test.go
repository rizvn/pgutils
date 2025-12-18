package pgmq

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/pgutils/testutil"
)

func TestProducer(t *testing.T) {
	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	// Create pgx pool
	dbPool, err := pgxpool.New(context.Background(), dsn)

	if err != nil {
		t.Fatalf("failed to create pgx pool: %v", err)
	}

	// Arrange - ensure pgmq queue exists
	conn, err := dbPool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	// Create pgmq queue
	_, err = conn.Exec(context.Background(), `SELECT *  FROM pgmq.create('test_queue')`)
	if err != nil {
		t.Fatalf("failed to create pgmq queue: %v", err)
	}

	// Clean up pgmq queue so its empty
	_, err = conn.Exec(context.Background(), `SELECT * FROM pgmq.purge_queue('test_queue')`)
	if err != nil {
		t.Fatalf("failed to truncate pgmq queue: %v", err)
	}

	// Act

	// Create producer
	p := &Producer{}
	p.DbPool = dbPool
	p.Init()

	// Produce a message
	p.Produce("test_queue", `{"content": "Hello, World!"}`, "{}")

	// Assert - check if message was produced in pgmq table
	row, err := conn.Query(context.Background(), `SELECT count(0) FROM pgmq.q_test_queue`)
	if err != nil {
		t.Fatalf("failed to query pgmq queue: %v", err)
	}

	defer row.Close()

	var msgCount int
	for row.Next() {
		err = row.Scan(&msgCount)
		if err != nil {
			t.Fatalf("failed to scan message count: %v", err)
		}
	}

	if msgCount == 0 {
		t.Errorf("Expected to have produced messages, but message count is 0")
	}

}
