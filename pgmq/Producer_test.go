package pgmq

import (
	"context"
	"database/sql"
	"testing"

	"github.com/rizvn/pgutils/testutil"
)

func TestProducer(t *testing.T) {
	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	// Create db pool
	dbPool, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to create db p pool pool %v", err)
	}
	dbPool.SetMaxOpenConns(20)

	// Create pgmq queue
	_, err = dbPool.Exec(`SELECT *  FROM pgmq.create('test_queue')`)
	if err != nil {
		t.Fatalf("failed to create pgmq queue: %v", err)
	}

	// Clean up pgmq queue so its empty
	_, err = dbPool.Exec(`SELECT * FROM pgmq.purge_queue('test_queue')`)
	if err != nil {
		t.Fatalf("failed to truncate pgmq queue: %v", err)
	}

	// Act

	// Create producer
	p := &Producer{
		DbPool: dbPool,
	}
	p.Init()

	// Produce a message
	p.Produce("test_queue", `{"content": "Hello, World!"}`, "{}")

	// Assert - check if message was produced in pgmq table
	row, err := dbPool.Query(`SELECT count(0) FROM pgmq.q_test_queue`)
	if err != nil {
		t.Fatalf("failed to query pgmq queue: %v", err)
	}

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
