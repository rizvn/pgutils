package pgmq

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/pgutils/testutil"
)

func TestProducer(t *testing.T) {
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	dbPool, err := pgxpool.New(context.Background(), dsn)

	if err != nil {
		t.Fatalf("failed to create pgx pool: %v", err)
	}

	conn, err := dbPool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	_, err = conn.Exec(context.Background(), `SELECT *  FROM pgmq.create('test_queue')`)
	if err != nil {
		t.Fatalf("failed to create pgmq queue: %v", err)
	}

	p := &Producer{}
	p.DbPool = dbPool
	p.Init()

	p.Produce("test_queue", `{"content": "Hello, World!"}`, "{}")

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
