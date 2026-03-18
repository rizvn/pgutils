package pgmq_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/rizvn/pgutil/pgmq"
	"github.com/rizvn/pgutil/testutil"
)

func TestConsumer(t *testing.T) {
	// create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)

	// Start Postgres test container
	ctr, dsn, err := testutil.StartPgTestContainer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ctr.Terminate(context.Background()) }()

	// Create db pool
	dbPool, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to create db p pool pool %v", err)
	}
	dbPool.SetMaxOpenConns(20)

	// create consumer
	var recvd *pgmq.PgmqMessage = nil

	c, err := pgmq.NewConsumer(dbPool, "test_queue",
		// message handler runs when a message is received
		func(ctx context.Context, msg *pgmq.PgmqMessage) {
			// capture received message
			recvd = msg

			// cancel context to end test
			cancel()
		})

	if err != nil {
		t.Fatal(err)
	}

	c.Start()

	// create producer
	p := pgmq.NewProducer(dbPool)

	// send a test message
	p.Produce("test_queue", `{"content": "Hello, Test!"}`, "{}")

	// wait for message or context timeout
	for {
		select {
		case <-ctx.Done():
			// check if message was received on cancel or timeout
			if recvd == nil {
				t.Errorf("Expected to receive a message, but none was received")
			}
			// shutdown consumer
			c.ShutdownWithWait()
			return
		}
	}
}
