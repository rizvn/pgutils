package pgmq_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/rizvn/pgutils/pgmq"
	"github.com/rizvn/pgutils/testutil"
)

func TestEndToEnd(t *testing.T) {
	// create context with timeout

	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	// Create db pool
	dbPool, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to create db p pool pool %v", err)
	}
	dbPool.SetMaxOpenConns(10)

	count := 0
	done := make(chan bool, 1)

	// message handler runs when a message is received
	handler := func(ctx context.Context, msg *pgmq.PgmqMessage) {

		fmt.Printf("Received message: %v\n", msg)
		count++
		if count == 100 {
			done <- true
		}

	}

	c, err := pgmq.NewConsumer(dbPool, "test_queue", handler, pgmq.WithExponentialBackoff(1))

	if err != nil {
		t.Fatal(err)
	}

	c.Start()

	// create producer
	p := pgmq.NewProducer(dbPool)

	// produce 100 messages
	for i := 0; i < 100; i++ {
		// send a test message
		p.Produce("test_queue", `{"content": "Hello, Test!"}`, "{}")
	}

	<-done

}
