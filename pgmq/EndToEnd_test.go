package pgmq

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

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

	c := &Consumer{
		DbPool:             dbPool,
		QueueName:          "test_queue",
		ExponentialBackoff: 1,
	}

	count := 0
	done := make(chan bool, 1)

	// message handler runs when a message is received
	c.MessageHandler = func(ctx context.Context, msg *PgmqMessage) {

		fmt.Printf("Received message: %v\n", msg)
		count++
		if count == 100 {
			done <- true
		}

	}

	c.Init()
	c.Start()

	// create producer
	p := &Producer{
		DbPool: dbPool,
	}

	p.Init()

	// produce 100 messages
	for i := 0; i < 100; i++ {
		// send a test message
		p.Produce("test_queue", `{"content": "Hello, Test!"}`, "{}")
	}

	<-done

}
