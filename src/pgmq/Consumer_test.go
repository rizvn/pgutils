package pgmq

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/pgutils/testutil"
)

func TestConsumer(t *testing.T) {
	// create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)

	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	// create pgx pool
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		panic("failed to parse pgx config")
	}
	config.MaxConns = 20 // set your desired max connections
	dbPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic("failed to create pgx pool")
	}

	// create consumer
	var recvd *PgmqMessage = nil

	c := &Consumer{}
	c.QueueName = "test_queue"
	c.DbPool = dbPool

	// message handler runs when a message is received
	c.MessageHandler = func(ctx context.Context, msg *PgmqMessage) {
		// capture received message
		recvd = msg

		// cancel context to end test
		cancel()
	}

	c.Init()
	c.Start()

	// create producer
	p := &Producer{}
	p.DbPool = dbPool
	p.Init()

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
