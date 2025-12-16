package pgmq

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/pgutils/testutil"
)

func TestConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	c := &Consumer{}
	p := &Producer{}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		panic("failed to parse pgx config")
	}
	config.MaxConns = 20 // set your desired max connections
	dbPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic("failed to create pgx pool")
	}

	var recvd *PgmqMessage = nil

	// set up consumer
	c.QueueName = "test_queue"
	c.DbPool = dbPool
	c.MessageHandler = func(ctx context.Context, msg *PgmqMessage) {
		recvd = msg
		cancel()
	}

	c.Init()
	c.Start()

	// set up producer
	p.DbPool = dbPool
	p.Init()
	p.Produce("test_queue", `{"content": "Hello, Test!"}`, "{}")

	for {
		select {
		case <-ctx.Done():
			if recvd == nil {
				t.Errorf("Expected to receive a message, but none was received")
			}
			// shutdown consumer
			c.ShutdownWithWait()
			return
		}
	}
}
