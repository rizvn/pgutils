package pgmq

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConsumer(t *testing.T) {
	c := &Consumer{}

	config, err := pgxpool.ParseConfig("postgres://app_admin:app_admin@localhost:5432/app_db")
	if err != nil {
		panic("failed to parse pgx config")
	}
	config.MaxConns = 20 // set your desired max connections
	dbPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic("failed to create pgx pool")
	}

	c.MessageHandler = func(ctx context.Context, msg *PgmqMessage) {
		fmt.Printf("Processing message: %v\n", msg)
		//time.Sleep(8 * time.Second) // Simulate processing time
		fmt.Printf("Finished processing message: %v\n", msg)
	}

	c.QueueName = "test_queue"
	c.DbPool = dbPool

	c.Init()
	c.Start()

	// Wait for SIGINT (Ctrl+C) to exit gracefully
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	c.ShutdownWitWait()

	fmt.Println("Received SIGINT, shutting down.")
}
