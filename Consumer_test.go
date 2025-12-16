package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/panics"
)

func TestConsumer(t *testing.T) {
	consumer := &Consumer{}

	config, err := pgxpool.ParseConfig("postgres://app_admin:app_admin@localhost:5432/app_db")
	if err != nil {
		panic("failed to parse pgx config")
	}
	config.MaxConns = 20 // set your desired max connections
	dbPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic("failed to create pgx pool")
	}

	handler := func(ctx context.Context, msg *PgmqMessage) {
		fmt.Printf("Processing message: %v\n", msg)
		//time.Sleep(8 * time.Second) // Simulate processing time
		fmt.Printf("Finished processing message: %v\n", msg)
	}

	consumer.Init(dbPool, "test_queue", 10, 10, 10, handler)
	consumer.start()

	// Wait for SIGINT (Ctrl+C) to exit gracefully
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	consumer.Shutdown()

	fmt.Println("Received SIGINT, shutting down.")
}

func TestProducer(t *testing.T) {
	producer := &Producer{}

	dbPool, err := pgxpool.New(context.Background(), "postgres://app_admin:app_admin@localhost:5432/app_db")
	panics.OnError(err, "failed to create pgx pool")

	producer.Init(dbPool)
	// start producer routine
	go func() {

		// producer function
		ticker := time.NewTicker(1 * time.Second)

		for {
			select {
			case <-ticker.C:
				fmt.Println("Producing message...")
				producer.Produce("test_queue", `{"content": "Hello, World!"}`, "")
			}
		}
	}()

	// Wait for SIGINT (Ctrl+C) to exit gracefully
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

}
