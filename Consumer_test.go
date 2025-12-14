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
	dbPool, err := pgxpool.New(context.Background(), "postgres://app_admin:app_admin@localhost:5432/app_db")
	panics.OnError(err, "failed to create pgx pool")

	handler := func(ctx context.Context, msg *Message) {
		fmt.Printf("Processing message: %v\n", msg)
	}

	consumer.Init(dbPool, "test_queue", 10, handler)
	consumer.start()

	// Wait for SIGINT (Ctrl+C) to exit gracefully
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	consumer.Shutdown()

	//wait for all in-flight routines to complete, on shutdown
	consumer.routinesInFlight.Wait()

	fmt.Println("Received SIGINT, shutting down.")
}

func TestProducer(t *testing.T) {
	consumer := &Consumer{}

	dbPool, err := pgxpool.New(context.Background(), "postgres://app_admin:app_admin@localhost:5432/app_db")
	panics.OnError(err, "failed to create pgx pool")

	handler := func(ctx context.Context, msg *Message) {
		time.Sleep(10 * time.Second)
	}

	consumer.Init(dbPool, "test_queue", 10, handler)
	// start producer routine
	go func() {
		ctx := context.Background()
		conn := consumer.getConnection()
		defer conn.Close(ctx)

		// producer function
		ticker := time.NewTicker(1 * time.Second)

		for {
			select {
			case <-ticker.C:
				_, err := conn.Exec(context.Background(), fmt.Sprintf(`SELECT * from pgmq.send(
									  queue_name  => '%s',
									  msg         => '%s'
									)`, consumer.queueName, `{"foo": "bar2"}`))
				panics.OnError(err, "failed to send message")
				fmt.Println("Produced a new message.")
			}
		}
	}()

	// Wait for SIGINT (Ctrl+C) to exit gracefully
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	consumer.Shutdown()

}
