package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rizvn/panics"
)

func TestConsumer(t *testing.T) {

	consumer := &Consumer{}

	handler := func(ctx context.Context, conn *pgx.Conn, msg *Message) {
		// sleep to simulate processing
		time.Sleep(10 * time.Second)
	}

	consumer.Init("test_queue", 6, 1, 5, handler)
	consumer.start()

	// start producer routine
	go func() {
		ctx := context.Background()
		conn := consumer.getConnection()
		defer conn.Close(ctx)

		// producer function
		ticker := time.NewTicker(3 * time.Second)

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

	//wait for all in-flight routines to complete, on shutdown
	consumer.routinesInFlight.Wait()

	fmt.Println("Received SIGINT, shutting down.")
}
