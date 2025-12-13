package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/rizvn/panics"
)

func TestConsumer(t *testing.T) {

	q := &Consumer{}
	q.Init("test_queue", 6, 1, 5)
	q.start()

	// start producer routine
	go func() {
		ctx := context.Background()
		conn := q.getConnection()
		defer conn.Close(ctx)

		// producer function
		ticker := time.NewTicker(3 * time.Second)

		for {
			select {
			case <-ticker.C:
				_, err := conn.Exec(context.Background(), fmt.Sprintf(`SELECT * from pgmq.send(
									  queue_name  => '%s',
									  msg         => '%s'
									)`, q.queueName, `{"foo": "bar2"}`))
				panics.OnError(err, "failed to send message")
				fmt.Println("Produced a new message.")
			}
		}
	}()

	// Wait for SIGINT (Ctrl+C) to exit gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	//wait for all in-flight routines to complete, on shutdown
	q.routinesInFlight.Wait()

	fmt.Println("Received SIGINT, shutting down.")
}
