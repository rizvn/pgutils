package pgmq

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestProducer(t *testing.T) {
	producer := &Producer{}

	dbPool, err := pgxpool.New(context.Background(), "postgres://app_admin:app_admin@localhost:5432/app_db")
	if err != nil {
		panic("failed to create pgx pool")
	}

	producer.Init(dbPool)
	// Start producer routine
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
