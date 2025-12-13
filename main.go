package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rizvn/panics"
)

type Consumer struct {
	queueName        string
	hideForSecs      int
	fetchCount       int
	maxRoutines      chan bool
	routinesInFlight sync.WaitGroup
}

func (r *Consumer) Init() {
	r.queueName = "my_queue"
	r.hideForSecs = 10
	r.fetchCount = 1
	r.maxRoutines = make(chan bool, 10)

	// Create queue if not exists
	conn := r.getConnection()
	defer conn.Close(context.Background())
	_, err := conn.Exec(context.Background(), fmt.Sprintf(`SELECT * FROM pgmq.create('%s')`, r.queueName))
	panics.OnError(err, "failed to create queue")
}

func (r *Consumer) start() {
	ctx := context.Background()

	pubConn := r.getConnection()
	defer pubConn.Close(ctx)

	subConn := r.getConnection()
	defer subConn.Close(ctx)

	// start producer routine
	go func(conn *pgx.Conn) {
		// producer function
		ticker := time.NewTicker(3 * time.Second)

		for {
			select {
			case <-ticker.C:
				_, err := conn.Exec(context.Background(), fmt.Sprintf(`SELECT * from pgmq.send(
									  queue_name  => '%s',
									  msg         => '%s'
									)`, r.queueName, `{"foo": "bar2"}`))
				panics.OnError(err, "failed to send message")
				fmt.Println("Produced a new message.")
			}
		}
	}(pubConn)

	// start consumer routine
	go func(ctx context.Context, conn *pgx.Conn) {
		for {
			select {

			default:
				fmt.Println("Polling for messages...")
				rows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM pgmq.read_with_poll('%s', %d, %d)", r.queueName, r.hideForSecs, r.fetchCount))
				panics.OnError(err, "failed to read messages")

				var msgId int64
				var msgPayload string
				msgId = -1

				// read message
				for rows.Next() {
					err := rows.Scan(&msgId, nil, nil, nil, &msgPayload, nil)
					panics.OnError(err, "failed to scan row")
				}

				if msgId == -1 {
					continue
				}

				go r.handleMessage(msgId, msgPayload, r.queueName)

			}

		}
	}()

}

func (r *Consumer) getConnection() *pgx.Conn {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://app_admin:app_admin@localhost:5432/app_db")
	panics.OnError(err, "failed to connect to database")
	return conn
}

func (r *Consumer) handleMessage(msgId int64, msgPayload string, queueName string) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := r.getConnection()
	r.routinesInFlight.Add(1)
	defer conn.Close(ctx)
	defer cancel()
	defer r.routinesInFlight.Done()

	fmt.Printf("Received message ID: %d, Payload: %s\n", msgId, msgPayload)

	go r.extendVisibilityTimeout(ctx, msgId, queueName)

	// sleep to simulate processing
	time.Sleep(10 * time.Second)

	fmt.Printf("Processed message ID: %d\n", msgId)
	_, err := conn.Exec(ctx, fmt.Sprintf("SELECT * FROM pgmq.delete('%s',%d)", queueName, msgId))
	fmt.Printf("Deleted message ID: %d\n", msgId)
	panics.OnError(err, "failed to delete message")
}

func (r *Consumer) extendVisibilityTimeout(ctx context.Context, msgId int64, queueName string) {
	conn := r.getConnection()
	defer conn.Close(ctx)

	ticker := time.NewTicker(time.Duration(r.hideForSecs/2) * time.Second)
	defer ticker.Stop()

	for {
		select {

		// when cancelled
		case <-ctx.Done():
			fmt.Printf("Stopping visibility timeout extension for message ID: %d\n", msgId)
			return
		case <-ticker.C:
			_, err := conn.Exec(ctx, fmt.Sprintf("select * from pgmq.set_vt('%s', %d, %d)", queueName, msgId, r.hideForSecs))
			fmt.Printf("Extending visibility timeout for message ID: %d\n", msgId)
			if err != nil {
				fmt.Printf("Failed to update visible time for message id=%d: %v\n", msgId, err)
			}
		}
	}
}

func main() {
	q := &Consumer{}
	q.Init()
	q.start()

	// Wait for SIGINT (Ctrl+C) to exit gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	//wait for all in-flight routines to complete, on shutdown
	q.routinesInFlight.Wait()

	fmt.Println("Received SIGINT, shutting down.")
}
