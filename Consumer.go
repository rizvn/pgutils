package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/panics"
)

type HandlerFunc func(ctx context.Context, conn *pgx.Conn, msg *Message)

type Consumer struct {
	queueName         string
	visibilityTimeout int
	fetchCount        int
	maxRoutines       chan bool
	routinesInFlight  sync.WaitGroup
	consumerCtx       context.Context
	consumerCancel    context.CancelFunc
	handlerFunc       HandlerFunc
	dbPool            *pgxpool.Pool
	RoutinesInFlight  sync.WaitGroup
}

type Message struct {
	MsgID      int64      `db:"msg_id"`
	ReadCount  int        `db:"read_ct"`
	EnqueuedAt *time.Time `db:"enqueued_at"`
	VT         *time.Time `db:"vt"`
	Message    *string    `db:"message"`
	Headers    *string    `db:"headers"`
}

func (r *Consumer) Init(queueName string, visibilityTimeoutSecs int, fetchCount int, maxRoutines int, handler HandlerFunc) {
	r.queueName = queueName
	r.visibilityTimeout = visibilityTimeoutSecs
	r.fetchCount = fetchCount
	r.maxRoutines = make(chan bool, maxRoutines)
	r.consumerCtx, r.consumerCancel = context.WithCancel(context.Background())
	r.handlerFunc = handler
	var err error
	r.dbPool, err = pgxpool.New(context.Background(), "postgres://app_admin:app_admin@localhost:5432/app_db")
	panics.OnError(err, "failed to create pgx pool")

	// Create queue if not exists
	r.createQueueIfNotExists()
}

func (r *Consumer) createQueueIfNotExists() {
	conn := r.getConnection()
	defer conn.Close(context.Background())
	_, err := conn.Exec(context.Background(), fmt.Sprintf(`SELECT * FROM pgmq.create('%s')`, r.queueName))
	panics.OnError(err, "failed to create queue")
}

func (r *Consumer) Shutdown() {
	if r.consumerCtx != nil {
		fmt.Println("Shutting down consumer...")
		r.consumerCancel()
	}
}

func (r *Consumer) start() {
	// start consumer routine
	go func() {
		// connect for this consumer
		consumerConn := r.getConnection()
		defer consumerConn.Close(r.consumerCtx)

		for {
			select {

			// check for shutdown
			case <-r.consumerCtx.Done():
				fmt.Println("Shutting down consumer...")
				return

			default:
				fmt.Println("Polling for messages...")
				rows, err := consumerConn.Query(r.consumerCtx, fmt.Sprintf("SELECT * FROM pgmq.read_with_poll('%s', %d, %d)", r.queueName, r.visibilityTimeout, r.fetchCount))
				panics.OnError(err, "failed to read messages")

				msg := &Message{}
				msg.MsgID = -1

				// read message
				for rows.Next() {
					err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message, &msg.Headers)
					panics.OnError(err, "failed to scan row")
				}

				// no message
				if msg.MsgID == -1 {
					continue
				}

				// acquire routine slot
				r.maxRoutines <- true
				go r.handleMessage(r.consumerCtx, msg)
			}
		}
	}()

	// listen for interrupt signal to shutdown consumer
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		println("Received interrupt signal, shutting down consumer...")
		r.consumerCancel()

		println("Waiting for in-flight routines to complete...")
		r.routinesInFlight.Wait()
		println("All routines completed, exiting.")
	}()
}

func (r *Consumer) getConnection() *pgx.Conn {
	conn, err := r.dbPool.Acquire(context.Background())
	panics.OnError(err, "failed to acquire connection from pool")
	return conn.Conn()
}

func (r *Consumer) handleMessage(consumerCtx context.Context, msg *Message) {
	handlerCtx, cancel := context.WithCancel(consumerCtx)
	conn := r.getConnection()
	r.routinesInFlight.Add(1)
	defer conn.Close(handlerCtx)
	defer cancel()
	defer r.routinesInFlight.Done()

	// release routine slot
	defer func() { <-r.maxRoutines }()

	fmt.Printf("Received message:  %v\n", msg)
	go r.extendVisibilityTimeout(handlerCtx, msg)

	r.handlerFunc(handlerCtx, conn, msg)

	fmt.Printf("Processed message ID: %d\n", msg.MsgID)
	_, err := conn.Exec(context.Background(), fmt.Sprintf("SELECT * FROM pgmq.delete('%s',%d)", r.queueName, msg.MsgID))
	fmt.Printf("Deleted message ID: %d\n", msg.MsgID)
	panics.OnError(err, "failed to delete message")
}

func (r *Consumer) extendVisibilityTimeout(ctx context.Context, msg *Message) {
	conn := r.getConnection()
	defer conn.Close(ctx)

	ticker := time.NewTicker(time.Duration(r.visibilityTimeout/2) * time.Second)
	defer ticker.Stop()

	for {
		select {
		// when cancelled
		case <-ctx.Done():
			fmt.Printf("Stopping visibility timeout extension for message ID: %d\n", msg.MsgID)
			return
		case <-ticker.C:
			_, err := conn.Exec(ctx, fmt.Sprintf("select * from pgmq.set_vt('%s', %d, %d)", r.queueName, msg.MsgID, r.visibilityTimeout))
			fmt.Printf("Extending visibility timeout for message ID: %d\n", msg.MsgID)
			if err != nil {
				fmt.Printf("Failed to update visible time for message id=%d: %v\n", msg.MsgID, err)
			}
		}
	}
}
