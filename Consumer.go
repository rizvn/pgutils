package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/panics"
)

type HandlerFunc func(ctx context.Context, msg *Message)

type Consumer struct {
	queueName        string
	routinesInFlight sync.WaitGroup
	consumerCtx      context.Context
	consumerCancel   context.CancelFunc
	handlerFunc      HandlerFunc
	dbPool           *pgxpool.Pool
	msgs             chan *Message
}

func (r *Consumer) Init(dbPool *pgxpool.Pool, queueName string, concurrentMsgs int, handler HandlerFunc) {
	r.queueName = queueName
	r.consumerCtx, r.consumerCancel = context.WithCancel(context.Background())
	r.handlerFunc = handler
	var err error
	r.dbPool = dbPool
	panics.OnError(err, "failed to create pgx pool")

	// Create queue if not exists
	r.createQueueIfNotExists()

	r.msgs = make(chan *Message, concurrentMsgs)

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

	// start message handler
	go r.handleMessages()

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
				rows, err := consumerConn.Query(r.consumerCtx, fmt.Sprintf("SELECT * FROM pgmq.read('%s', 10, 1)", r.queueName))

				if rows.CommandTag().RowsAffected() == 0 {
					time.Sleep(1 * time.Second)
				}

				panics.OnError(err, "failed to read messages")

				if rows.CommandTag().RowsAffected() == 0 {
					rows.Close()
					continue
				}

				msg := &Message{}

				// read message
				for rows.Next() {
					err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message, &msg.Headers)
					panics.OnError(err, "failed to scan row")
				}

				r.msgs <- msg
				rows.Close()
			}
		}
	}()
}

func (r *Consumer) getConnection() *pgx.Conn {
	conn, err := r.dbPool.Acquire(context.Background())
	panics.OnError(err, "failed to acquire connection from pool")
	return conn.Conn()
}

func (r *Consumer) handleMessages() {

	for {
		select {
		case <-r.consumerCtx.Done():
			return

		case msg := <-r.msgs:
			go func() {
				// track in-flight routines
				r.routinesInFlight.Add(1)

				// remove from in-flight on completion
				defer r.routinesInFlight.Done()
				r.handlerFunc(context.Background(), msg)

				conn := r.getConnection()
				defer conn.Close(context.Background())
				_, err := conn.Exec(context.Background(), fmt.Sprintf("SELECT * FROM pgmq.delete('%s', %d)", r.queueName, msg.MsgID))
				if err != nil {
					fmt.Printf("failed to delete message %d: %v\n", msg.MsgID, err)
				}

			}()
		}
	}
}
