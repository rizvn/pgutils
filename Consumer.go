package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type HandlerFunc func(ctx context.Context, msg *PgmqMessage)

type Consumer struct {
	queueName         string
	consumerCtx       context.Context
	consumerCancel    context.CancelFunc
	handlerFunc       HandlerFunc
	dbPool            *pgxpool.Pool
	RoutinesInflight  sync.WaitGroup
	msgChan           chan *PgmqMessage
	maxPollSecs       int
	visibilityTimeout int
	concurrentMsgs    int
}

func (r *Consumer) Init(dbPool *pgxpool.Pool, queueName string, concurrentMsgs, visibilityTimeout, maxPollSecs int, handler HandlerFunc) {
	r.queueName = queueName
	r.consumerCtx, r.consumerCancel = context.WithCancel(context.Background())
	r.handlerFunc = handler
	r.dbPool = dbPool
	r.maxPollSecs = maxPollSecs
	r.visibilityTimeout = visibilityTimeout
	r.concurrentMsgs = concurrentMsgs

	// Create queue if not exists
	r.createQueueIfNotExists()

	r.RoutinesInflight = sync.WaitGroup{}
	r.msgChan = make(chan *PgmqMessage, concurrentMsgs)
	go r.handleMessages()
}

func (r *Consumer) createQueueIfNotExists() {
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `SELECT * FROM pgmq.create($1)`, r.queueName)

	if err != nil {
		panic("failed to create queue")
	}
}

func (r *Consumer) Shutdown() {
	if r.consumerCtx != nil {
		fmt.Println("Shutting down consumer...")
		r.consumerCancel()
		log.Printf("Waiting for inflight routines to complete...")
		r.RoutinesInflight.Wait()
		fmt.Println("Consumer shut down complete.")
	}
}

func (r *Consumer) start() {

	// start message handler
	//go r.handleMessages()

	// start consumer routine
	go func() {
		// connect for this consumer
		conn := r.getConnection()
		defer conn.Release()

		for {
			select {

			// check for shutdown
			case <-r.consumerCtx.Done():
				fmt.Println("Shutting down consumer...")
				return

			default:
				fmt.Println("Polling for messages...")
				rows, err := conn.Query(r.consumerCtx, `
					SELECT * FROM pgmq.read_with_poll(
					  queue_name => $1,
					  vt         => $2,
					  qty        => $3,
					  max_poll_seconds  => $4
					);
				`, r.queueName, r.visibilityTimeout, 1, r.maxPollSecs)

				if err != nil {
					panic("failed to read messages")
				}

				msg := &PgmqMessage{}

				// read message
				for rows.Next() {
					err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message, &msg.Headers)
					if err != nil {
						panic("failed to scan row")
					}
					r.msgChan <- msg
				}
				rows.Close()
			}
		}
	}()
}

func (r *Consumer) getConnection() *pgxpool.Conn {
	conn, err := r.dbPool.Acquire(context.Background())

	if err != nil {
		panic("failed to acquire connection from pool")
	}
	return conn
}

func (r *Consumer) handleMessages() {
	for {
		// wait for message
		msg := <-r.msgChan

		// process message in a new goroutine
		go func() {
			r.RoutinesInflight.Add(1)
			defer r.RoutinesInflight.Done()

			ticker := time.NewTicker(time.Duration(r.visibilityTimeout/2) * time.Second)
			defer ticker.Stop()

			go func() {
				for {
					select {
					case <-ticker.C:
						r.updateVisbilityTimeout(msg)
					case <-r.consumerCtx.Done():
						ticker.Stop()
						return
					}
				}
			}()

			r.handlerFunc(context.Background(), msg)
			r.deleteMsg(msg)
		}()
	}
}

func (r *Consumer) deleteMsg(msg *PgmqMessage) {
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `
					SELECT * FROM pgmq.delete(
        				queue_name => $1,
        				msg_id     => $2
              		);`, r.queueName, msg.MsgID)

	if err != nil {
		fmt.Printf("failed to delete message %d: %v\n", msg.MsgID, err)
	}
}

func (r *Consumer) updateVisbilityTimeout(msg *PgmqMessage) {
	fmt.Printf("Extending visibility timeout for message %d by %d secs\n", msg.MsgID, r.visibilityTimeout)
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `
					SELECT * FROM pgmq.update_vt(
						queue_name => $1,
						msg_id     => $2,
						vt         => $3
			  		);`, r.queueName, msg.MsgID, r.visibilityTimeout)

	if err != nil {
		log.Printf("failed to update visibility timeout for message %d: %v\n", msg.MsgID, err)
	}
}
