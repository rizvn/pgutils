package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MessageHandlerFunc func(ctx context.Context, msg *PgmqMessage)

type Consumer struct {
	QueueName          string             `required:"true"`
	MessageHandler     MessageHandlerFunc `required:"true"`
	MaxPollSecs        int                `required:"true"`
	VisibilityTimeout  int                `required:"true"`
	ConcurrentMsgs     int                `required:"true"`
	ArchiveAfterHandle bool               `required:"true"`
	DbPool             *pgxpool.Pool      `required:"true"`

	//-- internal fields
	msgChan          chan *PgmqMessage
	consumerCtx      context.Context
	routinesInflight sync.WaitGroup
	consumerCancel   context.CancelFunc
}

func (r *Consumer) Init() {
	r.consumerCtx, r.consumerCancel = context.WithCancel(context.Background())

	// Create queue if not exists
	r.createQueueIfNotExists()

	r.routinesInflight = sync.WaitGroup{}
	r.msgChan = make(chan *PgmqMessage, r.ConcurrentMsgs)
}

func (r *Consumer) createQueueIfNotExists() {
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `SELECT * FROM pgmq.create($1)`, r.QueueName)

	if err != nil {
		panic("failed to create queue")
	}
}

func (r *Consumer) Shutdown() {
	if r.consumerCtx != nil {
		fmt.Println("Shutting down consumer...")
		r.consumerCancel()
		log.Printf("Waiting for inflight routines to complete...")
		r.routinesInflight.Wait()
		fmt.Println("Consumer shut down complete.")
	}
}

func (r *Consumer) Start() {

	// Start message handler
	go r.handleMessages()

	// Start consumer routine
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
				`, r.QueueName, r.VisibilityTimeout, 1, r.MaxPollSecs)

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
	conn, err := r.DbPool.Acquire(context.Background())

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
			r.routinesInflight.Add(1)
			defer r.routinesInflight.Done()

			ticker := time.NewTicker(time.Duration(r.VisibilityTimeout/2) * time.Second)
			defer ticker.Stop()

			r.VisibilityExtender(*ticker, msg)
			r.MessageHandler(context.Background(), msg)

			if r.ArchiveAfterHandle {
				r.ArchiveMessage(msg)
			} else {
				r.DeleteMessage(msg)
			}
		}()
	}
}

func (r *Consumer) VisibilityExtender(ticker time.Ticker, msg *PgmqMessage) {
	go func() {
		for {
			select {
			case <-ticker.C:
				r.updateVisibilityTimeout(msg)
			case <-r.consumerCtx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (r *Consumer) DeleteMessage(msg *PgmqMessage) {
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `
					SELECT * FROM pgmq.delete(
        				queue_name => $1,
        				msg_id     => $2
              		);`, r.QueueName, msg.MsgID)

	if err != nil {
		fmt.Printf("failed to delete message %d: %v\n", msg.MsgID, err)
	}
}

func (r *Consumer) ArchiveMessage(msg *PgmqMessage) {
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `
					SELECT * FROM pgmq.archive(
        				queue_name => $1,
        				msg_id     => $2
              		);`, r.QueueName, msg.MsgID)

	if err != nil {
		fmt.Printf("failed to archive message %d: %v\n", msg.MsgID, err)
	}
}

func (r *Consumer) PurgeQueue(msg *PgmqMessage) {
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `
					SELECT * FROM pgmq.purge_queue(
        				queue_name => $1,
              		);`, r.QueueName)

	if err != nil {
		fmt.Printf("failed to archive message %d: %v\n", msg.MsgID, err)
	}
}

func (r *Consumer) updateVisibilityTimeout(msg *PgmqMessage) {
	fmt.Printf("Extending visibility timeout for message %d by %d secs\n", msg.MsgID, r.VisibilityTimeout)
	conn := r.getConnection()
	defer conn.Release()
	_, err := conn.Exec(context.Background(), `
					SELECT * FROM pgmq.update_vt(
						queue_name => $1,
						msg_id     => $2,
						vt         => $3
			  		);`, r.QueueName, msg.MsgID, r.VisibilityTimeout)

	if err != nil {
		log.Printf("failed to update visibility timeout for message %d: %v\n", msg.MsgID, err)
	}
}
