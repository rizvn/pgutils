package pgmq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type MessageHandlerFunc func(ctx context.Context, msg *PgmqMessage)

type Consumer struct {
	QueueName      string             `required:"true"`
	MessageHandler MessageHandlerFunc `required:"true"`
	DbPool         *sql.DB            `required:"true"`

	//-- configurable fields with defaults
	MaxPollSecs        int
	VisibilityTimeout  int
	ConcurrentMsgs     int
	ArchiveAfterHandle bool

	//-- internal fields
	msgChan          chan *PgmqMessage
	consumerCtx      context.Context
	routinesInflight sync.WaitGroup
	consumerCancel   context.CancelFunc
}

func (r *Consumer) Init() {
	r.consumerCtx, r.consumerCancel = context.WithCancel(context.Background())

	// Set defaults
	if r.MaxPollSecs == 0 {
		r.MaxPollSecs = 10
	}

	if r.VisibilityTimeout == 0 {
		r.VisibilityTimeout = 10
	}

	if r.ConcurrentMsgs == 0 {
		r.ConcurrentMsgs = 10
	}

	// Create queue if not exists
	r.createQueueIfNotExists()

	r.routinesInflight = sync.WaitGroup{}
	r.msgChan = make(chan *PgmqMessage, r.ConcurrentMsgs)
}

func (r *Consumer) createQueueIfNotExists() {
	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.create($1)`, r.QueueName)

	if err != nil {
		panic("failed to create queue")
	}
}

func (r *Consumer) ShutdownWithWait() {
	if r.consumerCtx != nil {
		log.Println("Shutting down consumer...")
		r.consumerCancel()
		log.Printf("Waiting for inflight routines to complete...")
		r.routinesInflight.Wait()
		log.Println("Consumer shut down complete.")
	}
}

func (r *Consumer) Start() {

	// Start message handler
	go r.handleMessages()

	// Start consumer routine
	go func() {

		for {
			select {

			// check for shutdown
			case <-r.consumerCtx.Done():
				log.Println("Shutting down consumer...")
				return

			default:
				log.Println("Polling for messages...")
				rows, err := r.DbPool.Query(`
					SELECT * FROM pgmq.read_with_poll(
					  queue_name => $1,
					  vt         => $2,
					  qty        => $3,
					  max_poll_seconds  => $4
					);
				`, r.QueueName, r.VisibilityTimeout, 1, r.MaxPollSecs)

				if err != nil {
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) {
						if pgErr.Code == "57P01" {
							log.Println("Query cancelled, shutting down consumer...")
							return // query was cancelled
						}
					}
					panic(fmt.Sprintf("failed to read messages, %v", err))
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
				err = rows.Close()
				if err != nil {
					log.Printf("failed to close rows: %v\n", err)
				}
			}
		}
	}()
}

func (r *Consumer) handleMessages() {
	for {
		// wait for message
		msg := <-r.msgChan

		// process message in a new goroutine
		go func() {
			r.routinesInflight.Add(1)
			defer r.routinesInflight.Done()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			r.visibilityExtender(ctx, msg)
			r.MessageHandler(context.Background(), msg)
			cancel()

			if r.ArchiveAfterHandle {
				r.ArchiveMessage(msg)
			} else {
				r.DeleteMessage(msg)
			}
		}()
	}
}

// visibilityExtender periodically extends the visibility timeout of a message
// whilst the message is being processed so other processes cannot read it
func (r *Consumer) visibilityExtender(ctx context.Context, msg *PgmqMessage) {
	ticker := time.NewTicker(time.Duration(r.VisibilityTimeout/2) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				r.updateVisibilityTimeout(msg)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (r *Consumer) DeleteMessage(msg *PgmqMessage) {
	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.delete(
        				queue_name => $1,
        				msg_id     => $2
              		);`, r.QueueName, msg.MsgID)

	if err != nil {
		log.Printf("failed to delete message %d: %v\n", msg.MsgID, err)
	}
}

func (r *Consumer) ArchiveMessage(msg *PgmqMessage) {
	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.archive(
        				queue_name => $1,
        				msg_id     => $2
              		);`, r.QueueName, msg.MsgID)

	if err != nil {
		log.Printf("failed to archive message %d: %v\n", msg.MsgID, err)
	}
}

func (r *Consumer) PurgeQueue(msg *PgmqMessage) {
	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.purge_queue(
        				queue_name => $1,
              		);`, r.QueueName)

	if err != nil {
		log.Printf("failed to archive message %d: %v\n", msg.MsgID, err)
	}
}

func (r *Consumer) updateVisibilityTimeout(msg *PgmqMessage) {
	log.Printf("Extending visibility timeout for message %d by %d secs\n", msg.MsgID, r.VisibilityTimeout)

	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.update_vt(
						queue_name => $1,
						msg_id     => $2,
						vt         => $3
			  		);`, r.QueueName, msg.MsgID, r.VisibilityTimeout)

	if err != nil {
		log.Printf("failed to update visibility timeout for message %d: %v\n", msg.MsgID, err)
	}
}
