package pgmq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
	// PollingInterval is the number of seconds to wait between polling for new messages when none are found, default is 1 second
	PollingInterval int

	// VisibilityTimeout is the number of seconds a message is hidden from other consumers while being processed, default is 10 seconds
	VisibilityTimeout int

	// ConcurrentMsgs is the number of messages to process concurrently, default is 10
	ConcurrentMsgs int

	// ArchiveAfterHandle indicates whether to archive messages after they have been handled, default is false (messages are deleted)
	ArchiveAfterHandle bool

	// ExponentialBackoff is the number of seconds to increase the sleep time by when no messages are found, default is 0 seconds
	ExponentialBackoff int

	// ExponentialPollingLimit is the maximum number of seconds to sleep when no messages are found, default is 10 seconds
	ExponentialPollingLimit int

	//-- internal fields
	msgChan          chan *PgmqMessage
	consumerCtx      context.Context
	routinesInflight sync.WaitGroup
	consumerCancel   context.CancelFunc
	sleepSecs        int
}

func (r *Consumer) Init() {
	r.consumerCtx, r.consumerCancel = context.WithCancel(context.Background())

	// Set defaults
	if r.PollingInterval == 0 {
		r.PollingInterval = 1
	}

	if r.VisibilityTimeout == 0 {
		r.VisibilityTimeout = 10
	}

	if r.ConcurrentMsgs == 0 {
		r.ConcurrentMsgs = 10
	}

	if r.ExponentialPollingLimit == 0 {
		r.ExponentialPollingLimit = 10
	}

	r.sleepSecs = r.PollingInterval

	// Create queue if not exists
	r.createQueueIfNotExists()

	r.routinesInflight = sync.WaitGroup{}
	r.msgChan = make(chan *PgmqMessage, r.ConcurrentMsgs)
}

func (r *Consumer) createQueueIfNotExists() {
	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.create($1)`, r.QueueName)

	if err != nil {
		panic(fmt.Sprintf("failed to create queue, err: %v", err))
	}
}

func (r *Consumer) ShutdownWithWait() {
	if r.consumerCtx != nil {
		slog.Info("Shutting down consumer...")
		r.consumerCancel()
		slog.Info("Waiting for inflight routines to complete...")
		r.routinesInflight.Wait()
		slog.Info("Consumer shut down complete.")
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
				slog.Debug("Shutting down consumer...")
				return

			default:
				slog.Debug("Fetching for messages...")
				// dont use read_with_poll as it can be taxing on the DB under high load
				// instead poll on client side
				rows, err := r.DbPool.Query(`
					SELECT * FROM pgmq.read(
					  queue_name => $1,
					  vt         => $2,
					  qty        => $3
					);
				`, r.QueueName, r.VisibilityTimeout, 1)

				if err != nil {
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) {
						if pgErr.Code == "57P01" {
							slog.Debug(fmt.Sprintf("Query cancelled, shutting down consumer.. %v", err))
							return // query was cancelled
						}
					}
					panic(fmt.Sprintf("failed to read messages, %v", err))
				}

				msgCount := 0
				msg := &PgmqMessage{}
				// read message
				for rows.Next() {
					err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message, &msg.Headers)
					if err != nil {
						panic(fmt.Sprintf("failed to scan row, err: %v", err))
					}
					r.msgChan <- msg
					msgCount++
				}
				err = rows.Close()
				if err != nil {
					slog.Error(fmt.Sprintf("failed to close rows: %v\n", err.Error()))
				}

				// if no messages found
				if msgCount == 0 {
					// compute seconds to sleep with exponential backoff
					r.sleepSecs = r.sleepSecs + r.ExponentialBackoff

					// ensure we dont exceed cap
					if r.sleepSecs > r.ExponentialPollingLimit {
						r.sleepSecs = r.ExponentialPollingLimit
					}

					// no messages, wait before reading again
					slog.Debug(fmt.Sprintf("No messages found, sleeping for %d seconds...\n", r.sleepSecs))
					time.Sleep(time.Duration(r.sleepSecs) * time.Second)
				} else {
					// reset sleep seconds, when messages are found
					r.sleepSecs = r.PollingInterval
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
		slog.Error(fmt.Sprintf("failed to delete message %d: %v\n", msg.MsgID, err))
	}
}

func (r *Consumer) ArchiveMessage(msg *PgmqMessage) {
	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.archive(
        				queue_name => $1,
        				msg_id     => $2
              		);`, r.QueueName, msg.MsgID)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to archive message %d: %v\n", msg.MsgID, err))
	}
}

func (r *Consumer) PurgeQueue(msg *PgmqMessage) {
	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.purge_queue(
        				queue_name => $1,
              		);`, r.QueueName)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to archive message %d: %v\n", msg.MsgID, err))
	}
}

func (r *Consumer) updateVisibilityTimeout(msg *PgmqMessage) {
	slog.Debug(fmt.Sprintf("Extending visibility timeout for message %d by %d secs\n", msg.MsgID, r.VisibilityTimeout))

	_, err := r.DbPool.Exec(`SELECT * FROM pgmq.update_vt(
						queue_name => $1,
						msg_id     => $2,
						vt         => $3
			  		);`, r.QueueName, msg.MsgID, r.VisibilityTimeout)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to update visibility timeout for message %d: %v\n", msg.MsgID, err))
	}
}
