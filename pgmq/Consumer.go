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

func (this *Consumer) Init() {
	this.consumerCtx, this.consumerCancel = context.WithCancel(context.Background())

	// Set defaults
	if this.PollingInterval == 0 {
		this.PollingInterval = 1
	}

	if this.VisibilityTimeout == 0 {
		this.VisibilityTimeout = 10
	}

	if this.ConcurrentMsgs == 0 {
		this.ConcurrentMsgs = 10
	}

	if this.ExponentialPollingLimit == 0 {
		this.ExponentialPollingLimit = 10
	}

	this.sleepSecs = this.PollingInterval

	// Create queue if not exists
	this.createQueueIfNotExists()

	this.routinesInflight = sync.WaitGroup{}
	this.msgChan = make(chan *PgmqMessage, this.ConcurrentMsgs)
}

func (this *Consumer) createQueueIfNotExists() {
	_, err := this.DbPool.Exec(`SELECT * FROM pgmq.create($1)`, this.QueueName)

	if err != nil {
		panic(fmt.Sprintf("failed to create queue, err: %v", err))
	}
}

func (this *Consumer) ShutdownWithWait() {
	if this.consumerCtx != nil {
		slog.Info("Shutting down consumer...")
		this.consumerCancel()
		slog.Info("Waiting for inflight routines to complete...")
		this.routinesInflight.Wait()
		slog.Info("Consumer shut down complete.")
	}
}

func (this *Consumer) Start() {

	// Start message handler
	go this.handleMessages()

	// Start consumer routine
	go func() {

		for {
			select {

			// check for shutdown
			case <-this.consumerCtx.Done():
				slog.Debug("Shutting down consumer...")
				return

			default:
				slog.Debug("Fetching for messages...")
				// dont use read_with_poll as it can be taxing on the DB under high load
				// instead poll on client side
				rows, err := this.DbPool.Query(`
					SELECT * FROM pgmq.read(
					  queue_name => $1,
					  vt         => $2,
					  qty        => $3
					);
				`, this.QueueName, this.VisibilityTimeout, 1)

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
					this.msgChan <- msg
					msgCount++
				}
				err = rows.Close()
				if err != nil {
					slog.Error(fmt.Sprintf("failed to close rows: %v\n", err.Error()))
				}

				// if no messages found
				if msgCount == 0 {
					// compute seconds to sleep with exponential backoff
					this.sleepSecs = this.sleepSecs + this.ExponentialBackoff

					// ensure we dont exceed cap
					if this.sleepSecs > this.ExponentialPollingLimit {
						this.sleepSecs = this.ExponentialPollingLimit
					}

					// no messages, wait before reading again
					slog.Debug(fmt.Sprintf("No messages found, sleeping for %d seconds...\n", this.sleepSecs))
					time.Sleep(time.Duration(this.sleepSecs) * time.Second)
				} else {
					// reset sleep seconds, when messages are found
					this.sleepSecs = this.PollingInterval
				}

			}
		}
	}()
}

func (this *Consumer) handleMessages() {
	for {
		// wait for message
		msg := <-this.msgChan

		// process message in a new goroutine
		go func() {
			this.routinesInflight.Add(1)
			defer this.routinesInflight.Done()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			this.visibilityExtender(ctx, msg)
			this.MessageHandler(context.Background(), msg)
			cancel()

			if this.ArchiveAfterHandle {
				this.ArchiveMessage(msg)
			} else {
				this.DeleteMessage(msg)
			}
		}()
	}
}

// visibilityExtender periodically extends the visibility timeout of a message
// whilst the message is being processed so other processes cannot read it
func (this *Consumer) visibilityExtender(ctx context.Context, msg *PgmqMessage) {
	ticker := time.NewTicker(time.Duration(this.VisibilityTimeout/2) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				this.updateVisibilityTimeout(msg)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (this *Consumer) DeleteMessage(msg *PgmqMessage) {
	_, err := this.DbPool.Exec(`SELECT * FROM pgmq.delete(
        				queue_name => $1,
        				msg_id     => $2
              		);`, this.QueueName, msg.MsgID)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to delete message %d: %v\n", msg.MsgID, err))
	}
}

func (this *Consumer) ArchiveMessage(msg *PgmqMessage) {
	_, err := this.DbPool.Exec(`SELECT * FROM pgmq.archive(
        				queue_name => $1,
        				msg_id     => $2
              		);`, this.QueueName, msg.MsgID)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to archive message %d: %v\n", msg.MsgID, err))
	}
}

func (this *Consumer) PurgeQueue(msg *PgmqMessage) {
	_, err := this.DbPool.Exec(`SELECT * FROM pgmq.purge_queue(
        				queue_name => $1,
              		);`, this.QueueName)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to archive message %d: %v\n", msg.MsgID, err))
	}
}

func (this *Consumer) updateVisibilityTimeout(msg *PgmqMessage) {
	slog.Debug(fmt.Sprintf("Extending visibility timeout for message %d by %d secs\n", msg.MsgID, this.VisibilityTimeout))

	_, err := this.DbPool.Exec(`SELECT * FROM pgmq.set_vt(
						queue_name => $1,
						msg_id     => $2,
						vt         => $3
			  		);`, this.QueueName, msg.MsgID, this.VisibilityTimeout)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to update visibility timeout for message %d: %v\n", msg.MsgID, err))
	}
}
