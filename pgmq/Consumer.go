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

type ConsumerModifier func(*Consumer)

func WithPollingInterval(secs int) ConsumerModifier {
	return func(c *Consumer) {
		c.PollingInterval = secs
	}
}

func WithVisibilityTimeout(secs int) ConsumerModifier {
	return func(c *Consumer) {
		c.VisibilityTimeout = secs
	}
}

func WithConcurrentMsgs(count int) ConsumerModifier {
	return func(c *Consumer) {
		c.ConcurrentMsgs = count
	}
}

func WithExponentialBackoff(secs int) ConsumerModifier {
	return func(c *Consumer) {
		c.ExponentialBackoff = secs
	}
}

func NewConsumer(pool *sql.DB, queueName string, handlerFunc MessageHandlerFunc, mods ...ConsumerModifier) *Consumer {
	c := &Consumer{
		DbPool:                  pool,
		QueueName:               queueName,
		MessageHandler:          handlerFunc,
		PollingInterval:         1,
		VisibilityTimeout:       10,
		ConcurrentMsgs:          10,
		ExponentialPollingLimit: 10,
	}

	// apply options
	for _, opt := range mods {
		opt(c)
	}

	c.consumerCtx, c.consumerCancel = context.WithCancel(context.Background())

	c.sleepSecs = c.PollingInterval

	// Create queue if not exists
	c.createQueueIfNotExists()

	c.routinesInflight = sync.WaitGroup{}
	c.msgChan = make(chan *PgmqMessage, c.ConcurrentMsgs)
	return c
}

func (s *Consumer) createQueueIfNotExists() {
	_, err := s.DbPool.Exec(`SELECT * FROM pgmq.create($1)`, s.QueueName)

	if err != nil {
		panic(fmt.Sprintf("failed to create queue, err: %v", err))
	}
}

func (s *Consumer) ShutdownWithWait() {
	if s.consumerCtx != nil {
		slog.Info("Shutting down consumer...")
		s.consumerCancel()
		slog.Info("Waiting for inflight routines to complete...")
		s.routinesInflight.Wait()
		slog.Info("Consumer shut down complete.")
	}
}

func (s *Consumer) Start() {

	// Start message handler
	go s.handleMessages()

	// Start consumer routine
	go func() {

		for {
			select {

			// check for shutdown
			case <-s.consumerCtx.Done():
				slog.Debug("Shutting down consumer...")
				return

			default:
				slog.Debug("Fetching for messages...")
				// dont use read_with_poll as it can be taxing on the DB under high load
				// instead poll on client side
				rows, err := s.DbPool.Query(`
					SELECT * FROM pgmq.read(
					  queue_name => $1,
					  vt         => $2,
					  qty        => $3
					);
				`, s.QueueName, s.VisibilityTimeout, 1)

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
					s.msgChan <- msg
					msgCount++
				}
				err = rows.Close()
				if err != nil {
					slog.Error(fmt.Sprintf("failed to close rows: %v\n", err.Error()))
				}

				// if no messages found
				if msgCount == 0 {
					// compute seconds to sleep with exponential backoff
					s.sleepSecs = s.sleepSecs + s.ExponentialBackoff

					// ensure we dont exceed cap
					if s.sleepSecs > s.ExponentialPollingLimit {
						s.sleepSecs = s.ExponentialPollingLimit
					}

					// no messages, wait before reading again
					slog.Debug(fmt.Sprintf("No messages found, sleeping for %d seconds...\n", s.sleepSecs))
					time.Sleep(time.Duration(s.sleepSecs) * time.Second)
				} else {
					// reset sleep seconds, when messages are found
					s.sleepSecs = s.PollingInterval
				}

			}
		}
	}()
}

func (s *Consumer) handleMessages() {
	for {
		// wait for message
		msg := <-s.msgChan

		// process message in a new goroutine
		go func() {
			s.routinesInflight.Add(1)
			defer s.routinesInflight.Done()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s.visibilityExtender(ctx, msg)
			s.MessageHandler(context.Background(), msg)
			cancel()

			if s.ArchiveAfterHandle {
				s.ArchiveMessage(msg)
			} else {
				s.DeleteMessage(msg)
			}
		}()
	}
}

// visibilityExtender periodically extends the visibility timeout of a message
// whilst the message is being processed so other processes cannot read it
func (s *Consumer) visibilityExtender(ctx context.Context, msg *PgmqMessage) {
	ticker := time.NewTicker(time.Duration(s.VisibilityTimeout/2) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				s.updateVisibilityTimeout(msg)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *Consumer) DeleteMessage(msg *PgmqMessage) {
	_, err := s.DbPool.Exec(`SELECT * FROM pgmq.delete(
        				queue_name => $1,
        				msg_id     => $2
              		);`, s.QueueName, msg.MsgID)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to delete message %d: %v\n", msg.MsgID, err))
	}
}

func (s *Consumer) ArchiveMessage(msg *PgmqMessage) {
	_, err := s.DbPool.Exec(`SELECT * FROM pgmq.archive(
        				queue_name => $1,
        				msg_id     => $2
              		);`, s.QueueName, msg.MsgID)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to archive message %d: %v\n", msg.MsgID, err))
	}
}

func (s *Consumer) PurgeQueue(msg *PgmqMessage) {
	_, err := s.DbPool.Exec(`SELECT * FROM pgmq.purge_queue(
        				queue_name => $1,
              		);`, s.QueueName)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to archive message %d: %v\n", msg.MsgID, err))
	}
}

func (s *Consumer) updateVisibilityTimeout(msg *PgmqMessage) {
	slog.Debug(fmt.Sprintf("Extending visibility timeout for message %d by %d secs\n", msg.MsgID, s.VisibilityTimeout))

	_, err := s.DbPool.Exec(`SELECT * FROM pgmq.set_vt(
						queue_name => $1,
						msg_id     => $2,
						vt         => $3
			  		);`, s.QueueName, msg.MsgID, s.VisibilityTimeout)

	if err != nil {
		slog.Error(fmt.Sprintf("failed to update visibility timeout for message %d: %v\n", msg.MsgID, err))
	}
}
