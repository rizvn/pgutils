package pgmq

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Producer struct {
	DbPool *pgxpool.Pool
}

func (r *Producer) Init() {
	if r.DbPool == nil {
		panic("DbPool is required")
	}
}

func (r *Producer) Produce(queueName, message, headers string) {
	conn, err := r.DbPool.Acquire(context.Background())
	if err != nil {
		panic("failed to acquire connection")
	}
	defer conn.Release()

	if headers == "" {
		headers = "{}"
	}

	_, err = conn.Exec(context.Background(), `SELECT * from pgmq.send(
									  queue_name  => $1,
									  msg         => $2,
									  headers     => $3
									)`, queueName, message, headers)

	if err != nil {
		panic(fmt.Sprintf("failed to send message, %s", err.Error()))
	}
}
