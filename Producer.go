package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Producer struct {
	dbPool *pgxpool.Pool
}

func (r *Producer) Init(dbPool *pgxpool.Pool) {
	r.dbPool = dbPool
}

func (r *Producer) Produce(queueName, message, headers string) {
	conn, err := r.dbPool.Acquire(context.Background())
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
		panic("failed to send message")
	}
}
