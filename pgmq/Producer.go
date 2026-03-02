package pgmq

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Producer struct {
	DbPool *sql.DB `required:"true"`
}

func NewProducer(pool *sql.DB) *Producer {
	s := &Producer{
		DbPool: pool,
	}
	return s
}

func (s *Producer) Produce(queueName, message, headers string) {
	if headers == "" {
		headers = "{}"
	}

	_, err := s.DbPool.Exec(`SELECT * from pgmq.send(
									  queue_name  => $1,
									  msg         => $2,
									  headers     => $3
									)`, queueName, message, headers)

	if err != nil {
		panic(fmt.Sprintf("failed to send message, %s", err.Error()))
	}
}
