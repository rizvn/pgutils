package pgmq

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rizvn/pgutils/common"
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

func (s *Producer) Produce(queueName, message, headers string) error {
	if headers == "" {
		headers = "{}"
	}

	_, err := s.DbPool.Exec(`SELECT * from pgmq.send(
									  queue_name  => $1,
									  msg         => $2,
									  headers     => $3
									)`, queueName, message, headers)

	if err != nil {
		return common.NewErr("Failed to send message", err)
	}

	return nil
}
