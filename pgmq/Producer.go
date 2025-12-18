package pgmq

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Producer struct {
	DbPool *sql.DB `required:"true"`
}

func (r *Producer) Init() {
	if r.DbPool == nil {
		panic("DbPool is required")
	}
}

func (r *Producer) Produce(queueName, message, headers string) {
	if headers == "" {
		headers = "{}"
	}

	_, err := r.DbPool.Exec(`SELECT * from pgmq.send(
									  queue_name  => $1,
									  msg         => $2,
									  headers     => $3
									)`, queueName, message, headers)

	if err != nil {
		panic(fmt.Sprintf("failed to send message, %s", err.Error()))
	}
}
