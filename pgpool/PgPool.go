package pgpool

import (
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PgPool struct {

	// optionally, a database connection pool can be set directly
	DbPool *sql.DB

	// or the following parameters can be used to initialize the pool
	DbHost      string
	DbPort      string
	DbUser      string
	DbPass      string
	DBName      string
	DBUrlParams string

	// or set dsn
	DSN string
}

func (r *PgPool) Init() error {
	if r.DSN == "" {
		r.DSN = fmt.Sprintf("postgres://%s:%s@%s:%s/%s%s", r.DbUser, r.DbPass, r.DbHost, r.DbPort, r.DBName, r.DBUrlParams)
	}
	var err error
	r.DbPool, err = sql.Open("pgx", r.DSN)

	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}

	r.DbPool.SetMaxOpenConns(10)
	slog.Info("queue connection pool connected")
	return nil
}
