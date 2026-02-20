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

func (this *PgPool) Init() error {
	if this.DSN == "" {
		this.DSN = fmt.Sprintf("postgres://%s:%s@%s:%s/%s%s", this.DbUser, this.DbPass, this.DbHost, this.DbPort, this.DBName, this.DBUrlParams)
	}
	var err error
	this.DbPool, err = sql.Open("pgx", this.DSN)

	if err != nil {
		return fmt.Errorf("failed to open database connection: %v", err)
	}

	this.DbPool.SetMaxOpenConns(10)

	slog.Info("queue connection pool connected")
	return nil
}
