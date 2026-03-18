package pgpool

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rizvn/pgutil/common"
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

func (s *PgPool) Connect() error {
	rows, err := s.DbPool.QueryContext(context.Background(), "SELECT 1")
	defer func() { _ = rows.Close() }()
	if err != nil {
		return common.NewErr("failed to connect to database", err)
	}
	return nil
}

func NewPgPool(dbHost, dbPort, dbUser, dbPass, dbName, dbUrlParams string) (*PgPool, error) {
	p := &PgPool{}
	p.DbHost = dbHost
	p.DbPort = dbPort
	p.DbUser = dbUser
	p.DBName = dbName
	p.DbPass = dbPass
	p.DBUrlParams = dbUrlParams

	if p.DSN == "" {
		p.DSN = fmt.Sprintf("postgres://%s:%s@%s:%s/%s%s", p.DbUser, p.DbPass, p.DbHost, p.DbPort, p.DBName, p.DBUrlParams)
	}
	var err error
	p.DbPool, err = sql.Open("pgx", p.DSN)

	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %v", err)
	}

	p.DbPool.SetMaxOpenConns(10)

	slog.Info("queue connection pool connected")
	return p, nil
}
