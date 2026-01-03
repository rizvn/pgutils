package pglock

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PgLock struct {
	lockName string
	lockId   int64
	conn     *sql.Conn
}

func (r *PgLock) Unlock() {
	// Release the advisory lock
	var released bool
	err := r.conn.QueryRowContext(context.Background(), "SELECT pg_advisory_unlock($1)", r.lockId).Scan(&released)
	if err != nil || !released {
		fmt.Println("Warning: Lock was not released!")
	}

	// Close the dedicated connection
	r.conn.Close()
}
