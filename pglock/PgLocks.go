package pglock

import (
	"context"
	"database/sql"
	"hash/fnv"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PgLocks struct {
	DbPool *sql.DB `required:"true"`
}

// hash string to int64 using FNV-1a
func hashLock(lockName string) int64 {
	h := fnv.New64a()
	h.Write([]byte(lockName))
	return int64(h.Sum64())
}

func (r *PgLocks) Lock(lockName string) *PgLock {
	// Get a dedicated connection for this lock
	conn, err := r.DbPool.Conn(context.Background())
	if err != nil {
		panic("failed to get dedicated connection for lock: " + err.Error())
	}

	// Acquire the advisory lock or wait until it's available
	_, err = conn.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", hashLock(lockName))

	pgLock := &PgLock{
		lockName: lockName,
		lockId:   hashLock(lockName),
		conn:     conn,
	}
	return pgLock
}
