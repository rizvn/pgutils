package pgcache

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rizvn/pgutils/util"
)

type PgCache struct {
	CacheName string  `required:"true"`
	DbPool    *sql.DB `required:"true"`
	TTL       int

	//internal use
	cacheTable string
}

func (r *PgCache) Init() {
	if r.DbPool == nil {
		panic("DbPool is required")
	}

	if r.CacheName == "" {
		panic("CacheName is required")
	}

	if r.TTL == 0 {
		r.TTL = 3600 // Default TTL of 1 hour
	}

	r.cacheTable = "c_" + r.CacheName

	r.createCacheTable()
}

func (r *PgCache) createCacheTable() {
	conn := util.GetDbConnection(r.DbPool)
	defer util.CloseDbConnection(conn)

	query := `
	CREATE UNLOGGED TABLE IF NOT EXISTS ` + r.cacheTable + ` (
		id TEXT PRIMARY KEY,
        content BYTEA NOT NULL,
		created_on TIMESTAMPTZ NOT NULL,
		expires_on TIMESTAMPTZ NOT NULL
	)`
	_, err := conn.ExecContext(context.Background(), query)
	if err != nil {
		panic("failed to create cache store: " + err.Error())
	}
}

func (r *PgCache) Put(id string, content []byte) {
	r.PutWitTTL(id, content, r.TTL)
}

func (r *PgCache) PutWitTTL(id string, content []byte, ttl int) {
	conn := util.GetDbConnection(r.DbPool)
	defer util.CloseDbConnection(conn)

	// Upsert value with expiration
	query :=
		`INSERT INTO ` + r.cacheTable + ` (id, content, created_on, expires_on)
		 VALUES ($1, $2, NOW(), NOW() + INTERVAL '` + strconv.Itoa(ttl) + ` seconds')
	     ON CONFLICT (id) 
         DO UPDATE 
           SET content = EXCLUDED.content, 
               created_on = EXCLUDED.created_on, 
               expires_on = EXCLUDED.expires_on`
	_, err := conn.ExecContext(context.Background(), query, id, content)
	if err != nil {
		panic("failed to cache value: " + err.Error())
	}
}

func (r *PgCache) Get(id string) ([]byte, bool) {
	conn := util.GetDbConnection(r.DbPool)
	defer util.CloseDbConnection(conn)

	var content []byte
	query := `SELECT content FROM ` + r.cacheTable + ` WHERE id = $1 AND expires_on > NOW()`
	err := conn.QueryRowContext(context.Background(), query, id).Scan(&content)
	if err != nil {
		return nil, false
	}
	return content, true
}

func (r *PgCache) Delete(id string) {
	conn := util.GetDbConnection(r.DbPool)
	defer util.CloseDbConnection(conn)

	query := `DELETE FROM ` + r.cacheTable + ` WHERE id = $1`
	_, err := conn.ExecContext(context.Background(), query, id)
	if err != nil {
		panic("failed to delete cache entry: " + err.Error())
	}
}

func (r *PgCache) DeleteExpired() {
	conn := util.GetDbConnection(r.DbPool)
	defer util.CloseDbConnection(conn)

	query := `DELETE FROM ` + r.cacheTable + ` WHERE expires_on <= NOW()`
	_, err := conn.ExecContext(context.Background(), query)
	if err != nil {
		panic("failed to delete expired cache entries: " + err.Error())
	}
}

func (r *PgCache) CleanUp(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			sleepDuration := r.TTL / 2
			time.Sleep(time.Duration(sleepDuration) * time.Second)
			r.DeleteExpired()
		}
	}
}

func (r *PgCache) ClearCacheStore() {
	conn := util.GetDbConnection(r.DbPool)
	defer util.CloseDbConnection(conn)

	query := `TRUNCATE TABLE ` + r.cacheTable
	_, err := conn.ExecContext(context.Background(), query)
	if err != nil {
		panic("failed to clear cache store: " + err.Error())
	}
}

func (r *PgCache) DropCacheStore() {
	conn := util.GetDbConnection(r.DbPool)
	defer util.CloseDbConnection(conn)

	query := `DROP TABLE IF EXISTS ` + r.cacheTable
	_, err := conn.ExecContext(context.Background(), query)
	if err != nil {
		panic("failed to drop cache store: " + err.Error())
	}
}
