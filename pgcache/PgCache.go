package pgcache

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PgCache struct {
	DbPool *sql.DB `required:"true"`
	TTL    int

	CacheTable       string `required:"true"`
	CreateCacheTable bool   `default:"false"`
	StartCleaner     bool   `default:"true"`
	cancelCleanerCtx context.CancelFunc
}

func (r *PgCache) Init() {
	r.StartCleaner = true

	if r.DbPool == nil {
		panic("DbPool is required")
	}

	if r.CacheTable == "" {
		panic("CacheTable is required")
	}

	if r.TTL == 0 {
		r.TTL = 3600 // Default TTL of 1 hour
	}

	if r.CreateCacheTable {
		r.createCacheTable()
	}

	if r.StartCleaner {
		ctx, cancel := context.WithCancel(context.Background())
		go r.CleanUp(ctx)
		r.cancelCleanerCtx = cancel
	}

}

func (r *PgCache) createCacheTable() {

	query := `
	CREATE UNLOGGED TABLE IF NOT EXISTS ` + r.CacheTable + ` (
		id TEXT PRIMARY KEY,
        content BYTEA NOT NULL,
		created_on TIMESTAMPTZ NOT NULL,
		expires_on TIMESTAMPTZ NOT NULL
	)`
	_, err := r.DbPool.Exec(query)
	if err != nil {
		panic("failed to create cache store: " + err.Error())
	}
}

func (r *PgCache) Put(id string, content []byte) {
	r.PutWitTTL(id, content, r.TTL)
}

func (r *PgCache) PutWitTTL(id string, content []byte, ttl int) {

	// Upsert value with expiration
	query :=
		`INSERT INTO ` + r.CacheTable + ` (id, content, created_on, expires_on)
		 VALUES ($1, $2, NOW(), NOW() + INTERVAL '` + strconv.Itoa(ttl) + ` seconds')
	     ON CONFLICT (id) 
         DO UPDATE 
           SET content = EXCLUDED.content, 
               created_on = EXCLUDED.created_on, 
               expires_on = EXCLUDED.expires_on`
	_, err := r.DbPool.Exec(query, id, content)
	if err != nil {
		panic("failed to cache value: " + err.Error())
	}
}

func (r *PgCache) Get(id string) ([]byte, bool) {

	var content []byte
	query := `SELECT content FROM ` + r.CacheTable + ` WHERE id = $1 AND expires_on > NOW()`
	err := r.DbPool.QueryRow(query, id).Scan(&content)
	if err != nil {
		return nil, false
	}
	return content, true
}

func (r *PgCache) Delete(id string) {

	query := `DELETE FROM ` + r.CacheTable + ` WHERE id = $1`
	_, err := r.DbPool.Exec(query, id)
	if err != nil {
		panic("failed to delete cache entry: " + err.Error())
	}
}

func (r *PgCache) DeleteExpired() {
	query := `DELETE FROM ` + r.CacheTable + ` WHERE expires_on <= NOW()`
	_, err := r.DbPool.Exec(query)
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

func (r *PgCache) StopCleaner() {
	r.cancelCleanerCtx()
}

func (r *PgCache) ClearCacheStore() {
	query := `TRUNCATE TABLE ` + r.CacheTable
	_, err := r.DbPool.Exec(query)
	if err != nil {
		panic("failed to clear cache store: " + err.Error())
	}
}

func (r *PgCache) DropCacheStore() {
	query := `DROP TABLE IF EXISTS ` + r.CacheTable
	_, err := r.DbPool.Exec(query)
	if err != nil {
		panic("failed to drop cache store: " + err.Error())
	}
}
