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
	CleanerInterval  int    `default:"60"` // in seconds
	cancelCleanerCtx context.CancelFunc
}

func (this *PgCache) Init() {

	if this.DbPool == nil {
		panic("DbPool is required")
	}

	if this.CacheTable == "" {
		panic("CacheTable is required")
	}

	if this.TTL == 0 {
		this.TTL = 86400 // Default TTL of 1 day
	}

}

func (this *PgCache) CreateCacheTable() {

	query := `
	CREATE UNLOGGED TABLE IF NOT EXISTS ` + this.CacheTable + ` (
		id TEXT PRIMARY KEY,
        content BYTEA NOT NULL,
		created_on TIMESTAMPTZ NOT NULL,
		expires_on TIMESTAMPTZ NOT NULL
	)`
	_, err := this.DbPool.Exec(query)
	if err != nil {
		panic("failed to create cache store: " + err.Error())
	}
}

func (this *PgCache) Put(id string, content []byte) {
	this.PutWitTTL(id, content, this.TTL)
}

func (this *PgCache) PutWitTTL(id string, content []byte, ttl int) {

	// Upsert value with expiration
	query :=
		`INSERT INTO ` + this.CacheTable + ` (id, content, created_on, expires_on)
		 VALUES ($1, $2, NOW(), NOW() + INTERVAL '` + strconv.Itoa(ttl) + ` seconds')
	     ON CONFLICT (id) 
         DO UPDATE 
           SET content = EXCLUDED.content, 
               created_on = EXCLUDED.created_on, 
               expires_on = EXCLUDED.expires_on`
	_, err := this.DbPool.Exec(query, id, content)
	if err != nil {
		panic("failed to cache value: " + err.Error())
	}
}

func (this *PgCache) Get(id string) ([]byte, bool) {

	var content []byte
	query := `SELECT content FROM ` + this.CacheTable + ` WHERE id = $1 AND expires_on > NOW()`
	err := this.DbPool.QueryRow(query, id).Scan(&content)
	if err != nil {
		return nil, false
	}
	return content, true
}

func (this *PgCache) Delete(id string) {

	query := `DELETE FROM ` + this.CacheTable + ` WHERE id = $1`
	_, err := this.DbPool.Exec(query, id)
	if err != nil {
		panic("failed to delete cache entry: " + err.Error())
	}
}

func (this *PgCache) DeleteExpired() {
	query := `DELETE FROM ` + this.CacheTable + ` WHERE expires_on <= NOW()`
	_, err := this.DbPool.Exec(query)
	if err != nil {
		panic("failed to delete expired cache entries: " + err.Error())
	}
}

func (this *PgCache) CleanUp(ctx context.Context, interval int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Duration(interval) * time.Second)
			this.DeleteExpired()
		}
	}
}

func (this *PgCache) StartCleaner(interval int) {
	ctx, cancel := context.WithCancel(context.Background())
	go this.CleanUp(ctx, interval)
	this.cancelCleanerCtx = cancel
}

func (this *PgCache) StopCleaner() {
	this.cancelCleanerCtx()
}

func (this *PgCache) ClearCacheStore() {
	query := `TRUNCATE TABLE ` + this.CacheTable
	_, err := this.DbPool.Exec(query)
	if err != nil {
		panic("failed to clear cache store: " + err.Error())
	}
}

func (this *PgCache) DropCacheStore() {
	query := `DROP TABLE IF EXISTS ` + this.CacheTable
	_, err := this.DbPool.Exec(query)
	if err != nil {
		panic("failed to drop cache store: " + err.Error())
	}
}
