package pgcache

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rizvn/pgutil/common"
)

type PgCache struct {
	DbPool *sql.DB `required:"true"`
	TTL    int

	CacheTable       string `required:"true"`
	CleanerInterval  int    `default:"60"` // in seconds
	cancelCleanerCtx context.CancelFunc
}

// CacheMod modifier functions for optional fields
type CacheMod func(*PgCache)

// WithTTL sets the TTL (time-to-live) for cache entries in seconds. Default is 86400 seconds (1 day).
func WithTTL(ttl int) CacheMod {
	return func(cache *PgCache) {
		cache.TTL = ttl
	}
}

func NewPgCache(dbPool *sql.DB, cacheTable string, modifiers ...CacheMod) *PgCache {
	p := &PgCache{
		// Default TTL of 1 day
		TTL: 86400,
	}
	p.DbPool = dbPool
	p.CacheTable = cacheTable

	for _, mod := range modifiers {
		mod(p)
	}

	return p
}

func (s *PgCache) CreateCacheTable() error {
	query := `
	CREATE UNLOGGED TABLE IF NOT EXISTS ` + s.CacheTable + ` (
		id TEXT PRIMARY KEY,
        content BYTEA NOT NULL,
		created_on TIMESTAMPTZ NOT NULL,
		expires_on TIMESTAMPTZ NOT NULL
	)`
	_, err := s.DbPool.Exec(query)
	if err != nil {
		return common.NewErr("failed to create cache store", err)
	}
	return nil
}

func (s *PgCache) Put(id string, content []byte) error {
	return s.PutWitTTL(id, content, s.TTL)
}

func (s *PgCache) PutWitTTL(id string, content []byte, ttl int) error {

	// Upsert value with expiration
	query :=
		`INSERT INTO ` + s.CacheTable + ` (id, content, created_on, expires_on)
		 VALUES ($1, $2, NOW(), NOW() + INTERVAL '` + strconv.Itoa(ttl) + ` seconds')
	     ON CONFLICT (id) 
         DO UPDATE 
           SET content = EXCLUDED.content, 
               created_on = EXCLUDED.created_on, 
               expires_on = EXCLUDED.expires_on`
	_, err := s.DbPool.Exec(query, id, content)
	if err != nil {
		return common.NewErr("failed to cache value", err)
	}
	return nil
}

func (s *PgCache) Get(id string) ([]byte, bool) {

	var content []byte
	query := `SELECT content FROM ` + s.CacheTable + ` WHERE id = $1 AND expires_on > NOW()`
	err := s.DbPool.QueryRow(query, id).Scan(&content)
	if err != nil {
		return nil, false
	}
	return content, true
}

func (s *PgCache) Delete(id string) error {
	query := `DELETE FROM ` + s.CacheTable + ` WHERE id = $1`
	_, err := s.DbPool.Exec(query, id)
	if err != nil {
		return common.NewErr("failed to delete cache entry", err)
	}
	return nil
}

func (s *PgCache) DeleteExpired() error {
	query := `DELETE FROM ` + s.CacheTable + ` WHERE expires_on <= NOW()`
	_, err := s.DbPool.Exec(query)
	if err != nil {
		return common.NewErr("failed to delete expired cache entries", err)
	}
	return nil
}

func (s *PgCache) CleanUp(ctx context.Context, interval int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Duration(interval) * time.Second)
			err := s.DeleteExpired()
			if err != nil {
				slog.Error("cache cleanup error", "error", err)
			}
		}
	}
}

func (s *PgCache) StartCleaner(interval int) {
	ctx, cancel := context.WithCancel(context.Background())
	go s.CleanUp(ctx, interval)
	s.cancelCleanerCtx = cancel
}

func (s *PgCache) StopCleaner() {
	s.cancelCleanerCtx()
}

func (s *PgCache) ClearCacheStore() error {
	query := `TRUNCATE TABLE ` + s.CacheTable
	_, err := s.DbPool.Exec(query)
	if err != nil {
		return common.NewErr("failed to clear cache store", err)
	}
	return nil
}

func (s *PgCache) DropCacheStore() error {
	query := `DROP TABLE IF EXISTS ` + s.CacheTable
	_, err := s.DbPool.Exec(query)
	if err != nil {
		return common.NewErr("failed to drop cache store", err)
	}
	return nil
}
