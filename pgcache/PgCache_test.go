package pgcache_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/rizvn/pgutils/pgcache"
	"github.com/rizvn/pgutils/testutil"
)

func TestPgCache(t *testing.T) {
	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	dbPool, err := sql.Open("pgx", dsn)
	if err != nil {
		panic("failed to create pgx pool")
	}
	dbPool.SetMaxOpenConns(20)

	// Create PgCache instance
	// 600 seconds TTL (10 minutes) for testing
	pgCache := pgcache.NewPgCache(dbPool, "c_test", pgcache.WithTTL(600))

	pgCache.CreateCacheTable()

	t.Run("Put Value", func(t *testing.T) {
		// Test Put
		testID := "test_key"
		testContent := []byte("test_value")
		pgCache.Put(testID, testContent) // 5 minutes TTL
	})

	t.Run("Get Value", func(t *testing.T) {
		// Test Get
		testID := "test_key"
		content, found := pgCache.Get(testID)
		if !found {
			t.Errorf("Expected to find key %s, but it was not found", testID)
		}
		expectedContent := []byte("test_value")
		if string(content) != string(expectedContent) {
			t.Errorf("Expected content %s, got %s", expectedContent, content)
		}
	})

	t.Run("PutWitTTL Value", func(t *testing.T) {
		testID := "test_key_ttl"
		testContent := []byte("ttl_value")
		pgCache.PutWitTTL(testID, testContent, 1) // 1 second TTL
		content, found := pgCache.Get(testID)
		if !found || string(content) != "ttl_value" {
			t.Errorf("Expected to find key %s with correct value", testID)
		}
		time.Sleep(2 * time.Second)
		_, found = pgCache.Get(testID)
		if found {
			t.Errorf("Expected key %s to be expired", testID)
		}
	})

	t.Run("Delete Value", func(t *testing.T) {
		testID := "delete_key"
		testContent := []byte("delete_value")
		pgCache.Put(testID, testContent)
		pgCache.Delete(testID)
		_, found := pgCache.Get(testID)
		if found {
			t.Errorf("Expected key %s to be deleted", testID)
		}
	})

	t.Run("DeleteExpired", func(t *testing.T) {
		testID := "expired_key"
		testContent := []byte("expired_value")
		pgCache.PutWitTTL(testID, testContent, 1)
		time.Sleep(5 * time.Second)
		pgCache.DeleteExpired()
		_, found := pgCache.Get(testID)
		if found {
			t.Errorf("Expected expired key %s to be deleted", testID)
		}
	})

	t.Run("ClearCacheStore", func(t *testing.T) {
		pgCache.Put("clear_key", []byte("clear_value"))
		pgCache.ClearCacheStore()
		_, found := pgCache.Get("clear_key")
		if found {
			t.Errorf("Expected cache store to be cleared")
		}
	})

	t.Run("DropCacheStore", func(t *testing.T) {
		pgCache.Put("drop_key", []byte("drop_value"))
		pgCache.DropCacheStore()
		// Table should not exist, so Put should panic
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic after dropping cache table")
			}
		}()
		pgCache.Put("drop_key", []byte("drop_value"))
	})

}
