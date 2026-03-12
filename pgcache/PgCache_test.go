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
		t.Fatal("failed to create pgx pool")
	}
	dbPool.SetMaxOpenConns(20)

	// Create PgCache instance
	// 600 seconds TTL (10 minutes) for testing
	pgCache := pgcache.NewPgCache(dbPool, "c_test", pgcache.WithTTL(600))

	err = pgCache.CreateCacheTable()
	if err != nil {
		t.Fatalf("failed to create cache table: %v", err)
	}

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
		err = pgCache.PutWitTTL(testID, testContent, 1) // 1 second TTL

		if err != nil {
			t.Fatalf("failed to put value with TTL: %v", err)
		}

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
		err = pgCache.Delete(testID)
		if err != nil {
			t.Fatalf("failed to delete key %s: %v", testID, err)
		}
		_, found := pgCache.Get(testID)
		if found {
			t.Errorf("Expected key %s to be deleted", testID)
		}
	})

	t.Run("DeleteExpired", func(t *testing.T) {
		testID := "expired_key"
		testContent := []byte("expired_value")
		err = pgCache.PutWitTTL(testID, testContent, 1)
		if err != nil {
			t.Fatalf("failed to put value with TTL: %v", err)
		}
		time.Sleep(5 * time.Second)
		err = pgCache.DeleteExpired()
		if err != nil {
			t.Fatalf("failed to delete expired keys: %v", err)
		}
		_, found := pgCache.Get(testID)
		if found {
			t.Errorf("Expected expired key %s to be deleted", testID)
		}
	})

	t.Run("ClearCacheStore", func(t *testing.T) {
		pgCache.Put("clear_key", []byte("clear_value"))
		err = pgCache.ClearCacheStore()
		if err != nil {
			t.Fatalf("failed to clear cache store: %v", err)
		}
		_, found := pgCache.Get("clear_key")
		if found {
			t.Errorf("Expected cache store to be cleared")
		}
	})

	t.Run("DropCacheStore", func(t *testing.T) {
		pgCache.Put("drop_key", []byte("drop_value"))
		err = pgCache.DropCacheStore()

		if err != nil {
			t.Fatalf("failed to drop cache store: %v", err)
		}

		err = pgCache.Put("drop_key", []byte("drop_value"))
		if err == nil {
			t.Errorf("Expected error when putting value after dropping cache store, but got none")
		}
	})

}
