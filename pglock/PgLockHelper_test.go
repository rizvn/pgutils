package pglock_test

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/rizvn/pgutils/pglock"
	"github.com/rizvn/pgutils/testutil"
)

func TestPgLock(t *testing.T) {
	// Start Postgres test container
	ctr, dsn, err := testutil.StartPgTestContainer()

	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = ctr.Terminate(context.Background()) }()

	dbPool, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to Postgres: %v", err)
	}
	dbPool.SetMaxOpenConns(20)

	// Create PgLockHelper instance
	pgLockHelper := pglock.NewPgLockHelper(dbPool)

	t.Run("Lock and unlock", func(t *testing.T) {
		lock, err := pgLockHelper.Lock("test-lock")
		if err != nil {
			t.Fatal(err)
		}
		lock.Unlock()
	})

	t.Run("Verify no race condition", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(3)
		routineId := 0

		// Test Put
		go func() {
			id := 1
			fmt.Println("Goroutine 1 trying to acquire lock")
			lock, err := pgLockHelper.Lock("test-lock")
			if err != nil {
				slog.Error("", "error", err)
				return
			}

			routineId = id

			fmt.Println("Acquired lock 1")
			time.Sleep(1 * time.Second)

			if routineId != id {
				t.Errorf("expected to be routineId %d but got %d", id, routineId)
			}
			time.Sleep(1 * time.Second)

			if routineId != id {
				t.Errorf("expected to be routineId %d but got %d", id, routineId)
			}

			lock.Unlock()
			fmt.Println("Released lock 1")

			wg.Done()
		}()

		go func() {
			id := 2
			fmt.Println("Goroutine 2 trying to acquire lock")
			lock, err := pgLockHelper.Lock("test-lock")
			if err != nil {
				slog.Error("", "error", err)
				return
			}
			routineId = id

			fmt.Println("Acquired lock 2")
			time.Sleep(1 * time.Second)

			if routineId != id {
				t.Errorf("expected to be routineId %d but got %d", id, routineId)
			}
			time.Sleep(1 * time.Second)

			if routineId != id {
				t.Errorf("expected to be routineId %d but got %d", id, routineId)
			}

			lock.Unlock()
			fmt.Println("Released lock 2")
			wg.Done()
		}()

		go func() {
			id := 3
			fmt.Println("Goroutine 3 trying to acquire lock")
			lock, err := pgLockHelper.Lock("test-lock")
			if err != nil {
				slog.Error("", "error", err)
				return
			}

			routineId = id

			fmt.Println("Acquired lock 3")
			time.Sleep(1 * time.Second)

			if routineId != id {
				t.Errorf("expected to be routineId %d but got %d", id, routineId)
			}
			time.Sleep(1 * time.Second)

			if routineId != id {
				t.Errorf("expected to be routineId %d but got %d", id, routineId)
			}

			lock.Unlock()
			fmt.Println("Released lock 3")
			wg.Done()
		}()

		wg.Wait()

		//pgLockHelper.Unlock("test-lock")
	})

}
