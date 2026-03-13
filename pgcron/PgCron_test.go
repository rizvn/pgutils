package pgcron_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/rizvn/pgutils/pgcron"
	"github.com/rizvn/pgutils/testutil"
)

func TestPgCron(t *testing.T) {
	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	// Create db pool
	dbPool, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(fmt.Sprintf("failed to create pgx pool, err: %v", err))
	}
	dbPool.SetMaxOpenConns(20)

	// Create pgcron instance
	p := pgcron.NewPgCron(dbPool)

	// Test scheduling a job
	t.Run("Schedule Job", func(t *testing.T) {

		initTestQueue(dbPool, t)

		// Act - Schedule job to produce message every minute
		log.Print("Scheduling test_job to run every minute")
		err = p.Schedule("test_job", "* * * * *",
			`SELECT * from pgmq.send(
			queue_name  => 'test_queue',
			msg         => '{"msg":"hello from cron"}',
			headers     => '{}'
		)`)
		if err != nil {
			t.Fatalf("failed to schedule cron job: %v", err)
		}

		//wait just over a minute to allow job to run
		log.Print("Waiting for 70 seconds to allow job to run")
		time.Sleep(70 * time.Second)

		// check if message was produced in pgmq table

		//get db connection

		log.Print("Querying pgmq.q_test_queue table for messages")
		rows, err := dbPool.Query(`SELECT count(0) FROM pgmq.q_test_queue`)

		if err != nil {
			t.Fatalf("failed to query pgmq table: %v", err)
		}

		// Assert - verify at least 1 message was produced
		var count int
		for rows.Next() {
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("failed to scan count: %v", err)
			}
		}
		if count == 0 {
			t.Fatalf("expected at least 1 message in pgmq table, got 0")
		}
	})

	t.Run("Pause Job", func(t *testing.T) {
		initTestQueue(dbPool, t)

		log.Print("Scheduling test_job to run every minute")
		err = p.Schedule("test_job", "* * * * *",
			`SELECT * from pgmq.send(
			queue_name  => 'test_queue',
			msg         => '{"msg":"hello from cron"}',
			headers     => '{}'
		)`)

		if err != nil {
			t.Fatalf("failed to schedule cron job: %v", err)
		}

		err = p.Pause("test_job")
		if err != nil {
			t.Fatalf("failed to pause cron job: %v", err)
		}

		log.Print("Waiting for 70 seconds to allow job to run")
		time.Sleep(70 * time.Second)

		log.Print("Querying pgmq.q_test_queue table for messages")
		rows, err := dbPool.Query(`SELECT count(0) FROM pgmq.q_test_queue`)

		if err != nil {
			t.Fatalf("failed to query pgmq table: %v", err)
		}
		var count int
		for rows.Next() {
			err = rows.Scan(&count)
			if err != nil {
				t.Fatalf("failed to scan count: %v", err)
			}
		}
		if count != 0 {
			t.Fatalf("expected at least 0 message in pgmq table, got some")
		}
	})

}

func initTestQueue(dbPool *sql.DB, t *testing.T) {
	// Create pgmq queue to use in the test
	_, err := dbPool.Exec(`SELECT *  FROM pgmq.create('test_queue')`)
	if err != nil {
		t.Fatalf("failed to create pgmq queue: %v", err)
	}

	// Clean up pgmq queue so its empty
	_, err = dbPool.ExecContext(context.Background(), `SELECT * FROM pgmq.purge_queue('test_queue')`)
	if err != nil {
		t.Fatalf("failed to truncate pgmq queue: %v", err)
	}
}
