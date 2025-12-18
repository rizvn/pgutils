package pgcron_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/pgutils/pgcron"
	"github.com/rizvn/pgutils/testutil"
)

func TestPgCron(t *testing.T) {
	// Start Postgres test container
	ctr, dsn := testutil.StartPgTestContainer()
	defer func() { _ = ctr.Terminate(context.Background()) }()

	// Create pgx pool
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		panic("failed to parse pgx config")
	}
	config.MaxConns = 20 // set your desired max connections
	dbPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic("failed to create pgx pool")
	}

	// Create pgcron instance
	p := &pgcron.PgCron{}
	p.DbPool = dbPool
	p.Init()

	// Test scheduling a job
	t.Run("Schedule Job", func(t *testing.T) {

		initTestQueue(dbPool, t)

		// Act - Schedule job to produce message every minute
		log.Print("Scheduling test_job to run every minute")
		p.Schedule("test_job", "* * * * *",
			`SELECT * from pgmq.send(
			queue_name  => 'test_queue',
			msg         => '{"msg":"hello from cron"}',
			headers     => '{}'
		)`)

		//wait just over a minute to allow job to run
		log.Print("Waiting for 70 seconds to allow job to run")
		time.Sleep(70 * time.Second)

		// check if message was produced in pgmq table

		//get db connection
		conn, err := dbPool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("failed to acquire connection: %v", err)
		}
		defer conn.Release()

		log.Print("Querying pgmq.q_test_queue table for messages")
		rows, err := conn.Query(context.Background(), `SELECT count(0) FROM pgmq.q_test_queue`)
		defer rows.Close()

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
		p.Schedule("test_job", "* * * * *",
			`SELECT * from pgmq.send(
			queue_name  => 'test_queue',
			msg         => '{"msg":"hello from cron"}',
			headers     => '{}'
		)`)

		p.Pause("test_job")

		log.Print("Waiting for 70 seconds to allow job to run")
		time.Sleep(70 * time.Second)

		// check if message was produced in pgmq table
		conn, err := dbPool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("failed to acquire connection: %v", err)
		}
		defer conn.Release()

		log.Print("Querying pgmq.q_test_queue table for messages")
		rows, err := conn.Query(context.Background(), `SELECT count(0) FROM pgmq.q_test_queue`)
		defer rows.Close()

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

func initTestQueue(dbPool *pgxpool.Pool, t *testing.T) {

	//get db connection
	conn, err := dbPool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	// Create pgmq queue to use in the test
	_, err = conn.Exec(context.Background(), `SELECT *  FROM pgmq.create('test_queue')`)
	if err != nil {
		t.Fatalf("failed to create pgmq queue: %v", err)
	}

	// Clean up pgmq queue so its empty
	_, err = conn.Exec(context.Background(), `SELECT * FROM pgmq.purge_queue('test_queue')`)
	if err != nil {
		t.Fatalf("failed to truncate pgmq queue: %v", err)
	}
}
