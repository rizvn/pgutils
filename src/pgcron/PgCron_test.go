package pgcron_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/pgutils/pgcron"
)

func TestPgCron(t *testing.T) {
	config, err := pgxpool.ParseConfig("postgres://app_admin:app_admin@localhost:5432/app_db")
	if err != nil {
		panic("failed to parse pgx config")
	}
	config.MaxConns = 20 // set your desired max connections
	dbPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic("failed to create pgx pool")
	}

	p := &pgcron.PgCron{}
	p.DbPool = dbPool
	p.Init(dbPool)

	t.Run("Schedule Job", func(t *testing.T) {
		p.Schedule("test_job", "* * * * *",
			`SELECT * from pgmq.send(
			queue_name  => 'test_queue',
			msg         => '{"msg":"hello from cron"}',
			headers     => '{}'
		)`)
	})

	t.Run("Pause Job", func(t *testing.T) {
		p.Pause("test_job")
	})

}
