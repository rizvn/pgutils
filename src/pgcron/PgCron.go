package pgcron

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PgCron struct {
	DbPool *pgxpool.Pool
}

func (r *PgCron) Init(dbPool *pgxpool.Pool) {
	if dbPool == nil {
		panic("DbPool is required")
	}
}

func (r *PgCron) getConnection() *pgxpool.Conn {
	conn, err := r.DbPool.Acquire(context.Background())
	if err != nil {
		panic("failed to acquire connection")
	}
	return conn
}

func (r *PgCron) Schedule(jobName string, schedule string, command string) {
	conn := r.getConnection()
	defer conn.Release()

	_, err := conn.Exec(context.Background(),
		`SELECT cron.schedule($1, $2, $3)`,
		jobName, schedule, command)

	if err != nil {
		panic("failed to schedule cron job: " + err.Error())
	}
}

func (r *PgCron) Remove(jobName string) {
	conn := r.getConnection()
	defer conn.Release()

	_, err := conn.Exec(context.Background(),
		`SELECT cron.unschedule($1)`,
		jobName)

	if err != nil {
		panic("failed to remove cron job: " + err.Error())
	}
}

func (r *PgCron) Pause(jobName string) {
	conn := r.getConnection()
	defer conn.Release()

	_, err := conn.Exec(context.Background(),
		`SELECT cron.alter_job((SELECT jobid FROM cron.job WHERE jobname = $1), active := false)`,
		jobName)
	if err != nil {
		panic("failed to pause cron job: " + err.Error())
	}
}
