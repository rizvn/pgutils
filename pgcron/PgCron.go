package pgcron

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PgCron struct {
	DbPool *sql.DB `required:"true"`
}

func NewPgCron(cbPool *sql.DB) *PgCron {
	p := &PgCron{
		DbPool: cbPool,
	}
	return p
}

func (s *PgCron) Schedule(jobName string, schedule string, command string) {
	_, err := s.DbPool.Exec(`SELECT cron.schedule($1, $2, $3)`, jobName, schedule, command)

	if err != nil {
		panic("failed to schedule cron job: " + err.Error())
	}
}

func (s *PgCron) Remove(jobName string) {
	_, err := s.DbPool.Exec(`SELECT cron.unschedule($1)`, jobName)

	if err != nil {
		panic("failed to remove cron job: " + err.Error())
	}
}

func (s *PgCron) Pause(jobName string) {

	_, err := s.DbPool.Exec(
		`SELECT cron.alter_job((SELECT jobid FROM cron.job WHERE jobname = $1), active := false)`,
		jobName)
	if err != nil {
		panic("failed to pause cron job: " + err.Error())
	}
}
