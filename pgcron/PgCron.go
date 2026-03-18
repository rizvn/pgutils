package pgcron

import (
	"database/sql"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rizvn/pgutil/common"
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

func (s *PgCron) Schedule(jobName string, schedule string, command string) error {
	_, err := s.DbPool.Exec(`SELECT cron.schedule($1, $2, $3)`, jobName, schedule, command)

	if err != nil {
		return common.NewErr("failed to schedule cron job", err)
	}
	return nil
}

func (s *PgCron) Remove(jobName string) error {
	_, err := s.DbPool.Exec(`SELECT cron.unschedule($1)`, jobName)

	if err != nil {
		return common.NewErr("failed to remove cron job", err)
	}
	return nil
}

func (s *PgCron) Pause(jobName string) error {

	_, err := s.DbPool.Exec(
		`SELECT cron.alter_job((SELECT jobid FROM cron.job WHERE jobname = $1), active := false)`,
		jobName)
	if err != nil {
		return common.NewErr("failed to pause cron job", err)
	}
	return nil
}
