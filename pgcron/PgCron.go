package pgcron

import (
	"database/sql"
	"fmt"
	"runtime"

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

type Err struct {
	message string
}

func (e *Err) Error() string {
	return e.message
}

func NewErr(message string, wrapErr error) *Err {
	// get refenece to caller function
	pc, _, line, _ := runtime.Caller(1)
	funcName := runtime.FuncForPC(pc).Name()

	e := &Err{}
	e.message = fmt.Sprintf("\nError at: %s : %d\nMessage:%s\n%v", funcName, line, message, wrapErr)
	return e
}

func (s *PgCron) Schedule(jobName string, schedule string, command string) error {
	_, err := s.DbPool.Exec(`SELECT cron.schedule($1, $2, $3)`, jobName, schedule, command)

	if err != nil {
		return NewErr("failed to schedule cron job", err)
	}
	return nil
}

func (s *PgCron) Remove(jobName string) error {
	_, err := s.DbPool.Exec(`SELECT cron.unschedule($1)`, jobName)

	if err != nil {
		return NewErr("failed to remove cron job", err)
	}
	return nil
}

func (s *PgCron) Pause(jobName string) error {

	_, err := s.DbPool.Exec(
		`SELECT cron.alter_job((SELECT jobid FROM cron.job WHERE jobname = $1), active := false)`,
		jobName)
	if err != nil {
		return NewErr("failed to pause cron job", err)
	}
	return nil
}
