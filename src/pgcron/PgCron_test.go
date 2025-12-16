package pgcron_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rizvn/pgutils/pgcron"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPgCron(t *testing.T) {
	ctx := context.Background()

	// define Postgres container request
	req := testcontainers.ContainerRequest{
		// Using custom Dockerfile to include pg_cron extension
		FromDockerfile: testcontainers.FromDockerfile{
			Context:        "../../docker-compose/postgres",
			Dockerfile:     "postgres-custom.Dockerfile",
			BuildLogWriter: os.Stdout,
			Tag:            "pg-test",
			KeepImage:      true,
		},
		//Image:        "postgres:17-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "app_admin",
			"POSTGRES_PASSWORD": "app_admin",
			"POSTGRES_DB":       "app_db",
		},

		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5432/tcp"),
			wait.ForLog("database system is ready to accept connections").WithStartupTimeout(60*time.Second),
		),
	}

	postgresC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	// Ensure the container is terminated after the test
	defer func() { _ = postgresC.Terminate(ctx) }()

	host, err := postgresC.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := postgresC.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://app_admin:app_admin@%s:%s/app_db", host, port.Port())

	config, err := pgxpool.ParseConfig(dsn)
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
