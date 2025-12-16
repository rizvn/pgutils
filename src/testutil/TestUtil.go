package testutil

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func StartPgTestContainer() (testcontainers.Container, string) {
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
		panic(fmt.Sprintf("failed to start postgres container: %v", err))
	}

	host, err := postgresC.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get container host: %v", err))
	}
	port, err := postgresC.MappedPort(ctx, "5432")
	if err != nil {
		panic(fmt.Sprintf("failed to get mapped port: %v"))
	}
	dsn := fmt.Sprintf("postgres://app_admin:app_admin@%s:%s/app_db", host, port.Port())

	return postgresC, dsn
}
