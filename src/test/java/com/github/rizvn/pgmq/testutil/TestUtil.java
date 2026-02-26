package com.github.rizvn.pgmq.testutil;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Test utilities for starting PostgreSQL test containers.
 */
public class TestUtil {

    /**
     * Starts a PostgreSQL test container and returns it along with the connection string.
     *
     * @return a {@link PostgresContainerResult} containing the container and DSN
     */
    public static PostgresContainerResult startPgTestContainer() {
        PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
                DockerImageName.parse("postgres:17-alpine"))
                .withUsername("app_admin")
                .withPassword("app_admin")
                .withDatabaseName("app_db");

        postgres.start();

        String dsn = String.format(
                "jdbc:postgresql://%s:%d/%s",
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getDatabaseName()
        );

        return new PostgresContainerResult(postgres, dsn);
    }

    /**
     * Result class containing the PostgreSQL container and connection string.
     */
    public static class PostgresContainerResult {
        public final PostgreSQLContainer<?> container;
        public final String dsn;

        public PostgresContainerResult(PostgreSQLContainer<?> container, String dsn) {
            this.container = container;
            this.dsn = dsn;
        }
    }
}

