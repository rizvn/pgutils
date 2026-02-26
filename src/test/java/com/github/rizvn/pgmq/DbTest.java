package com.github.rizvn.pgmq;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

/**
 * Base class for database integration tests.
 * Handles PostgreSQL test container lifecycle and provides a DataSource for tests.
 */
public abstract class DbTest {

    protected static GenericContainer<?> container;
    protected static DataSource dataSource;

    @BeforeAll
    @SuppressWarnings("SqlSourceToSinkFlow")
    static void setUpDatabase() throws SQLException {
        // Build the image from the context directory (equivalent to Go's Context field)
        // Using ImageFromDockerfile with withDockerfile for the Dockerfile path
        ImageFromDockerfile imageFromDockerfile = new ImageFromDockerfile("pg-test", true)
          .withFileFromPath(".", new java.io.File("docker-compose/postgres").toPath())
          .withDockerfile(new java.io.File("docker-compose/postgres/postgres-custom.Dockerfile").toPath());

        // Create a combined wait strategy equivalent to wait.ForAll()
        WaitAllStrategy waitStrategy = new WaitAllStrategy()
                .withStrategy(Wait.forListeningPort())  // Wait for port 5432/tcp
                .withStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(60)));

        // Start PostgreSQL test container with custom Dockerfile
        // Equivalent to testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{...})
        container = new GenericContainer<>(imageFromDockerfile)
                .withExposedPorts(5432)
                .withEnv("POSTGRES_USER", "app_admin")
                .withEnv("POSTGRES_PASSWORD", "app_admin")
                .withEnv("POSTGRES_DB", "app_db")
                .waitingFor(waitStrategy);

        container.start();

        // Create Tomcat JDBC connection pool DataSource
        dataSource = new DataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(String.format("jdbc:postgresql://%s:%d/%s",
                container.getHost(),
                container.getMappedPort(5432),
                "app_db"));
        dataSource.setUsername("app_admin");
        dataSource.setPassword("app_admin");
        dataSource.setMaxActive(10);
        dataSource.setMaxIdle(5);
        dataSource.setMinIdle(2);

        initializePgmq();
    }

    @AfterAll
    static void tearDownDatabase() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    /**
     * Initializes PGMQ extension. Can be overridden by subclasses if needed.
     */
    protected static void initializePgmq() {
        // Default implementation - PGMQ may already be installed in the container
        // Subclasses can override this method for custom initialization
    }

    /**
     * Creates and purges a PGMQ queue for testing.
     *
     * @param queueName the name of the queue to create and purge
     * @throws SQLException if database operations fail
     */
    protected static void setupQueue(String queueName) throws SQLException {
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("SELECT * FROM pgmq.create('" + queueName + "')");
        }

        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("SELECT * FROM pgmq.purge_queue('" + queueName + "')");
        }
    }

    /**
     * Purges a PGMQ queue.
     *
     * @param queueName the name of the queue to purge
     * @throws SQLException if database operations fail
     */
    @SuppressWarnings("unused")
    protected static void purgeQueue(String queueName) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT * FROM pgmq.purge_queue('" + queueName + "')");
        }
    }
}

