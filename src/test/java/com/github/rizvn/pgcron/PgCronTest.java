package com.github.rizvn.pgcron;

import com.github.rizvn.DbTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PgCron Tests")
class PgCronTest extends DbTest {

    private static final Logger logger = LoggerFactory.getLogger(PgCronTest.class);
    private PgCron pgCron;

    @BeforeEach
    void setUp() {
        // Create PgCron instance with the test dataSource from DbTest
        pgCron = new PgCron(dataSource);
    }

    @Test
    @DisplayName("Schedule Job")
    void testScheduleJob() throws SQLException, InterruptedException {
        // Arrange
        String queueName = "test_queue";
        String jobName = "test_job";
        setupQueue(queueName);

        // Act - Schedule job to produce message every minute
        logger.info("Scheduling {} to run every minute", jobName);
        String command = """
            SELECT * from pgmq.send(
              queue_name  => 'test_queue',
              msg         => '{"msg":"hello from cron"}',
              headers     => '{}')
            """;
        pgCron.schedule(jobName, "* * * * *", command);

        // Wait just over a minute to allow job to run
        logger.info("Waiting for 70 seconds to allow job to run");
        Thread.sleep(70 * 1000);

        // Assert - verify at least 1 message was produced
        logger.info("Querying pgmq.q_test_queue table for messages");
        int count = getMessageCount(queueName);

        assertTrue(count > 0, "Expected at least 1 message in pgmq table, got " + count);
    }

    @Test
    @DisplayName("Pause Job")
    void testPauseJob() throws SQLException, InterruptedException {
        // Arrange
        String queueName = "test_queue";
        String jobName = "test_job";
        setupQueue(queueName);

        // Act - Schedule job
        logger.info("Scheduling {} to run every minute", jobName);
        String command = """
            SELECT * from pgmq.send(
              queue_name  => 'test_queue',
              msg         => '{"msg":"hello from cron"}',
              headers     => '{}')
            """;
        pgCron.schedule(jobName, "* * * * *", command);

        // Pause the job
        pgCron.pause(jobName);

        // Wait for 70 seconds
        logger.info("Waiting for 70 seconds to allow job to run");
        Thread.sleep(70 * 1000);

        // Assert - verify no messages were produced since job was paused
        logger.info("Querying pgmq.q_test_queue table for messages");
        int count = getMessageCount(queueName);

        assertEquals(0, count, "Expected 0 messages in pgmq table since job was paused, got " + count);
    }

    /**
     * Helper method to get the count of messages in a PGMQ queue.
     *
     * @param queueName the name of the queue
     * @return the count of messages
     * @throws SQLException if database operations fail
     */
    private int getMessageCount(String queueName) throws SQLException {
        String query = "SELECT COUNT(*) FROM pgmq.q_" + queueName;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }
}

