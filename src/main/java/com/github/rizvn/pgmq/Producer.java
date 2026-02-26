package com.github.rizvn.pgmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Producer for sending messages to a PGMQ queue.
 * Requires a DataSource to be provided for database connectivity.
 */
public class Producer {
    static final Logger logger = LoggerFactory.getLogger(Producer.class);

    final DataSource dataSource;

    /**
     * Constructs a Producer with the required DataSource.
     *
     * @param dataSource the DataSource for database connectivity (required)
     * @throws IllegalArgumentException if dataSource is null
     */
    public Producer(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("DataSource is required");
        }
        this.dataSource = dataSource;
    }

    /**
     * Sends a message to the specified queue without custom headers.
     *
     * @param queueName the name of the queue
     * @param message   the message content
     */
    public void produce(String queueName, String message) {
        produce(queueName, message, "{}");
    }

    /**
     * Sends a message to the specified queue with optional headers.
     *
     * @param queueName the name of the queue
     * @param message   the message content
     * @param headers   the JSON headers (defaults to "{}" if empty)
     * @throws IllegalStateException if message sending fails
     */
    public void produce(String queueName, String message, String headers) {
        if (headers == null || headers.isEmpty()) {
            headers = "{}";
        }

        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "SELECT * FROM pgmq.send(?, ?::jsonb, ?::jsonb)")) {
            stmt.setString(1, queueName);
            stmt.setString(2, message);
            stmt.setString(3, headers);
            stmt.execute();
        } catch (SQLException e) {
            logger.error("Failed to send message to queue: {}", queueName, e);
            throw new IllegalStateException("Failed to send message: " + e.getMessage(), e);
        }
    }
}

