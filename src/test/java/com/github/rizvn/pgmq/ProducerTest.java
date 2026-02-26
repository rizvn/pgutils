package com.github.rizvn.pgmq;

import com.github.rizvn.DbTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerTest extends DbTest {

    private Producer producer;
    private static final String QUEUE_NAME = "test_queue";

    @BeforeEach
    void setUp() throws SQLException {
        // Setup queue for this test
        setupQueue(QUEUE_NAME);

        // Create producer
        producer = new Producer(dataSource);
    }


    @Test
    void testProducer() throws SQLException {
        // Act - Produce a message
        producer.produce(QUEUE_NAME, "{\"content\": \"Hello, World!\"}", "{}");

        // Assert - Check if message was produced in PGMQ table
        int messageCount;
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM pgmq.q_" + QUEUE_NAME)) {
            assertTrue(rs.next(), "Result set should contain data");
            messageCount = rs.getInt(1);
        }

        assertNotEquals(0, messageCount, "Expected to have produced messages, but message count is 0");
    }

    @Test
    void testProducerWithDefaultHeaders() throws SQLException {
        // Act - Produce a message without headers
        producer.produce(QUEUE_NAME, "{\"content\": \"Hello, World 2!\"}");

        // Assert - Check if message was produced
        int messageCount;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM pgmq.q_" + QUEUE_NAME)) {
            assertTrue(rs.next(), "Result set should contain data");
            messageCount = rs.getInt(1);
        }

        assertNotEquals(0, messageCount, "Expected to have produced messages");
    }
}

