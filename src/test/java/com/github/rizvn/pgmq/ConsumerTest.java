package com.github.rizvn.pgmq;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest extends DbTest {

    private Producer producer;
    private static final String QUEUE_NAME = "test_queue";

    PgmqMessage receivedMessage = null;

    @BeforeEach
    void setUp() throws SQLException {
        // Setup queue for this test
        setupQueue(QUEUE_NAME);

        receivedMessage = null;

        // Create producer
        producer = new Producer(dataSource);
    }

    @Test
    void testConsumer() throws InterruptedException {

        var messageReceivedLatch = new CountDownLatch(1);

        // Create consumer with message handler
        Consumer testConsumer = new Consumer(dataSource, QUEUE_NAME, msg -> {
            receivedMessage = msg;
            messageReceivedLatch.countDown();
        });

        // Start consumer
        testConsumer.start();

        // Send a test message
        producer.produce(QUEUE_NAME, "{\"content\": \"Hello, Test!\"}", "{}");

        // Wait for message to be received (with timeout)
        boolean messageReceived = messageReceivedLatch.await(10, TimeUnit.SECONDS);

        // Shutdown consumer
        testConsumer.shutdown();

        // Assert that message was received
        assertTrue(messageReceived, "Expected to receive a message within timeout");
        assertNotNull(receivedMessage, "Expected to have received a message");
        assertEquals("{\"content\": \"Hello, Test!\"}", receivedMessage.getMessage(),
                "Expected message content to match");
    }

    @Test
    void testConsumerWithMultipleMessages() throws InterruptedException {
        // Create a latch that waits for 3 messages
        CountDownLatch multiMessageLatch = new CountDownLatch(3);
        final int[] messageCount = {0};

        // Create a new consumer with a message handler that counts messages
        Consumer multiConsumer = new Consumer(dataSource, QUEUE_NAME, msg -> {
            messageCount[0]++;
            multiMessageLatch.countDown();
        });

        // Start consumer
        multiConsumer.start();

        // Send multiple test messages
        producer.produce(QUEUE_NAME, "{\"msg\": \"Message 1\"}", "{}");
        producer.produce(QUEUE_NAME, "{\"msg\": \"Message 2\"}", "{}");
        producer.produce(QUEUE_NAME, "{\"msg\": \"Message 3\"}", "{}");

        // Wait for all messages to be received
        boolean allMessagesReceived = multiMessageLatch.await(15, TimeUnit.SECONDS);

        // Shutdown consumer
        multiConsumer.shutdown();

        // Assert that all messages were received
        assertTrue(allMessagesReceived, "Expected to receive all 3 messages within timeout");
        assertEquals(3, messageCount[0], "Expected to have processed 3 messages");
    }

    @Test
    void testConsumerArchivesMessages() throws SQLException, InterruptedException {

        // Create latch for message received
        CountDownLatch latch = new CountDownLatch(1);
        Consumer archiveConsumer = new Consumer(dataSource, QUEUE_NAME, msg -> {
            receivedMessage = msg;
            latch.countDown();
        });
        archiveConsumer.archiveAfterHandle = true;

        // Start consumer
        archiveConsumer.start();

        // Send a test message
        producer.produce(QUEUE_NAME, "{\"content\": \"Archive Test\"}", "{}");

        // Wait for message to be received
        boolean messageReceived = latch.await(10, TimeUnit.SECONDS);

        // Shutdown consumer
        archiveConsumer.shutdown();

        // Assert that message was received
        assertTrue(messageReceived, "Expected to receive a message");

        // Verify that the message was archived (not in main queue)
        int mainQueueCount;
        int archiveQueueCount;
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM pgmq.q_" + QUEUE_NAME)) {
            assertTrue(rs.next(), "Result set should contain data");
            mainQueueCount = rs.getInt(1);
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM pgmq.a_" + QUEUE_NAME)) {
            assertTrue(rs.next(), "Result set should contain data");
            archiveQueueCount = rs.getInt(1);
        }

        // After archiving, the main queue should be empty and archive should have the message
        assertEquals(0, mainQueueCount, "Main queue should be empty after archiving");
        assertEquals(1, archiveQueueCount, "Archive queue should contain the archived message");
    }
}

