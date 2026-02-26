package com.github.rizvn.pgmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;

public class Consumer {
    static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    final String queueName;
    final MessageHandler messageHandler;
    final DataSource dataSource;

    // Configurable fields with defaults
    int pollingInterval         = 1;
    int visibilityTimeout       = 10;
    int concurrentMsgs          = 10;
    boolean archiveAfterHandle  = false;
    int exponentialBackoff      = 0;
    int exponentialPollingLimit = 10;

    // Internal fields
    final BlockingQueue<PgmqMessage> buffer = new LinkedBlockingQueue<>(concurrentMsgs);

    // Using virtual threads for handling messages and polling
    final ExecutorService threadPool = Executors.newVirtualThreadPerTaskExecutor();
    volatile boolean running         = false;

    int sleepSecs;

    public Consumer(String queueName, MessageHandler messageHandler, DataSource dataSource) {
        this.queueName = queueName;
        this.messageHandler = messageHandler;
        this.dataSource = dataSource;
        this.sleepSecs = pollingInterval;
    }


    private void createQueueIfNotExists() throws SQLException {
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement("SELECT * FROM pgmq.create(?)")) {
            stmt.setString(1, queueName);
            stmt.execute();
        }
    }

    public void start() {
      try {
        createQueueIfNotExists();
        this.sleepSecs = pollingInterval;
      }
      catch (SQLException e) {
        throw new IllegalStateException("Failed to initialize Consumer", e);
      }

        running = true;

        // start message handler
        threadPool.submit(this::handleMessages);

        // start polling messages from db
        threadPool.submit(this::pollMessages);
    }

    public void shutdown() {
        running = false;
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

  /**
   * Poll messagees and put them on a blocking queue
   */
  private void pollMessages() {
        while (running) {
            try (var conn = dataSource.getConnection();
                 var stmt = conn.prepareStatement("SELECT * FROM pgmq.read(queue_name => ?, vt => ?, qty => 1)");) {
                stmt.setString(1, queueName);
                stmt.setInt(2, visibilityTimeout);
                try (var rs = stmt.executeQuery()) {
                    int msgCount = 0;
                    while (rs.next()) {
                        // if there is a message
                        var msg = new PgmqMessage();
                        msg.setMsgID(rs.getLong(1));
                        msg.setReadCount(rs.getInt(2));
                        msg.setEnqueuedAt(rs.getTimestamp(3).toInstant());
                        msg.setVT(rs.getTimestamp(4).toInstant());
                        msg.setMessage(rs.getString(5));
                        msg.setHeaders((Map<String, Object>) rs.getObject(6));

                        // add to processing queue
                        buffer.put(msg);
                        msgCount++;
                    }
                    if (msgCount == 0) {
                        sleepSecs = Math.min(sleepSecs + exponentialBackoff, exponentialPollingLimit);
                        logger.debug("No messages found, sleeping for {} seconds...", sleepSecs);
                        TimeUnit.SECONDS.sleep(sleepSecs);
                    } else {
                        sleepSecs = pollingInterval;
                    }
                }
            } catch (Exception e) {
                logger.error("Error polling messages", e);
            }
        }
    }

    private void handleMessages() {
        while (running) {
            try {
                // block until a message is available,
                PgmqMessage msg = buffer.take();

                //then process it in a separate thread
                threadPool.submit(() -> processMessage(msg));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void processMessage(PgmqMessage msg) {
        // start visibility extender thread
        var visibilityExtender = threadPool.submit(() -> visibilityExtender(msg));

        try {
            // handle the message
            messageHandler.handle(msg);

            if (archiveAfterHandle) {
                archiveMessage(msg);
            } else {
                deleteMessage(msg);
            }
        } finally {
            // stop visibility extender thread
            visibilityExtender.cancel(true);
        }
    }

    private void visibilityExtender(PgmqMessage msg) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                TimeUnit.SECONDS.sleep(visibilityTimeout / 2);
                updateVisibilityTimeout(msg);
            }
        } catch (InterruptedException ignored) {
        }
    }

    private void deleteMessage(PgmqMessage msg) {
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "SELECT * FROM pgmq.delete(queue_name => ?, msg_id => ?)");) {
            stmt.setString(1, queueName);
            stmt.setLong(2, msg.getMsgID());
            stmt.execute();
        } catch (SQLException e) {
            logger.error("Failed to delete message {}", msg.getMsgID(), e);
        }
    }

    private void archiveMessage(PgmqMessage msg) {
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "SELECT * FROM pgmq.archive(queue_name => ?, msg_id => ?)");) {
            stmt.setString(1, queueName);
            stmt.setLong(2, msg.getMsgID());
            stmt.execute();
        } catch (SQLException e) {
            logger.error("Failed to archive message {}", msg.getMsgID(), e);
        }
    }

    private void updateVisibilityTimeout(PgmqMessage msg) {
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                     "SELECT * FROM pgmq.set_vt(queue_name => ?, msg_id => ?, vt => ?)");) {
            stmt.setString(1, queueName);
            stmt.setLong(2, msg.getMsgID());
            stmt.setInt(3, visibilityTimeout);
            stmt.execute();
        } catch (SQLException e) {
            logger.error("Failed to update visibility timeout for message {}", msg.getMsgID(), e);
        }
    }


    // PgmqMessage class (simplified, needs to be expanded as per actual schema)
    public static class PgmqMessage {
        long msgID;
        int readCount;
        Instant enqueuedAt;
        Instant vt;
        String message;
        Map<String, Object> headers;
        // getters and setters
        public long getMsgID() { return msgID; }
        public void setMsgID(long msgID) { this.msgID = msgID; }
        public int getReadCount() { return readCount; }
        public void setReadCount(int readCount) { this.readCount = readCount; }
        public Instant getEnqueuedAt() { return enqueuedAt; }
        public void setEnqueuedAt(Instant enqueuedAt) { this.enqueuedAt = enqueuedAt; }
        public Instant getVT() { return vt; }
        public void setVT(Instant vt) { this.vt = vt; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public Map<String, Object> getHeaders() { return headers; }
        public void setHeaders(Map<String, Object> headers) { this.headers = headers; }
    }


  public void setPollingInterval(int pollingInterval) { this.pollingInterval = pollingInterval; }
  public void setVisibilityTimeout(int visibilityTimeout) { this.visibilityTimeout = visibilityTimeout; }
  public void setConcurrentMsgs(int concurrentMsgs) { this.concurrentMsgs = concurrentMsgs; }
  public void setArchiveAfterHandle(boolean archiveAfterHandle) { this.archiveAfterHandle = archiveAfterHandle; }
  public void setExponentialBackoff(int exponentialBackoff) { this.exponentialBackoff = exponentialBackoff; }
  public void setExponentialPollingLimit(int exponentialPollingLimit) { this.exponentialPollingLimit = exponentialPollingLimit; }
}
