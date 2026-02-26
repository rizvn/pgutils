package com.github.rizvn.pgmq;

/**
 * Handler interface for processing messages from the PGMQ queue.
 */
public interface MessageHandler {
    /**
     * Handles a message from the queue.
     *
     * @param msg the message to handle
     */
    void handle(Consumer.PgmqMessage msg);
}

