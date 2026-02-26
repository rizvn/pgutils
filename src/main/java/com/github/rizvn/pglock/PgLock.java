package com.github.rizvn.pglock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Represents a PostgreSQL advisory lock that manages its own connection.
 * The lock is automatically released when unlocked, and the connection is closed.
 */
public class PgLock {
    private final String lockName;
    private final long lockId;
    private final Connection conn;

    /**
     * Creates a new PgLock instance.
     *
     * @param lockName the name of the lock
     * @param lockId   the hashed lock ID
     * @param conn     the dedicated connection for this lock
     */
    public PgLock(String lockName, long lockId, Connection conn) {
        this.lockName = Objects.requireNonNull(lockName, "lockName cannot be null");
        this.lockId = lockId;
        this.conn = Objects.requireNonNull(conn, "conn cannot be null");
    }

    /**
     * Releases the advisory lock and closes the connection.
     * If the lock cannot be released, a warning is printed.
     */
    public void unlock() {
        try {
            // Release the advisory lock
            String query = "SELECT pg_advisory_unlock(?)";
            try (var stmt = conn.prepareStatement(query)) {
                stmt.setLong(1, lockId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        boolean released = rs.getBoolean(1);
                        if (!released) {
                            System.out.println("Warning: Lock was not released!");
                        }
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Warning: Lock was not released!");
        } finally {
            // Close the dedicated connection
            try {
                conn.close();
            } catch (SQLException e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }
    }

    /**
     * Gets the lock name.
     *
     * @return the lock name
     */
    public String getLockName() {
        return lockName;
    }

    /**
     * Gets the lock ID.
     *
     * @return the lock ID
     */
    public long getLockId() {
        return lockId;
    }
}

