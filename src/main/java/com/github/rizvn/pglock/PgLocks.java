package com.github.rizvn.pglock;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Manager for PostgreSQL advisory locks.
 * Provides methods to acquire and release locks with automatic hashing of lock names.
 */
public class PgLocks {
    private final DataSource dbPool;

    /**
     * Creates a new PgLocks instance.
     *
     * @param dbPool the connection pool (DataSource)
     * @throws NullPointerException if dbPool is null
     */
    public PgLocks(DataSource dbPool) {
        this.dbPool = Objects.requireNonNull(dbPool, "dbPool is required");
    }

    /**
     * @param lockName the name to hash
     */
    private static long hashLock(String lockName) {
        byte[] bytes = lockName.getBytes(StandardCharsets.UTF_8);
        long hash = 0;
        for (int i = 0; i < Math.min(bytes.length, 8); i++) {
            hash = (hash << 8) | (bytes[i] & 0xFF);
        }
        return hash;
    }

    /**
     * Acquires an advisory lock with the given name.
     * This method blocks until the lock is acquired.
     * The caller is responsible for calling unlock() on the returned PgLock to release it.
     *
     * @param lockName the name of the lock to acquire
     * @return a PgLock object representing the acquired lock
     * @throws RuntimeException if the lock cannot be acquired or the connection fails
     */
    public PgLock lock(String lockName) {
        Objects.requireNonNull(lockName, "lockName cannot be null");

        try {
            // Get a dedicated connection for this lock
            Connection conn = dbPool.getConnection();
            long lockId = hashLock(lockName);

            try {
                // Acquire the advisory lock (blocks until available)
                String query = "SELECT pg_advisory_lock(?)";
                try (var stmt = conn.prepareStatement(query)) {
                    stmt.setLong(1, lockId);
                    stmt.execute();
                }
            } catch (SQLException e) {
                conn.close();
                throw e;
            }

            return new PgLock(lockName, lockId, conn);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to acquire lock: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the DataSource used by this PgLocks instance.
     *
     * @return the DataSource
     */
    public DataSource getDbPool() {
        return dbPool;
    }
}

