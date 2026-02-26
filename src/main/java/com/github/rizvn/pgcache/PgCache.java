package com.github.rizvn.pgcache;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A PostgreSQL-backed distributed cache implementation with automatic TTL-based expiration.
 */
public class PgCache {
    private final DataSource dataSource;
    private final String cacheTable;
    private final int ttl;
    private final int cleanerInterval;
    private ScheduledExecutorService cleanerExecutor;

    /**
     * Creates a new PgCache instance.
     *
     * @param dataSource     the PostgreSQL DataSource (required)
     * @param cacheTable     the name of the cache table (required)
     * @param ttl            the default TTL in seconds
     * @param cleanerInterval the interval in seconds for cleaning expired entries (default: 60)
     */
    public PgCache(DataSource dataSource, String cacheTable, int ttl, int cleanerInterval) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource cannot be null");
        this.cacheTable = Objects.requireNonNull(cacheTable, "cacheTable cannot be null");
        this.ttl = ttl > 0 ? ttl : 86400; // Default TTL of 1 day if not specified
        this.cleanerInterval = cleanerInterval > 0 ? cleanerInterval : 60;
        createCacheTable();
    }

    /**
     * Creates a new PgCache instance with default cleaner interval of 60 seconds.
     *
     * @param dataSource the PostgreSQL DataSource (required)
     * @param cacheTable the name of the cache table (required)
     * @param ttl        the default TTL in seconds
     */
    public PgCache(DataSource dataSource, String cacheTable, int ttl) {
        this(dataSource, cacheTable, ttl, 60);
    }


    /**
     * Creates the cache table if it doesn't exist.
     */
    public void createCacheTable() {
        String query = "CREATE UNLOGGED TABLE IF NOT EXISTS " + cacheTable + " (\n" +
                "    id TEXT PRIMARY KEY,\n" +
                "    content BYTEA NOT NULL,\n" +
                "    created_on TIMESTAMPTZ NOT NULL,\n" +
                "    expires_on TIMESTAMPTZ NOT NULL\n" +
                ")";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to create cache store: " + e.getMessage(), e);
        }
    }

    /**
     * Puts a value in the cache with the default TTL.
     *
     * @param id      the cache key
     * @param content the cache value as bytes
     */
    public void put(String id, byte[] content) {
        putWithTtl(id, content, ttl);
    }

    /**
     * Puts a value in the cache with a custom TTL.
     *
     * @param id      the cache key
     * @param content the cache value as bytes
     * @param ttlSeconds the TTL in seconds
     */
    public void putWithTtl(String id, byte[] content, int ttlSeconds) {
        String query = "INSERT INTO " + cacheTable + " (id, content, created_on, expires_on) " +
                "VALUES (?, ?, NOW(), NOW() + INTERVAL '" + ttlSeconds + " seconds') " +
                "ON CONFLICT (id) " +
                "DO UPDATE SET content = EXCLUDED.content, created_on = EXCLUDED.created_on, expires_on = EXCLUDED.expires_on";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, id);
            stmt.setBytes(2, content);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to cache value: " + e.getMessage(), e);
        }
    }

    /**
     * Gets a value from the cache if it exists and has not expired.
     *
     * @param id the cache key
     * @return an array containing the cache value, or null if not found or expired
     */
    public byte[] get(String id) {
        String query = "SELECT content FROM " + cacheTable + " WHERE id = ? AND expires_on > NOW()";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBytes(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("failed to retrieve cache value: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Deletes a specific cache entry.
     *
     * @param id the cache key
     */
    public void delete(String id) {
        String query = "DELETE FROM " + cacheTable + " WHERE id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, id);
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to delete cache entry: " + e.getMessage(), e);
        }
    }

    /**
     * Deletes all expired cache entries.
     */
    public void deleteExpired() {
        String query = "DELETE FROM " + cacheTable + " WHERE expires_on <= NOW()";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to delete expired cache entries: " + e.getMessage(), e);
        }
    }

    /**
     * Starts the background cleanup task that periodically removes expired entries.
     */
    public synchronized void startCleaner() {
        if (cleanerExecutor == null || cleanerExecutor.isShutdown()) {
            cleanerExecutor = Executors.newScheduledThreadPool(1, r -> {
                Thread t = new Thread(r, "PgCache-Cleaner");
                t.setDaemon(true);
                return t;
            });
            cleanerExecutor.scheduleAtFixedRate(
                    this::deleteExpired,
                    cleanerInterval,
                    cleanerInterval,
                    TimeUnit.SECONDS
            );
        }
    }

    /**
     * Stops the background cleanup task.
     */
    public synchronized void stopCleaner() {
        if (cleanerExecutor != null && !cleanerExecutor.isShutdown()) {
            cleanerExecutor.shutdown();
            try {
                if (!cleanerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Removes all cache entries.
     */
    public void clearCacheStore() {
        String query = "TRUNCATE TABLE " + cacheTable;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to clear cache store: " + e.getMessage(), e);
        }
    }

    /**
     * Drops the entire cache table.
     */
    public void dropCacheStore() {
        String query = "DROP TABLE IF EXISTS " + cacheTable;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("failed to drop cache store: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the current TTL in seconds.
     *
     * @return the default TTL value
     */
    public int getTtl() {
        return ttl;
    }

    /**
     * Gets the cache table name.
     *
     * @return the cache table name
     */
    public String getCacheTable() {
        return cacheTable;
    }

    /**
     * Gets the cleaner interval in seconds.
     *
     * @return the cleaner interval value
     */
    public int getCleanerInterval() {
        return cleanerInterval;
    }
}

