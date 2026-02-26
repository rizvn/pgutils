package com.github.rizvn.pgcache;

import com.github.rizvn.DbTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PgCache Tests")
class PgCacheTest extends DbTest {

    private static final Logger logger = LoggerFactory.getLogger(PgCacheTest.class);
    private PgCache pgCache;

    @BeforeEach
    void setUp() {
        // Create PgCache instance with the test dataSource from DbTest
        pgCache = new PgCache(dataSource, "c_test", 600); // 10 minutes TTL
    }

    @Test
    @DisplayName("Put Value")
    void testPutValue() {
        // Arrange
        String testId = "test_key";
        byte[] testContent = "test_value".getBytes();

        // Act
        pgCache.put(testId, testContent);

        // Assert - no exception means success
        logger.info("Successfully put value for key: {}", testId);
    }

    @Test
    @DisplayName("Get Value")
    void testGetValue() {
        // Arrange
        String testId = "test_key";
        byte[] testContent = "test_value".getBytes();
        pgCache.put(testId, testContent);

        // Act
        byte[] content = pgCache.get(testId);

        // Assert
        assertNotNull(content, "Expected to find key " + testId + ", but it was not found");
        byte[] expectedContent = "test_value".getBytes();
        assertArrayEquals(expectedContent, content,
            "Expected content " + new String(expectedContent) + ", got " + new String(content));
    }

    @Test
    @DisplayName("PutWithTTL Value")
    void testPutWithTTL() throws InterruptedException {
        // Arrange
        String testId = "test_key_ttl";
        byte[] testContent = "ttl_value".getBytes();

        // Act - put with 1 second TTL
        pgCache.putWithTtl(testId, testContent, 1);

        // Assert - value should be found immediately
        byte[] content = pgCache.get(testId);
        assertNotNull(content, "Expected to find key " + testId + " with correct value");
        assertEquals("ttl_value", new String(content), "Expected correct value");

        // Wait for TTL to expire
        logger.info("Waiting for 2 seconds for TTL to expire");
        Thread.sleep(2000);

        // Assert - value should be expired
        content = pgCache.get(testId);
        assertNull(content, "Expected key " + testId + " to be expired");
    }

    @Test
    @DisplayName("Delete Value")
    void testDeleteValue() {
        // Arrange
        String testId = "delete_key";
        byte[] testContent = "delete_value".getBytes();
        pgCache.put(testId, testContent);

        // Act
        pgCache.delete(testId);

        // Assert
        byte[] content = pgCache.get(testId);
        assertNull(content, "Expected key " + testId + " to be deleted");
    }

    @Test
    @DisplayName("DeleteExpired")
    void testDeleteExpired() throws InterruptedException {
        // Arrange
        String testId = "expired_key";
        byte[] testContent = "expired_value".getBytes();
        pgCache.putWithTtl(testId, testContent, 1);

        // Wait for expiration
        logger.info("Waiting for 2 seconds for TTL to expire");
        Thread.sleep(2000);

        // Act
        pgCache.deleteExpired();

        // Assert - value should not be found (even without expiration check)
        byte[] content = pgCache.get(testId);
        assertNull(content, "Expected expired key " + testId + " to be deleted");
    }

    @Test
    @DisplayName("ClearCacheStore")
    void testClearCacheStore() {
        // Arrange
        pgCache.put("clear_key", "clear_value".getBytes());

        // Act
        pgCache.clearCacheStore();

        // Assert
        byte[] content = pgCache.get("clear_key");
        assertNull(content, "Expected cache store to be cleared");
    }

    @Test
    @DisplayName("DropCacheStore")
    void testDropCacheStore() {
        // Arrange
        pgCache.put("drop_key", "drop_value".getBytes());

        // Act
        pgCache.dropCacheStore();

        // Assert - table should not exist, so put should throw exception
        assertThrows(RuntimeException.class,
            () -> pgCache.put("drop_key", "drop_value".getBytes()),
            "Expected RuntimeException after dropping cache table");
    }
}

