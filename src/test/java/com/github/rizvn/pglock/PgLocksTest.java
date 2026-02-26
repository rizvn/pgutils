package com.github.rizvn.pglock;

import com.github.rizvn.DbTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PgLocks Tests")
class PgLocksTest extends DbTest {

    private PgLocks pgLocks;

    @BeforeEach
    void setUp() {
        // Create PgLocks instance with the test dataSource from DbTest
        pgLocks = new PgLocks(dataSource);
    }

    @Test
    @DisplayName("Lock and unlock")
    void testLockAndUnlock() {
        // Acquire a lock
        PgLock lock = pgLocks.lock("test-lock");
        assertNotNull(lock, "Expected to acquire a lock");

        // Unlock
        lock.unlock();
    }

    @Test
    @DisplayName("Verify no race condition")
    void testVerifyNoRaceCondition() throws InterruptedException {
        // Use CountDownLatch to synchronize 3 threads
        CountDownLatch latch = new CountDownLatch(3);
        AtomicInteger routineId = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Thread 1
        Thread thread1 = new Thread(() -> {
            try {
                int id = 1;
                System.out.println("Thread 1 trying to acquire lock");
                PgLock lock = pgLocks.lock("test-lock");
                routineId.set(id);

                System.out.println("Acquired lock 1");
                Thread.sleep(1000);

                if (routineId.get() != id) {
                    System.err.println("Thread 1: expected routineId " + id + " but got " + routineId.get());
                    errorCount.incrementAndGet();
                }
                Thread.sleep(1000);

                if (routineId.get() != id) {
                    System.err.println("Thread 1: expected routineId " + id + " but got " + routineId.get());
                    errorCount.incrementAndGet();
                }

                lock.unlock();
                System.out.println("Released lock 1");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });

        // Thread 2
        Thread thread2 = new Thread(() -> {
            try {
                int id = 2;
                System.out.println("Thread 2 trying to acquire lock");
                PgLock lock = pgLocks.lock("test-lock");
                routineId.set(id);

                System.out.println("Acquired lock 2");
                Thread.sleep(1000);

                if (routineId.get() != id) {
                    System.err.println("Thread 2: expected routineId " + id + " but got " + routineId.get());
                    errorCount.incrementAndGet();
                }
                Thread.sleep(1000);

                if (routineId.get() != id) {
                    System.err.println("Thread 2: expected routineId " + id + " but got " + routineId.get());
                    errorCount.incrementAndGet();
                }

                lock.unlock();
                System.out.println("Released lock 2");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });

        // Thread 3
        Thread thread3 = new Thread(() -> {
            try {
                int id = 3;
                System.out.println("Thread 3 trying to acquire lock");
                PgLock lock = pgLocks.lock("test-lock");
                routineId.set(id);

                System.out.println("Acquired lock 3");
                Thread.sleep(1000);

                if (routineId.get() != id) {
                    System.err.println("Thread 3: expected routineId " + id + " but got " + routineId.get());
                    errorCount.incrementAndGet();
                }
                Thread.sleep(1000);

                if (routineId.get() != id) {
                    System.err.println("Thread 3: expected routineId " + id + " but got " + routineId.get());
                    errorCount.incrementAndGet();
                }

                lock.unlock();
                System.out.println("Released lock 3");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });

        // Start all threads
        thread1.start();
        thread2.start();
        thread3.start();

        // Wait for all threads to complete
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");
        assertEquals(0, errorCount.get(), "No race condition errors should occur");
    }
}

