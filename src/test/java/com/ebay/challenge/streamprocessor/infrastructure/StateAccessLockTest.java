package com.ebay.challenge.streamprocessor.infrastructure;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateAccessLockTest {

    private final StateAccessLock stateAccessLock = new StateAccessLock();

    /**
     * readLock() / writeLock()
     *
     * 1.1 readLock() returns a lock handle
     * 1.2 writeLock() returns a lock handle
     * */

    @Test
    void readLock_returnsLockHandle() {
        Lock readLock = stateAccessLock.readLock();

        assertNotNull(readLock);
        assertSame(readLock, stateAccessLock.readLock());
    }

    @Test
    void writeLock_returnsLockHandle() {
        Lock writeLock = stateAccessLock.writeLock();

        assertNotNull(writeLock);
        assertSame(writeLock, stateAccessLock.writeLock());
    }

    /**
     * Read/write coordination
     *
     * 2.1 multiple readers can hold the read lock concurrently
     * 2.2 a writer excludes another writer
     * 2.3 a read lock excludes a writer
     * 2.4 a write lock excludes a reader
     * */

    @Test
    void readLock_allowsConcurrentReaders() {
        Lock readLock = stateAccessLock.readLock();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        readLock.lock();
        try {
            Future<Boolean> secondReader = executor.submit(() -> {
                boolean acquired = readLock.tryLock();
                if (acquired) {
                    readLock.unlock();
                }
                return acquired;
            });

            assertTrue(secondReader.get(1, TimeUnit.SECONDS));
        } catch (Exception exception) {
            throw new AssertionError("The concurrent reader did not complete", exception);
        } finally {
            readLock.unlock();
            executor.shutdownNow();
        }
    }

    @Test
    void writeLock_excludesAnotherWriter() {
        Lock writeLock = stateAccessLock.writeLock();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        writeLock.lock();
        try {
            Future<Boolean> secondWriter = executor.submit(() -> {
                boolean acquired = writeLock.tryLock();
                if (acquired) {
                    writeLock.unlock();
                }
                return acquired;
            });

            assertFalse(secondWriter.get(1, TimeUnit.SECONDS));
        } catch (Exception exception) {
            throw new AssertionError("The competing writer did not complete", exception);
        } finally {
            writeLock.unlock();
            executor.shutdownNow();
        }
    }

    @Test
    void readLock_excludesWriter() {
        Lock readLock = stateAccessLock.readLock();
        Lock writeLock = stateAccessLock.writeLock();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        readLock.lock();
        try {
            Future<Boolean> writer = executor.submit(() -> {
                boolean acquired = writeLock.tryLock();
                if (acquired) {
                    writeLock.unlock();
                }
                return acquired;
            });

            assertFalse(writer.get(1, TimeUnit.SECONDS));
        } catch (Exception exception) {
            throw new AssertionError("The writer did not complete", exception);
        } finally {
            readLock.unlock();
            executor.shutdownNow();
        }
    }

    @Test
    void writeLock_excludesReader() {
        Lock readLock = stateAccessLock.readLock();
        Lock writeLock = stateAccessLock.writeLock();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        writeLock.lock();
        try {
            Future<Boolean> reader = executor.submit(() -> {
                boolean acquired = readLock.tryLock();
                if (acquired) {
                    readLock.unlock();
                }
                return acquired;
            });

            assertFalse(reader.get(1, TimeUnit.SECONDS));
        } catch (Exception exception) {
            throw new AssertionError("The reader did not complete", exception);
        } finally {
            writeLock.unlock();
            executor.shutdownNow();
        }
    }
}
