package com.ebay.challenge.streamprocessor.infrastructure;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Coordinates access between event processing and global state eviction.
 * <p>
 * Read lock:
 * - Process event state and watermark updates
 * - Query click state
 * <p>
 * Write lock:
 * - Calculate the global watermark
 * - Evict click state
 * <p>
 * Singleton
 */
@Component
public class StateAccessLock {

    private final ReentrantReadWriteLock lock =
            new ReentrantReadWriteLock(true);

    public Lock readLock() {
        return lock.readLock();
    }

    public Lock writeLock() {
        return lock.writeLock();
    }

}