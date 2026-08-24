package com.ebay.challenge.streamprocessor.job;

import com.ebay.challenge.streamprocessor.infrastructure.StateAccessLock;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputHistoryMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.Lock;

/**
 * Periodically archives processed input records older than twelve hours.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProcessedInputHistoryMigrationJob {

    private static final Duration RETENTION_DURATION = Duration.ofHours(12);

    private final ProcessedInputHistoryMapper processedInputHistoryMapper;
    private final StateAccessLock stateAccessLock;
    private final TransactionTemplate transactionTemplate;

    @Value("${processed-input-history.migration-batch-size:500}")
    private int migrationBatchSize;

    @Scheduled(fixedRateString = "${processed-input-history.migration-interval-ms:86400000}")
    public void migrateExpiredProcessedInputHistory() {
        Lock writeLock = stateAccessLock.writeLock();
        writeLock.lock();

        try {
            Instant cutoffTime = Instant.now().minus(RETENTION_DURATION);

            Integer migratedCount = transactionTemplate.execute(status
                    -> processedInputHistoryMapper.migrateOlderThan(cutoffTime, migrationBatchSize));

            log.debug("ProcessedInputHistoryMigrationJob done. cutoffTime: {}, migratedCount: {}",
                    cutoffTime, migratedCount == null ? 0 : migratedCount);
        } finally {
            writeLock.unlock();
        }
    }
}
