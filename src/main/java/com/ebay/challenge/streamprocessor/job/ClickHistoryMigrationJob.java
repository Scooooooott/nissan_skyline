package com.ebay.challenge.streamprocessor.job;

import com.ebay.challenge.streamprocessor.infrastructure.StateAccessLock;
import com.ebay.challenge.streamprocessor.mapper.ClickHistoryMapper;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
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
 * Periodically archives non-ACTIVE clicks
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClickHistoryMigrationJob {

    private static final Duration MAX_ARCHIVE_AGE = Duration.ofMinutes(90);

    private final WatermarkTracker watermarkTracker;
    private final ClickHistoryMapper clickHistoryMapper;
    private final StateAccessLock stateAccessLock;
    private final TransactionTemplate transactionTemplate;

    @Value("${click-history.migration-batch-size:500}")
    private int migrationBatchSize;

    /**
     * default: 60 min
     * can be configged by key: output.database.click-state-migration-interval-ms.
     */
    @Scheduled(fixedRateString = "${output.database.click-state-migration-interval-ms:3600000}")
    public void migrateExpiredClickHistory() {
        Lock writeLock = stateAccessLock.writeLock();
        writeLock.lock();

        try {
            Instant globalWatermark = watermarkTracker.getGlobalMinimumWatermark();
            if (Instant.MIN.equals(globalWatermark)) {
                log.debug("ClickHistoryMigrationJob skipped: no global watermark.");
                return;
            }

            Instant now = Instant.now();
            Duration watermarkLag = Duration.between(globalWatermark, now);
            if (watermarkLag.isNegative()) {
                watermarkLag = Duration.ZERO;
            }

            Duration archiveDuration = getArchiveDuration(watermarkLag);
            Instant cutoffTime = now.minus(archiveDuration);

            // against click_history/processed_input for uncommitted Kafka offsets.
            Integer migratedCount = transactionTemplate.execute(status
                    -> clickHistoryMapper.migrateNonActiveOlderThan(cutoffTime, migrationBatchSize));

            log.debug("ClickHistoryMigrationJob done. globalWatermark: {}, archiveDuration: {}, cutoffTime: {}, migratedCount: {}",
                    globalWatermark, archiveDuration, cutoffTime, migratedCount == null ? 0 : migratedCount);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * if watermark is in past 10 min, return 30 min as safety net
     * Min [now - (now-watermark)*3, 90min] as historical data
     * */
    private static Duration getArchiveDuration(Duration watermarkLag) {
        if (watermarkLag.isNegative() || watermarkLag.compareTo(Duration.ofMinutes(10)) < 0) {
            return Duration.ofMinutes(30);
        }
        Duration threeTimesLag = watermarkLag.multipliedBy(3);
        return threeTimesLag.compareTo(MAX_ARCHIVE_AGE) > 0 ? MAX_ARCHIVE_AGE : threeTimesLag;
    }
}
