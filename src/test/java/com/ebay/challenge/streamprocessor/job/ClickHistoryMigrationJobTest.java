package com.ebay.challenge.streamprocessor.job;

import com.ebay.challenge.streamprocessor.infrastructure.StateAccessLock;
import com.ebay.challenge.streamprocessor.mapper.ClickHistoryMapper;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.Lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClickHistoryMigrationJobTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-08-24T10:00:00Z");
    private static final int BATCH_SIZE = 50;

    @Mock
    private WatermarkTracker watermarkTracker;

    @Mock
    private ClickHistoryMapper clickHistoryMapper;

    @Mock
    private StateAccessLock stateAccessLock;

    @Mock
    private TransactionTemplate transactionTemplate;

    @Mock
    private Lock writeLock;

    private ClickHistoryMigrationJob job;

    @BeforeEach
    void setUp() {
        when(stateAccessLock.writeLock()).thenReturn(writeLock);
        job = new ClickHistoryMigrationJob(
                watermarkTracker,
                clickHistoryMapper,
                stateAccessLock,
                transactionTemplate
        );
        ReflectionTestUtils.setField(job, "migrationBatchSize", BATCH_SIZE);
    }

    // No watermark initialized, skip history migration
    @Test
    void skipsMigrationWhenGlobalWatermarkIsNotInitialized() {
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(Instant.MIN);

        job.migrateExpiredClickHistory();

        verify(writeLock).lock();
        verify(writeLock).unlock();
        verifyNoInteractions(clickHistoryMapper, transactionTemplate);
    }

    // Watermark lag below ten minutes, retain thirty minutes
    @Test
    void usesThirtyMinutesWhenWatermarkLagIsBelowTenMinutes() {
        Instant cutoff = migrateWithWatermark(FIXED_NOW.minus(Duration.ofMinutes(5)), 2);

        assertEquals(FIXED_NOW.minus(Duration.ofMinutes(30)), cutoff);
    }

    // Watermark lag exactly ten minutes, retain thirty minutes
    @Test
    void usesThirtyMinutesWhenWatermarkLagIsExactlyTenMinutes() {
        Instant cutoff = migrateWithWatermark(FIXED_NOW.minus(Duration.ofMinutes(10)), 2);

        assertEquals(FIXED_NOW.minus(Duration.ofMinutes(30)), cutoff);
    }

    // Watermark lag between ten and thirty minutes, retain three times the lag
    @Test
    void usesThreeTimesLagBetweenTenAndThirtyMinutes() {
        Instant cutoff = migrateWithWatermark(FIXED_NOW.minus(Duration.ofMinutes(15)), 2);

        assertEquals(FIXED_NOW.minus(Duration.ofMinutes(45)), cutoff);
    }

    // Watermark lag exactly thirty minutes, retain ninety minutes
    @Test
    void usesNinetyMinutesWhenWatermarkLagIsExactlyThirtyMinutes() {
        Instant cutoff = migrateWithWatermark(FIXED_NOW.minus(Duration.ofMinutes(30)), 2);

        assertEquals(FIXED_NOW.minus(Duration.ofMinutes(90)), cutoff);
    }

    // Watermark lag exceeds thirty minutes, cap retention at ninety minutes
    @Test
    void capsArchiveDurationAtNinetyMinutes() {
        Instant cutoff = migrateWithWatermark(FIXED_NOW.minus(Duration.ofMinutes(45)), 2);

        assertEquals(FIXED_NOW.minus(Duration.ofMinutes(90)), cutoff);
    }

    // Future watermark, clamp negative lag to zero
    @Test
    void clampsFutureWatermarkLagToZero() {
        Instant cutoff = migrateWithWatermark(FIXED_NOW.plus(Duration.ofMinutes(5)), 2);

        assertEquals(FIXED_NOW.minus(Duration.ofMinutes(30)), cutoff);
    }

    // Configured migration batch size passed to the mapper
    @Test
    void passesConfiguredBatchSizeToMapper() {
        migrateWithWatermark(FIXED_NOW.minus(Duration.ofMinutes(5)), 2);

        verify(clickHistoryMapper).migrateNonActiveOlderThan(any(Instant.class), eq(BATCH_SIZE));
    }

    // Null transaction result, migration still completes
    @Test
    void handlesNullMigrationResult() {
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(FIXED_NOW.minus(Duration.ofMinutes(5)));
        when(clickHistoryMapper.migrateNonActiveOlderThan(any(Instant.class), eq(BATCH_SIZE)))
                .thenReturn(2);
        doAnswer(invocation -> {
            TransactionCallback<Integer> callback = invocation.getArgument(0);
            callback.doInTransaction(null);
            return null;
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        try (MockedStatic<Instant> ignored = mockNow()) {
            job.migrateExpiredClickHistory();
        }

        verify(clickHistoryMapper).migrateNonActiveOlderThan(any(Instant.class), eq(BATCH_SIZE));
        verify(writeLock).unlock();
    }

    // Migration failure, release write lock and propagate error
    @Test
    void releasesWriteLockWhenMigrationFails() {
        RuntimeException failure = new RuntimeException("migration failed");
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(FIXED_NOW.minus(Duration.ofMinutes(5)));
        doAnswer(invocation -> {
            throw failure;
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        RuntimeException actual;
        try (MockedStatic<Instant> ignored = mockNow()) {
            actual = assertThrows(RuntimeException.class, () -> job.migrateExpiredClickHistory());
        }

        assertSame(failure, actual);
        verify(writeLock).lock();
        verify(writeLock).unlock();
    }

    private Instant migrateWithWatermark(Instant watermark, int migratedCount) {
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(watermark);
        when(clickHistoryMapper.migrateNonActiveOlderThan(any(Instant.class), eq(BATCH_SIZE)))
                .thenReturn(migratedCount);
        doAnswer(invocation -> {
            TransactionCallback<Integer> callback = invocation.getArgument(0);
            return callback.doInTransaction(null);
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        try (MockedStatic<Instant> ignored = mockNow()) {
            job.migrateExpiredClickHistory();
        }

        ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(clickHistoryMapper).migrateNonActiveOlderThan(cutoffCaptor.capture(), eq(BATCH_SIZE));
        verify(writeLock).lock();
        verify(writeLock).unlock();
        return cutoffCaptor.getValue();
    }

    private static MockedStatic<Instant> mockNow() {
        MockedStatic<Instant> instantMock = mockStatic(Instant.class, Answers.CALLS_REAL_METHODS);
        instantMock.when(Instant::now).thenReturn(FIXED_NOW);
        return instantMock;
    }
}
