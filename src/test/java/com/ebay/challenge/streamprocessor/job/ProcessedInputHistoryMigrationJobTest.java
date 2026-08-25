package com.ebay.challenge.streamprocessor.job;

import com.ebay.challenge.streamprocessor.infrastructure.StateAccessLock;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputHistoryMapper;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProcessedInputHistoryMigrationJobTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-08-24T10:00:00Z");
    private static final int BATCH_SIZE = 50;

    @Mock
    private ProcessedInputHistoryMapper processedInputHistoryMapper;

    @Mock
    private StateAccessLock stateAccessLock;

    @Mock
    private TransactionTemplate transactionTemplate;

    @Mock
    private Lock writeLock;

    private ProcessedInputHistoryMigrationJob job;

    @BeforeEach
    void setUp() {
        when(stateAccessLock.writeLock()).thenReturn(writeLock);
        job = new ProcessedInputHistoryMigrationJob(
                processedInputHistoryMapper,
                stateAccessLock,
                transactionTemplate
        );
        ReflectionTestUtils.setField(job, "migrationBatchSize", BATCH_SIZE);
    }

    // Normal migration, use the twelve-hour retention cutoff
    @Test
    void migratesRecordsOlderThanTwelveHours() {
        when(processedInputHistoryMapper.migrateOlderThan(any(Instant.class), eq(BATCH_SIZE)))
                .thenReturn(3);
        doAnswer(invocation -> {
            TransactionCallback<Integer> callback = invocation.getArgument(0);
            return callback.doInTransaction(null);
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        try (MockedStatic<Instant> ignored = mockNow()) {
            job.migrateExpiredProcessedInputHistory();
        }

        ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(processedInputHistoryMapper).migrateOlderThan(cutoffCaptor.capture(), eq(BATCH_SIZE));
        assertEquals(FIXED_NOW.minus(Duration.ofHours(12)), cutoffCaptor.getValue());
        verify(writeLock).lock();
        verify(writeLock).unlock();
    }

    // Configured migration batch size passed to the mapper
    @Test
    void passesConfiguredBatchSizeToMapper() {
        runMigrationWithResult(1);

        verify(processedInputHistoryMapper).migrateOlderThan(any(Instant.class), eq(BATCH_SIZE));
    }

    // No eligible processed input records, migration returns zero
    @Test
    void handlesZeroMigrationResult() {
        runMigrationWithResult(0);

        verify(processedInputHistoryMapper).migrateOlderThan(any(Instant.class), eq(BATCH_SIZE));
        verify(writeLock).unlock();
    }

    // Null transaction result, migration still completes
    @Test
    void handlesNullMigrationResult() {
        when(processedInputHistoryMapper.migrateOlderThan(any(Instant.class), eq(BATCH_SIZE)))
                .thenReturn(1);
        doAnswer(invocation -> {
            TransactionCallback<Integer> callback = invocation.getArgument(0);
            callback.doInTransaction(null);
            return null;
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        try (MockedStatic<Instant> ignored = mockNow()) {
            job.migrateExpiredProcessedInputHistory();
        }

        verify(processedInputHistoryMapper).migrateOlderThan(any(Instant.class), eq(BATCH_SIZE));
        verify(writeLock).unlock();
    }

    // Migration failure, release write lock and propagate error
    @Test
    void releasesWriteLockWhenMigrationFails() {
        RuntimeException failure = new RuntimeException("migration failed");
        doAnswer(invocation -> {
            throw failure;
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        RuntimeException actual;
        try (MockedStatic<Instant> ignored = mockNow()) {
            actual = assertThrows(RuntimeException.class,
                    () -> job.migrateExpiredProcessedInputHistory());
        }

        assertSame(failure, actual);
        verify(writeLock).lock();
        verify(writeLock).unlock();
    }

    private void runMigrationWithResult(int migratedCount) {
        when(processedInputHistoryMapper.migrateOlderThan(any(Instant.class), eq(BATCH_SIZE)))
                .thenReturn(migratedCount);
        doAnswer(invocation -> {
            TransactionCallback<Integer> callback = invocation.getArgument(0);
            return callback.doInTransaction(null);
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        try (MockedStatic<Instant> ignored = mockNow()) {
            job.migrateExpiredProcessedInputHistory();
        }
    }

    private static MockedStatic<Instant> mockNow() {
        MockedStatic<Instant> instantMock = mockStatic(Instant.class, Answers.CALLS_REAL_METHODS);
        instantMock.when(Instant::now).thenReturn(FIXED_NOW);
        return instantMock;
    }
}
