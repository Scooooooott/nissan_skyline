package com.ebay.challenge.streamprocessor.mapper;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProcessedInputHistoryMapperTest extends MapperTestSupport {

    private ProcessedInputHistoryMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new ProcessedInputHistoryMapper(jdbcTemplate);
    }

    @Test
    void migrateOlderThan_whenBatchSizeIsInvalid_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> mapper.migrateOlderThan(BASE_TIME, 0));
        assertThrows(IllegalArgumentException.class,
                () -> mapper.migrateOlderThan(BASE_TIME, -1));
    }

    @Test
    void migrateOlderThan_whenNoEligibleRecord_returnsZero() {
        insertProcessedInput("page_views", 1, 1L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, BASE_TIME, BASE_TIME);

        assertEquals(0, mapper.migrateOlderThan(BASE_TIME, 10));
    }

    @Test
    void migrateOlderThan_usesProcessedAtForCutoff() {
        insertProcessedInput("page_views", 1, 1L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, plusMinutes(10), minusMinutes(1));

        assertEquals(1, mapper.migrateOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_input_history", Integer.class));
    }

    @Test
    void migrateOlderThan_usesReceivedAtWhenProcessedAtIsNull() {
        insertProcessedInput("page_views", 1, 1L, "page_views", "pv-1", BASE_TIME,
                "DEAD_LETTER", 1, minusMinutes(1), null);

        assertEquals(1, mapper.migrateOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_input_history", Integer.class));
    }

    @Test
    void migrateOlderThan_excludesRecordAtExactCutoff() {
        insertProcessedInput("page_views", 1, 1L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, BASE_TIME, BASE_TIME);

        assertEquals(0, mapper.migrateOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_input", Integer.class));
    }

    @Test
    void migrateOlderThan_respectsBatchSize() {
        for (int i = 0; i < 3; i++) {
            insertProcessedInput("page_views", 1, i + 1L, "page_views", "pv-" + i,
                    BASE_TIME, "PROCESSED", 1, minusMinutes(i + 1), minusMinutes(i + 1));
        }

        assertEquals(2, mapper.migrateOlderThan(BASE_TIME, 2));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_input", Integer.class));
    }

    @Test
    void migrateOlderThan_ordersByProcessingTimeAndCompositeKey() {
        insertProcessedInput("page_views", 2, 1L, "page_views", "pv-page", BASE_TIME,
                "PROCESSED", 1, minusMinutes(1), minusMinutes(1));
        insertProcessedInput("ad_clicks", 1, 2L, "ad_clicks", "click-2", BASE_TIME,
                "PROCESSED", 1, minusMinutes(1), minusMinutes(1));
        insertProcessedInput("ad_clicks", 1, 1L, "ad_clicks", "click-1", BASE_TIME,
                "PROCESSED", 1, minusMinutes(1), minusMinutes(1));

        assertEquals(2, mapper.migrateOlderThan(BASE_TIME, 2));
        assertEquals(List.of("click-1", "click-2"), jdbcTemplate.query(
                "SELECT event_key FROM processed_input_history ORDER BY topic, partition_no, offset_no",
                (rs, rowNum) -> rs.getString("event_key")));
    }

    @Test
    void migrateOlderThan_copiesAllFieldsToHistory() {
        insertProcessedInput("page_views", 3, 30L, "page_views", "pv-1", BASE_TIME,
                "DEAD_LETTER", 4, minusMinutes(2), minusMinutes(1));

        mapper.migrateOlderThan(BASE_TIME, 10);

        var row = jdbcTemplate.queryForList("SELECT * FROM processed_input_history").getFirst();
        assertEquals("page_views", row.get("topic"));
        assertEquals(3, row.get("partition_no"));
        assertEquals(30L, ((Number) row.get("offset_no")).longValue());
        assertEquals("pv-1", row.get("event_key"));
        assertEquals("hash-1", row.get("payload_hash"));
        assertEquals("DEAD_LETTER", row.get("processing_status"));
        assertEquals(4, row.get("attempt_count"));
        assertEquals(minusMinutes(1).toEpochMilli(), row.get("processed_at_epoch_ms"));
        long archivedAt = ((Number) row.get("archived_at_epoch_ms")).longValue();
        assertEquals(true, archivedAt > 0L);
    }

    @Test
    void migrateOlderThan_deletesMigratedSourceRows() {
        insertProcessedInput("page_views", 1, 1L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, minusMinutes(1), minusMinutes(1));

        mapper.migrateOlderThan(BASE_TIME, 10);

        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_input", Integer.class));
    }

    @Test
    void migrateOlderThan_whenHistoryAlreadyContainsKey_remainsIdempotent() {
        insertProcessedInput("page_views", 1, 1L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, minusMinutes(1), minusMinutes(1));
        insertProcessedInputHistory("page_views", 1, 1L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, minusMinutes(1), minusMinutes(1));

        assertEquals(1, mapper.migrateOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_input_history", Integer.class));
    }

    @Test
    void migrateOlderThan_whenHistoryCountMismatches_throwsException() {
        JdbcTemplate mockedJdbcTemplate = mock(JdbcTemplate.class);
        ProcessedInputHistoryMapper mockedMapper = new ProcessedInputHistoryMapper(mockedJdbcTemplate);
        ProcessedInputHistoryMapper.ProcessedInputKey key =
                new ProcessedInputHistoryMapper.ProcessedInputKey("page_views", 1, 1L);

        when(mockedJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of(key));
        when(mockedJdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(Object[].class)))
                .thenReturn(0);

        assertThrows(IllegalStateException.class,
                () -> mockedMapper.migrateOlderThan(BASE_TIME, 10));
    }

    @Test
    void migrateOlderThan_whenDeleteCountMismatches_throwsException() {
        JdbcTemplate mockedJdbcTemplate = mock(JdbcTemplate.class);
        ProcessedInputHistoryMapper mockedMapper = new ProcessedInputHistoryMapper(mockedJdbcTemplate);
        ProcessedInputHistoryMapper.ProcessedInputKey key =
                new ProcessedInputHistoryMapper.ProcessedInputKey("page_views", 1, 1L);

        when(mockedJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of(key));
        when(mockedJdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(Object[].class)))
                .thenReturn(1);
        when(mockedJdbcTemplate.update(anyString(), any(Object[].class)))
                .thenReturn(1, 0);

        assertThrows(IllegalStateException.class,
                () -> mockedMapper.migrateOlderThan(BASE_TIME, 10));
    }
}
