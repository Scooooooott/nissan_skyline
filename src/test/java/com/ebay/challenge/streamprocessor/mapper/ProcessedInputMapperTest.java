package com.ebay.challenge.streamprocessor.mapper;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessedInputMapperTest extends MapperTestSupport {

    private ProcessedInputMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new ProcessedInputMapper(jdbcTemplate);
    }

    @Test
    void insertTerminalRecord_whenRecordIsNew_usesDefaultAttemptCount() {
        assertTrue(mapper.insertTerminalRecord(
                "page_views", 1, 10L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", BASE_TIME, BASE_TIME));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM processed_input", Integer.class));
    }

    @Test
    void insertTerminalRecord_whenCustomAttemptCountIsProvided_persistsIt() {
        assertTrue(mapper.insertTerminalRecord(
                "page_views", 1, 10L, "page_views", "pv-1", BASE_TIME,
                "DEAD_LETTER", BASE_TIME, BASE_TIME, 4));
        assertEquals(4, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM processed_input", Integer.class));
    }

    @Test
    void insertTerminalRecord_preservesNullableEventTimeAndProcessedAt() {
        assertTrue(mapper.insertTerminalRecord(
                "ad_clicks", 1, 10L, "ad_clicks", null, null,
                "DEAD_LETTER", BASE_TIME, null));

        assertNull(jdbcTemplate.queryForObject(
                "SELECT event_time_epoch_ms FROM processed_input", Long.class));
        assertNull(jdbcTemplate.queryForObject(
                "SELECT processed_at_epoch_ms FROM processed_input", Long.class));
    }

    @Test
    void insertTerminalRecord_whenSameOffsetExists_returnsFalse() {
        insertProcessedInput("page_views", 1, 10L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, BASE_TIME, BASE_TIME);

        assertEquals(false, mapper.insertTerminalRecord(
                "page_views", 1, 10L, "page_views", "pv-1", BASE_TIME,
                "DEAD_LETTER", plusMinutes(1), plusMinutes(1), 4));
    }

    @Test
    void insertTerminalRecord_doesNotOverwriteExistingStatus() {
        insertProcessedInput("page_views", 1, 10L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 1, BASE_TIME, BASE_TIME);
        mapper.insertTerminalRecord("page_views", 1, 10L, "page_views", "pv-1", BASE_TIME,
                "DEAD_LETTER", plusMinutes(1), plusMinutes(1), 4);

        assertEquals("PROCESSED", jdbcTemplate.queryForObject(
                "SELECT processing_status FROM processed_input", String.class));
    }

    @Test
    void findByOffset_whenRecordExists_mapsAllFields() {
        insertProcessedInput("page_views", 3, 30L, "page_views", "pv-1", BASE_TIME,
                "PROCESSED", 2, minusMinutes(1), BASE_TIME);

        ProcessedInputMapper.ProcessedInput result = mapper.findByOffset("page_views", 3, 30L).orElseThrow();

        assertEquals("page_views", result.topic());
        assertEquals(3, result.partition());
        assertEquals(30L, result.offset());
        assertEquals("pv-1", result.eventKey());
        assertEquals(BASE_TIME, result.eventTime());
        assertEquals("PROCESSED", result.processingStatus());
        assertEquals(2, result.attemptCount());
        assertEquals(minusMinutes(1), result.receivedAt());
        assertEquals(BASE_TIME, result.processedAt());
    }

    @Test
    void findByOffset_whenRecordDoesNotExist_returnsEmpty() {
        assertTrue(mapper.findByOffset("page_views", 1, 1L).isEmpty());
    }

    @Test
    void findByOffset_mapsNullEventTime() {
        insertProcessedInput("ad_clicks", 1, 10L, "ad_clicks", null, null,
                "DEAD_LETTER", 1, BASE_TIME, BASE_TIME);

        assertNull(mapper.findByOffset("ad_clicks", 1, 10L).orElseThrow().eventTime());
    }

    @Test
    void findByOffset_mapsNullProcessedAt() {
        insertProcessedInput("ad_clicks", 1, 10L, "ad_clicks", null, BASE_TIME,
                "DEAD_LETTER", 1, BASE_TIME, null);

        assertNull(mapper.findByOffset("ad_clicks", 1, 10L).orElseThrow().processedAt());
    }

    @Test
    void insertLateRecord_persistsDroppedLateStatus() {
        mapper.insertLateRecord("page_views", 1, 10L, "page_views", "pv-1", BASE_TIME);

        assertEquals("DROPPED_LATE", jdbcTemplate.queryForObject(
                "SELECT processing_status FROM processed_input", String.class));
    }

    @Test
    void insertProcessedRecord_persistsProcessedStatus() {
        mapper.insertProcessedRecord("page_views", 1, 10L, "page_views", "pv-1", BASE_TIME);

        assertEquals("PROCESSED", jdbcTemplate.queryForObject(
                "SELECT processing_status FROM processed_input", String.class));
    }

    @Test
    void insertDeadLetterRecord_persistsDeadLetterStatusAndAttemptCount() {
        mapper.insertDeadLetterRecord("ad_clicks", 1, 10L, "ad_clicks", "click-1", BASE_TIME, 4);

        assertEquals("DEAD_LETTER", jdbcTemplate.queryForObject(
                "SELECT processing_status FROM processed_input", String.class));
        assertEquals(4, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM processed_input", Integer.class));
    }
}
