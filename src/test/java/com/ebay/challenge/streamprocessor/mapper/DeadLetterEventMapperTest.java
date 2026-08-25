package com.ebay.challenge.streamprocessor.mapper;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeadLetterEventMapperTest extends MapperTestSupport {

    private DeadLetterEventMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new DeadLetterEventMapper(jdbcTemplate);
    }

    @Test
    void insertIfAbsent_whenRecordIsNew_insertsAndReturnsTrue() {
        assertEquals(true, mapper.insertIfAbsent(
                "ad_clicks", 1, 10L, "ad_clicks", "click-1", BASE_TIME,
                "{payload}", "INVALID_KAFKA_RECORD", "invalid", 1, BASE_TIME));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM dead_letter_event", Integer.class));
    }

    @Test
    void insertIfAbsent_whenSameTopicPartitionOffsetExists_returnsFalse() {
        insertDeadLetter("ad_clicks", 1, 10L, "ad_clicks", "click-1", BASE_TIME,
                "first", "ERROR_A", "first error", 1, BASE_TIME);

        assertEquals(false, mapper.insertIfAbsent(
                "ad_clicks", 1, 10L, "ad_clicks", "click-1", plusMinutes(1),
                "second", "ERROR_B", "second error", 9, plusMinutes(1)));
        assertEquals("first", jdbcTemplate.queryForObject(
                "SELECT payload FROM dead_letter_event WHERE topic = ? AND partition_no = ? AND offset_no = ?",
                String.class, "ad_clicks", 1, 10L));
    }

    @Test
    void insertIfAbsent_preservesAllDeadLetterFields() {
        Instant createdAt = BASE_TIME.plusSeconds(5);

        mapper.insertIfAbsent("page_views", 2, 20L, "page_views", "pv-1", BASE_TIME,
                "payload", "PROCESSING_RETRY_EXHAUSTED", "database failed", 4, createdAt);

        var row = jdbcTemplate.queryForList("SELECT * FROM dead_letter_event").getFirst();
        assertEquals("page_views", row.get("topic"));
        assertEquals(2, row.get("partition_no"));
        assertEquals(20L, ((Number) row.get("offset_no")).longValue());
        assertEquals("pv-1", row.get("event_key"));
        assertEquals(BASE_TIME.toEpochMilli(), row.get("event_time_epoch_ms"));
        assertEquals("payload", row.get("payload"));
        assertEquals("PROCESSING_RETRY_EXHAUSTED", row.get("error_type"));
        assertEquals("database failed", row.get("error_message"));
        assertEquals(4, row.get("attempt_count"));
        assertEquals(createdAt.toEpochMilli(), row.get("created_at_epoch_ms"));
    }

    @Test
    void insertIfAbsent_allowsNullEventTime() {
        mapper.insertIfAbsent("ad_clicks", 1, 10L, "ad_clicks", null, null,
                "bad-json", "INVALID_KAFKA_RECORD", "parse failed", 1, BASE_TIME);

        assertEquals(null, jdbcTemplate.queryForObject(
                "SELECT event_time_epoch_ms FROM dead_letter_event", Long.class));
    }

    @Test
    void insertIfAbsent_allowsNullPayloadAndEventMetadata() {
        mapper.insertIfAbsent("unknown", 1, 10L, null, null, null,
                null, "UNKNOWN", "unsupported", 1, BASE_TIME);

        var row = jdbcTemplate.queryForList("SELECT * FROM dead_letter_event").getFirst();
        assertEquals(null, row.get("event_type"));
        assertEquals(null, row.get("event_key"));
        assertEquals(null, row.get("payload"));
    }

    @Test
    void insertIfAbsent_preservesAttemptCountAndCreatedAt() {
        Instant createdAt = plusMinutes(2);

        mapper.insertIfAbsent("ad_clicks", 1, 11L, "ad_clicks", "click-2", BASE_TIME,
                "payload", "RETRY", "failed", 7, createdAt);

        assertEquals(7, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM dead_letter_event", Integer.class));
        assertEquals(createdAt.toEpochMilli(), jdbcTemplate.queryForObject(
                "SELECT created_at_epoch_ms FROM dead_letter_event", Long.class));
    }
}
