package com.ebay.challenge.streamprocessor.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * JDBC mapper for terminal input errors and idempotency conflicts.
 */
@Repository
@RequiredArgsConstructor
public class DeadLetterEventMapper {

    private final JdbcTemplate jdbcTemplate;

    public record DeadLetterEvent(
            String topic,
            int partition,
            long offset,
            String eventType,
            String eventKey,
            Instant eventTime,
            String payload,
            String errorType,
            String errorMessage,
            int attemptCount,
            Instant createdAt
    ) {
    }

    private static final RowMapper<DeadLetterEvent> ROW_MAPPER =
            (rs, rowNum) -> new DeadLetterEvent(
                    rs.getString("topic"),
                    rs.getInt("partition_no"),
                    rs.getLong("offset_no"),
                    rs.getString("event_type"),
                    rs.getString("event_key"),
                    nullableInstant(rs.getObject("event_time_epoch_ms")),
                    rs.getString("payload"),
                    rs.getString("error_type"),
                    rs.getString("error_message"),
                    rs.getInt("attempt_count"),
                    Instant.ofEpochMilli(rs.getLong("created_at_epoch_ms"))
            );

    public boolean insertIfAbsent(
            String topic,
            int partition,
            long offset,
            String eventType,
            String eventKey,
            Instant eventTime,
            String payload,
            String errorType,
            String errorMessage,
            int attemptCount,
            Instant createdAt
    ) {
        String sql = """
                INSERT INTO dead_letter_event (
                    topic,
                    partition_no,
                    offset_no,
                    event_type,
                    event_key,
                    event_time,
                    event_time_epoch_ms,
                    payload,
                    error_type,
                    error_message,
                    attempt_count,
                    created_at,
                    created_at_epoch_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(topic, partition_no, offset_no) DO NOTHING
                """;

        return jdbcTemplate.update(
                sql,
                topic,
                partition,
                offset,
                eventType,
                eventKey,
                eventTime == null ? null : eventTime.toString(),
                eventTime == null ? null : eventTime.toEpochMilli(),
                payload,
                errorType,
                errorMessage,
                attemptCount,
                createdAt.toString(),
                createdAt.toEpochMilli()
        ) == 1;
    }

    private static Instant nullableInstant(Object value) {
        if (value == null) {
            return null;
        }

        return Instant.ofEpochMilli(((Number) value).longValue());
    }
}
