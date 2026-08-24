package com.ebay.challenge.streamprocessor.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

/**
 * JDBC mapper for processed_input.
 *
 * The table is the idempotency and terminal-processing ledger. A conflict is
 * represented by processing_status=DEAD_LETTER and is detailed in the
 * dead_letter_event table.
 */
@Repository
@RequiredArgsConstructor
public class ProcessedInputMapper {

    private final JdbcTemplate jdbcTemplate;

    public record ProcessedInput(
            String topic,
            int partition,
            long offset,
            String eventType,
            String eventKey,
            Instant eventTime,
            String processingStatus,
            int attemptCount,
            Instant receivedAt,
            Instant processedAt
    ) {
    }

    private static final RowMapper<ProcessedInput> ROW_MAPPER =
            (rs, rowNum) -> new ProcessedInput(
                    rs.getString("topic"),
                    rs.getInt("partition_no"),
                    rs.getLong("offset_no"),
                    rs.getString("event_type"),
                    rs.getString("event_key"),
                    nullableInstant(rs, "event_time_epoch_ms"),
                    rs.getString("processing_status"),
                    rs.getInt("attempt_count"),
                    instant(rs, "received_at_epoch_ms"),
                    nullableInstant(rs, "processed_at_epoch_ms")
            );

    public boolean insertTerminalRecord(
            String topic,
            int partition,
            long offset,
            String eventType,
            String eventKey,
            Instant eventTime,
            String processingStatus,
            Instant receivedAt,
            Instant processedAt
    ) {
        String sql = """
                INSERT INTO processed_input (
                    topic,
                    partition_no,
                    offset_no,
                    event_type,
                    event_key,
                    event_time,
                    event_time_epoch_ms,
                    processing_status,
                    received_at,
                    received_at_epoch_ms,
                    processed_at,
                    processed_at_epoch_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(topic, partition_no, offset_no) DO NOTHING
                """;

        return jdbcTemplate.update(
                sql,
                topic,
                partition,
                offset,
                eventType,
                eventKey,
                nullableText(eventTime),
                nullableEpochMillis(eventTime),
                processingStatus,
                receivedAt.toString(),
                receivedAt.toEpochMilli(),
                nullableText(processedAt),
                nullableEpochMillis(processedAt)
        ) == 1;
    }

    public Optional<ProcessedInput> findByOffset(String topic, int partition, long offset) {
        String sql = """
                SELECT *
                FROM processed_input
                WHERE topic = ?
                  AND partition_no = ?
                  AND offset_no = ?
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER, topic, partition, offset)
                .stream()
                .findFirst();
    }

    public Optional<ProcessedInput> findByEventKey(String topic, String eventKey) {
        String sql = """
                SELECT *
                FROM processed_input
                WHERE topic = ?
                  AND event_key = ?
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER, topic, eventKey)
                .stream()
                .findFirst();
    }

    private static Instant nullableInstant(ResultSet rs, String column) throws SQLException {
        long epochMillis = rs.getLong(column);
        return rs.wasNull() ? null : Instant.ofEpochMilli(epochMillis);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return Instant.ofEpochMilli(rs.getLong(column));
    }

    private static String nullableText(Instant instant) {
        return instant == null ? null : instant.toString();
    }

    private static Long nullableEpochMillis(Instant instant) {
        return instant == null ? null : instant.toEpochMilli();
    }


    public void insertLateRecord(String topic, int partition, long offset,
                                    String eventType, String eventKey, Instant eventTime) {
        Instant processedAt = Instant.now();

        insertTerminalRecord(topic, partition, offset, eventType,
                eventKey, eventTime, "DROPPED_LATE", processedAt, processedAt );
    }

    /**
     * Insert PROCESSED records
     */
    public void insertProcessedRecord(String topic, int partition, long offset,
                                      String eventType, String eventKey, Instant eventTime) {
        Instant processedAt = Instant.now();

        insertTerminalRecord(topic, partition, offset, eventType,
                eventKey, eventTime, "PROCESSED", processedAt, processedAt);
    }

}
