package com.ebay.challenge.streamprocessor.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * JDBC mapper for the shared per-partition watermark state.
 */
@Repository
@RequiredArgsConstructor
public class WatermarkStateMapper {

    private final JdbcTemplate jdbcTemplate;

    public record WatermarkState(
            int partition,
            Instant maxEventTime,
            String status,
            Instant lastSeenAt,
            Instant updatedAt
    ) {
    }

    private static final RowMapper<WatermarkState> ROW_MAPPER =
            (rs, rowNum) -> new WatermarkState(
                    rs.getInt("partition_no"),
                    Instant.ofEpochMilli(rs.getLong("max_event_time_epoch_ms")),
                    rs.getString("watermark_status"),
                    Instant.ofEpochMilli(rs.getLong("last_seen_at_epoch_ms")),
                    Instant.ofEpochMilli(rs.getLong("updated_at_epoch_ms"))
            );

    public void upsertObserved(int partition, Instant eventTime, Instant observedAt) {
        String sql = """
                INSERT INTO watermark_state (
                    partition_no,
                    max_event_time,
                    max_event_time_epoch_ms,
                    watermark_status,
                    last_seen_at,
                    last_seen_at_epoch_ms,
                    updated_at,
                    updated_at_epoch_ms
                )
                VALUES (?, ?, ?, 'OBSERVED', ?, ?, ?, ?)
                ON CONFLICT(partition_no) DO UPDATE SET
                    max_event_time = CASE
                        WHEN excluded.max_event_time_epoch_ms > watermark_state.max_event_time_epoch_ms
                        THEN excluded.max_event_time
                        ELSE watermark_state.max_event_time
                    END,
                    max_event_time_epoch_ms = CASE
                        WHEN excluded.max_event_time_epoch_ms > watermark_state.max_event_time_epoch_ms
                        THEN excluded.max_event_time_epoch_ms
                        ELSE watermark_state.max_event_time_epoch_ms
                    END,
                    watermark_status = 'OBSERVED',
                    last_seen_at = excluded.last_seen_at,
                    last_seen_at_epoch_ms = excluded.last_seen_at_epoch_ms,
                    updated_at = excluded.updated_at,
                    updated_at_epoch_ms = excluded.updated_at_epoch_ms
                """;

        jdbcTemplate.update(
                sql,
                partition,
                eventTime.toString(),
                eventTime.toEpochMilli(),
                observedAt.toString(),
                observedAt.toEpochMilli(),
                observedAt.toString(),
                observedAt.toEpochMilli()
        );
    }

    public List<WatermarkState> findAll() {
        return jdbcTemplate.query(
                "SELECT * FROM watermark_state ORDER BY partition_no",
                ROW_MAPPER
        );
    }

}
