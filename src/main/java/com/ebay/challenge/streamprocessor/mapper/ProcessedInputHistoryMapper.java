package com.ebay.challenge.streamprocessor.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * JDBC mapper for moving old processed input records into history.
 */
@Repository
@RequiredArgsConstructor
public class ProcessedInputHistoryMapper {

    private final JdbcTemplate jdbcTemplate;

    public record ProcessedInputKey(
            String topic,
            int partition,
            long offset
    ) {
    }

    /**
     * Copies one eligible batch of processed input records to history and
     * removes the same source rows. The caller owns the transaction so both
     * operations commit or roll back together.
     *
     * @param cutoffTime records processed before this time are eligible
     * @param batchSize maximum number of source rows to process
     * @return number of source rows removed from processed_input
     */
    public int migrateOlderThan(Instant cutoffTime, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Processed input history migration batch size must be positive");
        }

        List<ProcessedInputKey> keys = findEligibleKeys(cutoffTime, batchSize);
        if (keys.isEmpty()) {
            return 0;
        }

        Instant archivedAt = Instant.now();

        String copySql = """
                INSERT OR IGNORE INTO processed_input_history (
                    topic,
                    partition_no,
                    offset_no,
                    event_type,
                    event_key,
                    event_time,
                    event_time_epoch_ms,
                    payload_hash,
                    processing_status,
                    attempt_count,
                    received_at,
                    received_at_epoch_ms,
                    processed_at,
                    processed_at_epoch_ms,
                    archived_at,
                    archived_at_epoch_ms
                )
                SELECT
                    topic,
                    partition_no,
                    offset_no,
                    event_type,
                    event_key,
                    event_time,
                    event_time_epoch_ms,
                    payload_hash,
                    processing_status,
                    attempt_count,
                    received_at,
                    received_at_epoch_ms,
                    processed_at,
                    processed_at_epoch_ms,
                    ?,
                    ?
                FROM processed_input
                WHERE (topic, partition_no, offset_no) IN (%s)
                """.formatted(keyPlaceholders(keys.size()));

        jdbcTemplate.update(copySql, copyArguments(archivedAt, keys));

        int historyCount = countHistoryRows(keys);
        if (historyCount != keys.size()) {
            throw new IllegalStateException("Processed input history migration verification failed. "
                    + "Expected " + keys.size() + " rows but found " + historyCount);
        }

        String deleteSql = """
                DELETE FROM processed_input
                WHERE (topic, partition_no, offset_no) IN (%s)
                """.formatted(keyPlaceholders(keys.size()));

        int deletedCount = jdbcTemplate.update(deleteSql, keyArguments(keys));

        if (deletedCount != keys.size()) {
            throw new IllegalStateException("Processed input history migration deletion failed. "
                    + "Expected " + keys.size() + " rows but deleted " + deletedCount);
        }

        return deletedCount;
    }

    private List<ProcessedInputKey> findEligibleKeys(Instant cutoffTime, int batchSize) {
        return jdbcTemplate.query(
                """
                SELECT topic, partition_no, offset_no
                FROM processed_input
                WHERE COALESCE(processed_at_epoch_ms, received_at_epoch_ms) < ?
                ORDER BY COALESCE(processed_at_epoch_ms, received_at_epoch_ms),
                         topic, partition_no, offset_no
                LIMIT ?
                """,
                (rs, rowNum) -> new ProcessedInputKey(
                        rs.getString("topic"),
                        rs.getInt("partition_no"),
                        rs.getLong("offset_no")
                ),
                cutoffTime.toEpochMilli(),
                batchSize
        );
    }

    private int countHistoryRows(List<ProcessedInputKey> keys) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_input_history "
                        + "WHERE (topic, partition_no, offset_no) IN ("
                        + keyPlaceholders(keys.size()) + ")",
                Integer.class,
                keyArguments(keys)
        );
        return count == null ? 0 : count;
    }

    private static String keyPlaceholders(int count) {
        return String.join(",", Collections.nCopies(count, "(?,?,?)"));
    }

    private static Object[] copyArguments(Instant archivedAt, List<ProcessedInputKey> keys) {
        Object[] arguments = new Object[keys.size() * 3 + 2];
        arguments[0] = archivedAt.toString();
        arguments[1] = archivedAt.toEpochMilli();

        int index = 2;
        for (ProcessedInputKey key : keys) {
            arguments[index++] = key.topic();
            arguments[index++] = key.partition();
            arguments[index++] = key.offset();
        }

        return arguments;
    }

    private static Object[] keyArguments(List<ProcessedInputKey> keys) {
        Object[] arguments = new Object[keys.size() * 3];
        int index = 0;
        for (ProcessedInputKey key : keys) {
            arguments[index++] = key.topic();
            arguments[index++] = key.partition();
            arguments[index++] = key.offset();
        }

        return arguments;
    }
}
