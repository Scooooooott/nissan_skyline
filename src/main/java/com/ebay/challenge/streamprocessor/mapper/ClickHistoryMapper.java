package com.ebay.challenge.streamprocessor.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * JDBC mapper for moving non-active click state into click history.
 */
@Repository
@RequiredArgsConstructor
public class ClickHistoryMapper {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Copies one eligible batch of non-active click state to history and
     * removes the same source rows. The caller owns the transaction so both
     * operations commit or roll back together.
     *
     * @param cutoffTime clicks older than this event time are eligible
     * @param batchSize maximum number of source rows to process
     * @return number of source rows removed from click_state
     */
    public int migrateNonActiveOlderThan(Instant cutoffTime, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Click history migration batch size must be positive");
        }

        List<String> clickIds = findNonActiveClickIdsOlderThan(cutoffTime, batchSize);
        if (clickIds.isEmpty()) {
            return 0;
        }

        Instant archivedAt = Instant.now();

        String copySql = """
                INSERT OR IGNORE INTO click_history (
                    click_id,
                    user_id,
                    event_time,
                    event_time_epoch_ms,
                    campaign_id,
                    source_topic,
                    partition_no,
                    offset_no,
                    created_at,
                    created_at_epoch_ms,
                    updated_at,
                    updated_at_epoch_ms,
                    archived_at,
                    archived_at_epoch_ms
                )
                SELECT
                    click_id,
                    user_id,
                    event_time,
                    event_time_epoch_ms,
                    campaign_id,
                    source_topic,
                    partition_no,
                    offset_no,
                    created_at,
                    created_at_epoch_ms,
                    updated_at,
                    updated_at_epoch_ms,
                    ?,
                    ?
                FROM click_state
                WHERE click_id IN (%s)
                  AND state_status <> 'ACTIVE'
                """.formatted(placeholders(clickIds.size()));

        jdbcTemplate.update(copySql, copyArguments(archivedAt, clickIds));

        int historyCount = countHistoryRows(clickIds);
        if (historyCount != clickIds.size()) {
            throw new IllegalStateException("Click history migration verification failed. "
                    + "Expected " + clickIds.size() + " rows but found " + historyCount);
        }

        // The current verification confirms click_id coverage only.
        String deleteSql = """
                DELETE FROM click_state
                WHERE click_id IN (%s)
                  AND state_status <> 'ACTIVE'
                """.formatted(placeholders(clickIds.size()));

        int deletedCount = jdbcTemplate.update(deleteSql, clickIds.toArray());

        if (deletedCount != clickIds.size()) {
            throw new IllegalStateException("Click history migration deletion failed. "
                    + "Expected " + clickIds.size() + " rows but deleted " + deletedCount);
        }

        return deletedCount;
    }

    private List<String> findNonActiveClickIdsOlderThan(Instant cutoffTime, int batchSize) {
        return jdbcTemplate.query(
                """
                SELECT click_id
                FROM click_state
                WHERE state_status <> 'ACTIVE'
                  AND event_time_epoch_ms < ?
                ORDER BY event_time_epoch_ms, click_id
                LIMIT ?
                """,
                (rs, rowNum) -> rs.getString("click_id"),
                cutoffTime.toEpochMilli(),
                batchSize
        );
    }

    private int countHistoryRows(List<String> clickIds) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM click_history WHERE click_id IN (" + placeholders(clickIds.size()) + ")",
                Integer.class,
                clickIds.toArray()
        );
        return count == null ? 0 : count;
    }

    private static String placeholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    private static Object[] copyArguments(Instant archivedAt, List<String> clickIds) {
        Object[] arguments = new Object[clickIds.size() + 2];
        arguments[0] = archivedAt.toString();
        arguments[1] = archivedAt.toEpochMilli();
        for (int i = 0; i < clickIds.size(); i++) {
            arguments[i + 2] = clickIds.get(i);
        }
        return arguments;
    }
}
