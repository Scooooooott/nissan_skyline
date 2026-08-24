package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * JDBC mapper for the durable click-state copy.
 */
@Repository
@RequiredArgsConstructor
public class ClickStateMapper {

    private final JdbcTemplate jdbcTemplate;

    public static final String INSERTED = "INSERTED";
    public static final String REPLAY = "REPLAY";
    public static final String DUPLICATE = "DUPLICATE";
    public static final String CONFLICT = "CONFLICT";

    private static final RowMapper<AdClickEvent> ROW_MAPPER =
            (rs, rowNum) -> {
                AdClickEvent click = AdClickEvent.builder()
                        .userId(rs.getString("user_id"))
                        .eventTime(Instant.ofEpochMilli(rs.getLong("event_time_epoch_ms")))
                        .campaignId(rs.getString("campaign_id"))
                        .clickId(rs.getString("click_id"))
                        .build();

                click.setPartition(rs.getInt("partition_no"));
                click.setOffset(rs.getLong("offset_no"));
                return click;
            };

    public String insertIfAbsent(String sourceTopic, AdClickEvent click) {
        String sql = """
                INSERT INTO click_state (
                    click_id,
                    user_id,
                    event_time,
                    event_time_epoch_ms,
                    campaign_id,
                    source_topic,
                    partition_no,
                    offset_no
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(click_id) DO NOTHING
                """;

        int affectedRows = jdbcTemplate.update(
                sql,
                click.getClickId(),
                click.getUserId(),
                click.getEventTime().toString(),
                click.getEventTime().toEpochMilli(),
                click.getCampaignId(),
                sourceTopic,
                click.getPartition(),
                click.getOffset()
        );

        if (affectedRows == 1) {
            return INSERTED;
        }

        Optional<AdClickEvent> existing = findByClickId(click.getClickId());

        if (existing.isEmpty()) {
            throw new IllegalStateException("click_id conflict detected but existing row cannot be loaded: " + click.getClickId());
        }

        AdClickEvent stored = existing.get();

        boolean sameBusinessContent = (stored.getUserId().equals(click.getUserId()))
                && (stored.getEventTime().equals(click.getEventTime()))
                && (stored.getCampaignId().equals(click.getCampaignId()));

        boolean sameKafkaSource = (stored.getPartition() == click.getPartition())
                && (stored.getOffset() == click.getOffset());

        if (sameKafkaSource && sameBusinessContent) {
            return REPLAY;
        }

        if (sameBusinessContent) {
            return DUPLICATE;
        }

        return CONFLICT;
    }

    public Optional<AdClickEvent> findByClickId(String clickId) {
        String sql = """
                SELECT *
                FROM click_state
                WHERE click_id = ?
                  AND state_status = 'ACTIVE'
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER, clickId)
                .stream()
                .findFirst();
    }

    public List<AdClickEvent> findActiveByUserId(String userId) {
        String sql = """
                SELECT *
                FROM click_state
                WHERE user_id = ?
                  AND state_status = 'ACTIVE'
                ORDER BY event_time_epoch_ms, click_id
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER, userId);
    }

    public List<AdClickEvent> findAllActive() {
        String sql = """
                SELECT *
                FROM click_state
                WHERE state_status = 'ACTIVE'
                ORDER BY user_id, event_time_epoch_ms, click_id
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER);
    }

    /**
     * UPDATE evicted clicks in db
     */
    public int markOlderThanEvicted(Instant cutoffTime) {
        Instant updatedAt = Instant.now();
        return jdbcTemplate.update(
                """
                UPDATE click_state
                SET state_status = 'EVICTED',
                    updated_at = ?,
                    updated_at_epoch_ms = ?
                WHERE state_status = 'ACTIVE'
                  AND event_time_epoch_ms < ?
                """,
                updatedAt.toString(),
                updatedAt.toEpochMilli(),
                cutoffTime.toEpochMilli()
        );
    }
}
