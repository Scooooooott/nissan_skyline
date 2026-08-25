package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
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

    public record StoredClick(AdClickEvent click, String stateStatus) {
    }

    private static final RowMapper<AdClickEvent> ROW_MAPPER =
            (rs, rowNum) -> mapClick(rs);

    private static final RowMapper<StoredClick> STATE_ROW_MAPPER =
            (rs, rowNum) -> new StoredClick(mapClick(rs), rs.getString("state_status"));

    private static final RowMapper<StoredClick> HISTORY_ROW_MAPPER =
            (rs, rowNum) -> new StoredClick(mapClick(rs), "ARCHIVED");

    public String insertIfAbsent(String sourceTopic, AdClickEvent click) {
        Optional<StoredClick> existing = findByClickIdIncludingInactive(click.getClickId());
        if (existing.isPresent()) {
            return classify(existing.get().click(), click);
        }

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

        existing = findByClickIdIncludingInactive(click.getClickId());

        if (existing.isEmpty()) {
            throw new IllegalStateException("click_id conflict detected but existing row cannot be loaded: " + click.getClickId());
        }

        return classify(existing.get().click(), click);
    }

    public Optional<String> classifyExisting(AdClickEvent click) {
        return findByClickIdIncludingInactive(click.getClickId())
                .map(existing -> classify(existing.click(), click));
    }

    private String classify(AdClickEvent stored, AdClickEvent incoming) {
        boolean sameBusinessContent = Objects.equals(stored.getUserId(), incoming.getUserId())
                && Objects.equals(stored.getEventTime(), incoming.getEventTime())
                && Objects.equals(stored.getCampaignId(), incoming.getCampaignId());

        boolean sameKafkaSource = (stored.getPartition() == incoming.getPartition())
                && (stored.getOffset() == incoming.getOffset());

        if (sameKafkaSource && sameBusinessContent) {
            return REPLAY;
        }

        if (sameBusinessContent) {
            return DUPLICATE;
        }

        return CONFLICT;
    }

    public Optional<StoredClick> findByClickIdIncludingInactive(String clickId) {
        String stateSql = """
                SELECT *
                FROM click_state
                WHERE click_id = ?
                """;

        List<StoredClick> stateRows = jdbcTemplate.query(stateSql, STATE_ROW_MAPPER, clickId);
        if (!stateRows.isEmpty()) {
            return Optional.of(stateRows.get(0));
        }

        String historySql = """
                SELECT *
                FROM click_history
                WHERE click_id = ?
                """;

        return jdbcTemplate.query(historySql, HISTORY_ROW_MAPPER, clickId)
                .stream()
                .findFirst();
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

    private static AdClickEvent mapClick(ResultSet rs) throws SQLException {
        AdClickEvent click = AdClickEvent.builder()
                .userId(rs.getString("user_id"))
                .eventTime(Instant.ofEpochMilli(rs.getLong("event_time_epoch_ms")))
                .campaignId(rs.getString("campaign_id"))
                .clickId(rs.getString("click_id"))
                .build();

        click.setPartition(rs.getInt("partition_no"));
        click.setOffset(rs.getLong("offset_no"));
        return click;
    }
}
