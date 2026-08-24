package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * JDBC mapper for page views waiting for watermark finalization.
 */
@Repository
@RequiredArgsConstructor
public class PendingPageViewMapper {

    private final JdbcTemplate jdbcTemplate;

    private static final RowMapper<PageViewEvent> ROW_MAPPER =
            (rs, rowNum) -> {
                PageViewEvent pageView = PageViewEvent.builder()
                        .userId(rs.getString("user_id"))
                        .eventTime(Instant.ofEpochMilli(rs.getLong("event_time_epoch_ms")))
                        .url(rs.getString("url"))
                        .eventId(rs.getString("page_view_id"))
                        .build();

                pageView.setPartition(rs.getInt("partition_no"));
                pageView.setOffset(rs.getLong("offset_no"));
                return pageView;
            };

    public boolean insertIfAbsent(String sourceTopic, PageViewEvent pageView) {
        String sql = """
                INSERT INTO pending_page_view (
                    page_view_id,
                    user_id,
                    event_time,
                    event_time_epoch_ms,
                    url,
                    source_topic,
                    partition_no,
                    offset_no
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(page_view_id) DO NOTHING
                """;

        return jdbcTemplate.update(
                sql,
                pageView.getEventId(),
                pageView.getUserId(),
                pageView.getEventTime().toString(),
                pageView.getEventTime().toEpochMilli(),
                pageView.getUrl(),
                sourceTopic,
                pageView.getPartition(),
                pageView.getOffset()
        ) == 1;
    }

    public Optional<PageViewEvent> findByPageViewId(String pageViewId) {
        String sql = """
                SELECT *
                FROM pending_page_view
                WHERE page_view_id = ?
                  AND pending_status = 'PENDING'
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER, pageViewId)
                .stream()
                .findFirst();
    }

    public List<PageViewEvent> findAllPending() {
        String sql = """
                SELECT *
                FROM pending_page_view
                WHERE pending_status = 'PENDING'
                ORDER BY event_time_epoch_ms, page_view_id
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER);
    }

    public int markEmitted(String pageViewId) {
        return jdbcTemplate.update(
                """
                UPDATE pending_page_view
                SET pending_status = 'EMITTED',
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    updated_at_epoch_ms = CAST(strftime('%s', 'now') AS INTEGER) * 1000
                WHERE page_view_id = ?
                  AND pending_status = 'PENDING'
                """,
                pageViewId
        );
    }

    public int deleteByPageViewId(String pageViewId) {
        return jdbcTemplate.update(
                "DELETE FROM pending_page_view WHERE page_view_id = ?",
                pageViewId
        );
    }
}
