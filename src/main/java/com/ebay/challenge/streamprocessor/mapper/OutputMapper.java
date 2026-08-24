package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Mapper for table attributed_page_view
 */
@Repository
@RequiredArgsConstructor
public class OutputMapper {

    private final JdbcTemplate jdbcTemplate;

    private static final RowMapper<AttributedPageView> ROW_MAPPER =
            (rs, rowNum) -> AttributedPageView.builder()
                    .pageViewId(rs.getString("page_view_id"))
                    .userId(rs.getString("user_id"))
                    .eventTime(Instant.ofEpochMilli(rs.getLong("event_time_epoch_ms")))
                    .url(rs.getString("url"))
                    .attributedCampaignId(rs.getString("attributed_campaign_id"))
                    .attributedClickId(rs.getString("attributed_click_id"))
                    .build();

    public boolean insertIfAbsent(AttributedPageView record) {
        String sql = """
                INSERT INTO attributed_page_view (
                    page_view_id,
                    user_id,
                    event_time,
                    event_time_epoch_ms,
                    url,
                    attributed_campaign_id,
                    attributed_click_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(page_view_id) DO NOTHING
                """;

        int affectedRows = jdbcTemplate.update(
                sql,
                record.getPageViewId(),
                record.getUserId(),
                record.getEventTime().toString(),
                record.getEventTime().toEpochMilli(),
                record.getUrl(),
                record.getAttributedCampaignId(),
                record.getAttributedClickId()
        );

        return affectedRows == 1;
    }

    public AttributedPageView findByPageViewId(String pageViewId) {
        String sql = """
                SELECT
                    page_view_id,
                    user_id,
                    event_time,
                    event_time_epoch_ms,
                    url,
                    attributed_campaign_id,
                    attributed_click_id
                FROM attributed_page_view
                WHERE page_view_id = ?
                """;

        List<AttributedPageView> results =
                jdbcTemplate.query(sql, ROW_MAPPER, pageViewId);

        if (CollectionUtils.isEmpty(results)){
            return null;
        }

        return results.getFirst();
    }

    public List<AttributedPageView> findAll() {
        String sql = """
                SELECT
                    page_view_id,
                    user_id,
                    event_time,
                    event_time_epoch_ms,
                    url,
                    attributed_campaign_id,
                    attributed_click_id
                FROM attributed_page_view
                ORDER BY event_time_epoch_ms, page_view_id
                """;

        return jdbcTemplate.query(sql, ROW_MAPPER);
    }

    public long count() {
        Long result = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM attributed_page_view",
                Long.class
        );

        return result == null ? 0L : result;
    }
}
