package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.sql.Connection;
import java.time.Instant;
import java.util.UUID;

abstract class MapperTestSupport {

    protected static final Instant BASE_TIME = Instant.parse("2026-08-24T10:00:00Z");
    protected JdbcTemplate jdbcTemplate;

    private Connection keepAliveConnection;

    @BeforeEach
    void setUpDatabase() throws Exception {
        String databaseName = "mapper_test_" + UUID.randomUUID().toString().replace("-", "");
        String url = "jdbc:sqlite:file:" + databaseName + "?mode=memory&cache=shared";
        DriverManagerDataSource dataSource = new DriverManagerDataSource(url);
        keepAliveConnection = dataSource.getConnection();
        ScriptUtils.executeSqlScript(keepAliveConnection, new ClassPathResource("schema.sql"));
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @AfterEach
    void tearDownDatabase() throws Exception {
        if (keepAliveConnection != null) {
            keepAliveConnection.close();
        }
    }

    protected AdClickEvent click(String clickId, String userId, Instant eventTime,
                                 String campaignId, int partition, long offset) {
        AdClickEvent result = AdClickEvent.builder()
                .clickId(clickId)
                .userId(userId)
                .eventTime(eventTime)
                .campaignId(campaignId)
                .build();
        result.setPartition(partition);
        result.setOffset(offset);
        return result;
    }

    protected PageViewEvent pageView(String pageViewId, String userId, Instant eventTime,
                                     String url, int partition, long offset) {
        PageViewEvent result = PageViewEvent.builder()
                .eventId(pageViewId)
                .userId(userId)
                .eventTime(eventTime)
                .url(url)
                .build();
        result.setPartition(partition);
        result.setOffset(offset);
        return result;
    }

    protected AttributedPageView attributedPageView(String pageViewId, String userId,
                                                    Instant eventTime, String url,
                                                    String campaignId, String clickId) {
        return AttributedPageView.builder()
                .pageViewId(pageViewId)
                .userId(userId)
                .eventTime(eventTime)
                .url(url)
                .attributedCampaignId(campaignId)
                .attributedClickId(clickId)
                .build();
    }

    protected Instant plusMinutes(long minutes) {
        return BASE_TIME.plusSeconds(minutes * 60L);
    }

    protected Instant minusMinutes(long minutes) {
        return BASE_TIME.minusSeconds(minutes * 60L);
    }

    protected void insertClickState(AdClickEvent click, String status) {
        jdbcTemplate.update("""
                INSERT INTO click_state (
                    click_id, user_id, event_time, event_time_epoch_ms, campaign_id,
                    source_topic, partition_no, offset_no, state_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                click.getClickId(), click.getUserId(), click.getEventTime().toString(),
                click.getEventTime().toEpochMilli(), click.getCampaignId(),
                "ad_clicks", click.getPartition(), click.getOffset(), status);
    }

    protected void insertClickHistory(AdClickEvent click) {
        jdbcTemplate.update("""
                INSERT INTO click_history (
                    click_id, user_id, event_time, event_time_epoch_ms, campaign_id,
                    source_topic, partition_no, offset_no, archived_at, archived_at_epoch_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                click.getClickId(), click.getUserId(), click.getEventTime().toString(),
                click.getEventTime().toEpochMilli(), click.getCampaignId(),
                "ad_clicks", click.getPartition(), click.getOffset(),
                BASE_TIME.toString(), BASE_TIME.toEpochMilli());
    }

    protected void insertPendingPageView(PageViewEvent pageView, String status) {
        jdbcTemplate.update("""
                INSERT INTO pending_page_view (
                    page_view_id, user_id, event_time, event_time_epoch_ms, url,
                    source_topic, partition_no, offset_no, pending_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                pageView.getEventId(), pageView.getUserId(), pageView.getEventTime().toString(),
                pageView.getEventTime().toEpochMilli(), pageView.getUrl(), "page_views",
                pageView.getPartition(), pageView.getOffset(), status);
    }

    protected void insertOutput(AttributedPageView output) {
        jdbcTemplate.update("""
                INSERT INTO attributed_page_view (
                    page_view_id, user_id, event_time, event_time_epoch_ms, url,
                    attributed_campaign_id, attributed_click_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                output.getPageViewId(), output.getUserId(), output.getEventTime().toString(),
                output.getEventTime().toEpochMilli(), output.getUrl(),
                output.getAttributedCampaignId(), output.getAttributedClickId());
    }

    protected void insertProcessedInput(String topic, int partition, long offset,
                                         String eventType, String eventKey, Instant eventTime,
                                         String processingStatus, int attemptCount,
                                         Instant receivedAt, Instant processedAt) {
        jdbcTemplate.update("""
                INSERT INTO processed_input (
                    topic, partition_no, offset_no, event_type, event_key,
                    event_time, event_time_epoch_ms, payload_hash, processing_status,
                    attempt_count, received_at, received_at_epoch_ms,
                    processed_at, processed_at_epoch_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                topic, partition, offset, eventType, eventKey,
                eventTime == null ? null : eventTime.toString(),
                eventTime == null ? null : eventTime.toEpochMilli(),
                "hash-1", processingStatus, attemptCount,
                receivedAt.toString(), receivedAt.toEpochMilli(),
                processedAt == null ? null : processedAt.toString(),
                processedAt == null ? null : processedAt.toEpochMilli());
    }

    protected void insertProcessedInputHistory(String topic, int partition, long offset,
                                                String eventType, String eventKey,
                                                Instant eventTime, String processingStatus,
                                                int attemptCount, Instant receivedAt,
                                                Instant processedAt) {
        jdbcTemplate.update("""
                INSERT INTO processed_input_history (
                    topic, partition_no, offset_no, event_type, event_key,
                    event_time, event_time_epoch_ms, payload_hash, processing_status,
                    attempt_count, received_at, received_at_epoch_ms,
                    processed_at, processed_at_epoch_ms,
                    archived_at, archived_at_epoch_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                topic, partition, offset, eventType, eventKey,
                eventTime == null ? null : eventTime.toString(),
                eventTime == null ? null : eventTime.toEpochMilli(),
                "hash-1", processingStatus, attemptCount,
                receivedAt.toString(), receivedAt.toEpochMilli(),
                processedAt == null ? null : processedAt.toString(),
                processedAt == null ? null : processedAt.toEpochMilli(),
                BASE_TIME.toString(), BASE_TIME.toEpochMilli());
    }

    protected void insertDeadLetter(String topic, int partition, long offset,
                                    String eventType, String eventKey, Instant eventTime,
                                    String payload, String errorType, String errorMessage,
                                    int attemptCount, Instant createdAt) {
        jdbcTemplate.update("""
                INSERT INTO dead_letter_event (
                    topic, partition_no, offset_no, event_type, event_key,
                    event_time, event_time_epoch_ms, payload, error_type,
                    error_message, attempt_count, created_at, created_at_epoch_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                topic, partition, offset, eventType, eventKey,
                eventTime == null ? null : eventTime.toString(),
                eventTime == null ? null : eventTime.toEpochMilli(),
                payload, errorType, errorMessage, attemptCount,
                createdAt.toString(), createdAt.toEpochMilli());
    }

    protected void insertWatermark(int partition, Instant maxEventTime, String status,
                                   Instant lastSeenAt, Instant updatedAt) {
        jdbcTemplate.update("""
                INSERT INTO watermark_state (
                    partition_no, max_event_time, max_event_time_epoch_ms,
                    watermark_status, last_seen_at, last_seen_at_epoch_ms,
                    updated_at, updated_at_epoch_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                partition, maxEventTime.toString(), maxEventTime.toEpochMilli(), status,
                lastSeenAt.toString(), lastSeenAt.toEpochMilli(),
                updatedAt.toString(), updatedAt.toEpochMilli());
    }
}
