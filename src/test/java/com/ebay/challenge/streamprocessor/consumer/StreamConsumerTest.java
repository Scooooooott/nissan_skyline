package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.mapper.ClickStateMapper;
import com.ebay.challenge.streamprocessor.mapper.DeadLetterEventMapper;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputMapper;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamConsumerTest {

    private static final String AD_CLICK_TOPIC = "ad_clicks";
    private static final String PAGE_VIEW_TOPIC = "page_views";
    private static final String AD_CLICK_PAYLOAD = "{\"click_id\":\"click-1\"}";
    private static final String PAGE_VIEW_PAYLOAD = "{\"event_id\":\"event-1\"}";
    private static final int PARTITION = 2;
    private static final long OFFSET = 42L;
    private static final Instant EVENT_TIME = Instant.parse("2026-08-24T10:00:00Z");

    @Mock
    private JoinEngine joinEngine;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private DeadLetterEventMapper deadLetterEventMapper;

    @Mock
    private ProcessedInputMapper processedInputMapper;

    @Mock
    private Acknowledgment acknowledgment;

    private StreamConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new StreamConsumer(joinEngine, objectMapper, deadLetterEventMapper, processedInputMapper);
    }

    // Valid ad click, process successfully and acknowledge the Kafka record
    @Test
    void consumesValidAdClickAndAcknowledges() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        AdClickEvent click = validClick();
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class)).thenReturn(click);

        consumer.consumeAdClick(record, acknowledgment);

        assertEquals(PARTITION, click.getPartition());
        assertEquals(OFFSET, click.getOffset());
        verify(joinEngine).processClick(click);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterEventMapper, processedInputMapper);
    }

    // Click ID conflict, persist both dead-letter and processed-input records
    @Test
    void persistsAdClickConflictAndAcknowledges() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        AdClickEvent click = validClick();
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class)).thenReturn(click);
        when(joinEngine.processClick(click)).thenReturn(ClickStateMapper.CONFLICT);

        consumer.consumeAdClick(record, acknowledgment);

        verify(deadLetterEventMapper).insertIfAbsent(
                eq(AD_CLICK_TOPIC), eq(PARTITION), eq(OFFSET), eq(AD_CLICK_TOPIC),
                eq(click.getClickId()), eq(EVENT_TIME), eq(AD_CLICK_PAYLOAD),
                eq("CLICK_ID_CONFLICT"), eq("The same click_id already exists with different content"),
                eq(1), any(Instant.class));
        verify(processedInputMapper).insertDeadLetterRecord(
                AD_CLICK_TOPIC, PARTITION, OFFSET, AD_CLICK_TOPIC,
                click.getClickId(), EVENT_TIME, 1);
        verify(acknowledgment).acknowledge();
    }

    // Null ad click record, acknowledge without writing a dead-letter record
    @Test
    void acknowledgesNullAdClickRecordWithoutPersistingDeadLetter() {
        consumer.consumeAdClick(null, acknowledgment);

        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, objectMapper, deadLetterEventMapper, processedInputMapper);
    }

    // Blank ad click payload, persist invalid-record dead letter and acknowledge
    @Test
    void persistsBlankAdClickRecordAndAcknowledges() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        stubInvalidDeadLetter(true);

        consumer.consumeAdClick(record, acknowledgment);

        verifyInvalidDeadLetter(AD_CLICK_TOPIC, null, null, " ");
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, objectMapper, processedInputMapper);
    }

    // Malformed ad click JSON, persist invalid-record dead letter and acknowledge
    @Test
    void persistsMalformedAdClickAndAcknowledges() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class))
                .thenThrow(jsonProcessingException("invalid ad click"));

        consumer.consumeAdClick(record, acknowledgment);

        verifyInvalidDeadLetter(AD_CLICK_TOPIC, null, null, AD_CLICK_PAYLOAD);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, processedInputMapper);
    }

    // Object mapper returns no ad click event, reject the invalid record
    @Test
    void acknowledgesAdClickWhenParsedEventIsNull() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class)).thenReturn(null);

        consumer.consumeAdClick(record, acknowledgment);

        verifyInvalidDeadLetter(AD_CLICK_TOPIC, null, null, AD_CLICK_PAYLOAD);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, processedInputMapper);
    }

    // Ad click without click ID, reject before calling the join engine
    @Test
    void rejectsAdClickWithoutClickId() throws JsonProcessingException {
        AdClickEvent click = validClick();
        click.setClickId(null);

        assertInvalidAdClick(click, null, EVENT_TIME);
    }

    // Ad click without user ID, reject before calling the join engine
    @Test
    void rejectsAdClickWithoutUserId() throws JsonProcessingException {
        AdClickEvent click = validClick();
        click.setUserId(" ");

        assertInvalidAdClick(click, click.getClickId(), EVENT_TIME);
    }

    // Ad click without campaign ID, reject before calling the join engine
    @Test
    void rejectsAdClickWithoutCampaignId() throws JsonProcessingException {
        AdClickEvent click = validClick();
        click.setCampaignId("");

        assertInvalidAdClick(click, click.getClickId(), EVENT_TIME);
    }

    // Ad click without event time, reject before calling the join engine
    @Test
    void rejectsAdClickWithoutEventTime() throws JsonProcessingException {
        AdClickEvent click = validClick();
        click.setEventTime(null);

        assertInvalidAdClick(click, click.getClickId(), null);
    }

    // Already persisted invalid ad click, keep the operation idempotent
    @Test
    void handlesAlreadyPersistedInvalidAdClick() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        stubInvalidDeadLetter(false);

        consumer.consumeAdClick(record, acknowledgment);

        verifyInvalidDeadLetter(AD_CLICK_TOPIC, null, null, " ");
        verify(acknowledgment).acknowledge();
    }

    // Unexpected ad click parsing failure, wrap error and leave record unacknowledged
    @Test
    void wrapsUnexpectedAdClickParsingFailureWithoutAcknowledging() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        IllegalStateException failure = new IllegalStateException("mapper unavailable");
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class)).thenThrow(failure);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> consumer.consumeAdClick(record, acknowledgment));

        assertEquals("Failed to process ad click", actual.getMessage());
        assertSame(failure, actual.getCause());
        verifyNoInteractions(acknowledgment, joinEngine, deadLetterEventMapper, processedInputMapper);
    }

    // Ad click processing failure, wrap error and leave record unacknowledged
    @Test
    void wrapsAdClickProcessingFailureWithoutAcknowledging() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        AdClickEvent click = validClick();
        IllegalStateException failure = new IllegalStateException("join engine unavailable");
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class)).thenReturn(click);
        when(joinEngine.processClick(click)).thenThrow(failure);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> consumer.consumeAdClick(record, acknowledgment));

        assertEquals("Failed to process ad click", actual.getMessage());
        assertSame(failure, actual.getCause());
        verifyNoInteractions(acknowledgment, deadLetterEventMapper, processedInputMapper);
    }

    // Invalid ad click dead-letter persistence failure, propagate error without acknowledging
    @Test
    void propagatesAdClickDeadLetterPersistenceFailure() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        RuntimeException failure = new RuntimeException("dead letter unavailable");
        when(deadLetterEventMapper.insertIfAbsent(
                eq(AD_CLICK_TOPIC), eq(PARTITION), eq(OFFSET), eq(AD_CLICK_TOPIC),
                nullable(String.class), nullable(Instant.class), eq(" "),
                eq("INVALID_KAFKA_RECORD"), anyString(), eq(1), any(Instant.class)))
                .thenThrow(failure);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> consumer.consumeAdClick(record, acknowledgment));

        assertSame(failure, actual);
        verifyNoInteractions(acknowledgment, processedInputMapper, joinEngine);
    }

    // Valid page view, process successfully and acknowledge the Kafka record
    @Test
    void consumesValidPageViewAndAcknowledges() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD);
        PageViewEvent pageView = validPageView();
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class)).thenReturn(pageView);

        consumer.consumePageView(record, acknowledgment);

        assertEquals(PARTITION, pageView.getPartition());
        assertEquals(OFFSET, pageView.getOffset());
        verify(joinEngine).processPageView(pageView);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterEventMapper, processedInputMapper);
    }

    // Null page view record, acknowledge without writing a dead-letter record
    @Test
    void acknowledgesNullPageViewRecordWithoutPersistingDeadLetter() {
        consumer.consumePageView(null, acknowledgment);

        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, objectMapper, deadLetterEventMapper, processedInputMapper);
    }

    // Blank page view payload, persist invalid-record dead letter and acknowledge
    @Test
    void persistsBlankPageViewRecordAndAcknowledges() {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, " ");

        consumer.consumePageView(record, acknowledgment);

        verifyInvalidDeadLetter(PAGE_VIEW_TOPIC, null, null, " ");
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, objectMapper, processedInputMapper);
    }

    // Malformed page view JSON, persist invalid-record dead letter and acknowledge
    @Test
    void persistsMalformedPageViewAndAcknowledges() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD);
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class))
                .thenThrow(jsonProcessingException("invalid page view"));

        consumer.consumePageView(record, acknowledgment);

        verifyInvalidDeadLetter(PAGE_VIEW_TOPIC, null, null, PAGE_VIEW_PAYLOAD);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, processedInputMapper);
    }

    // Object mapper returns no page view event, reject the invalid record
    @Test
    void acknowledgesPageViewWhenParsedEventIsNull() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD);
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class)).thenReturn(null);

        consumer.consumePageView(record, acknowledgment);

        verifyInvalidDeadLetter(PAGE_VIEW_TOPIC, null, null, PAGE_VIEW_PAYLOAD);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, processedInputMapper);
    }

    // Page view without event ID, reject before calling the join engine
    @Test
    void rejectsPageViewWithoutEventId() throws JsonProcessingException {
        PageViewEvent pageView = validPageView();
        pageView.setEventId(null);

        assertInvalidPageView(pageView, null, EVENT_TIME);
    }

    // Page view without user ID, reject before calling the join engine
    @Test
    void rejectsPageViewWithoutUserId() throws JsonProcessingException {
        PageViewEvent pageView = validPageView();
        pageView.setUserId(" ");

        assertInvalidPageView(pageView, pageView.getEventId(), EVENT_TIME);
    }

    // Page view without URL, reject before calling the join engine
    @Test
    void rejectsPageViewWithoutUrl() throws JsonProcessingException {
        PageViewEvent pageView = validPageView();
        pageView.setUrl("");

        assertInvalidPageView(pageView, pageView.getEventId(), EVENT_TIME);
    }

    // Page view without event time, reject before calling the join engine
    @Test
    void rejectsPageViewWithoutEventTime() throws JsonProcessingException {
        PageViewEvent pageView = validPageView();
        pageView.setEventTime(null);

        assertInvalidPageView(pageView, pageView.getEventId(), null);
    }

    // Unexpected page view parsing failure, wrap error and leave record unacknowledged
    @Test
    void wrapsUnexpectedPageViewParsingFailureWithoutAcknowledging() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD);
        IllegalStateException failure = new IllegalStateException("mapper unavailable");
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class)).thenThrow(failure);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> consumer.consumePageView(record, acknowledgment));

        assertEquals("Failed to process page view", actual.getMessage());
        assertSame(failure, actual.getCause());
        verifyNoInteractions(acknowledgment, joinEngine, deadLetterEventMapper, processedInputMapper);
    }

    // Page view processing failure, wrap error and leave record unacknowledged
    @Test
    void wrapsPageViewProcessingFailureWithoutAcknowledging() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD);
        PageViewEvent pageView = validPageView();
        IllegalStateException failure = new IllegalStateException("join engine unavailable");
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class)).thenReturn(pageView);
        doThrow(failure).when(joinEngine).processPageView(pageView);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> consumer.consumePageView(record, acknowledgment));

        assertEquals("Failed to process page view", actual.getMessage());
        assertSame(failure, actual.getCause());
        verifyNoInteractions(acknowledgment, deadLetterEventMapper, processedInputMapper);
    }

    // Invalid page view dead-letter persistence failure, propagate error without acknowledging
    @Test
    void propagatesPageViewDeadLetterPersistenceFailure() {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, " ");
        RuntimeException failure = new RuntimeException("dead letter unavailable");
        when(deadLetterEventMapper.insertIfAbsent(
                eq(PAGE_VIEW_TOPIC), eq(PARTITION), eq(OFFSET), eq(PAGE_VIEW_TOPIC),
                nullable(String.class), nullable(Instant.class), eq(" "),
                eq("INVALID_KAFKA_RECORD"), anyString(), eq(1), any(Instant.class)))
                .thenThrow(failure);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> consumer.consumePageView(record, acknowledgment));

        assertSame(failure, actual);
        verifyNoInteractions(acknowledgment, processedInputMapper, joinEngine);
    }

    private void assertInvalidAdClick(AdClickEvent click, String eventKey, Instant eventTime)
            throws JsonProcessingException {
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class)).thenReturn(click);

        consumer.consumeAdClick(record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD), acknowledgment);

        verifyInvalidDeadLetter(AD_CLICK_TOPIC, eventKey, eventTime, AD_CLICK_PAYLOAD);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, processedInputMapper);
    }

    private void assertInvalidPageView(PageViewEvent pageView, String eventKey, Instant eventTime)
            throws JsonProcessingException {
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class)).thenReturn(pageView);

        consumer.consumePageView(record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD), acknowledgment);

        verifyInvalidDeadLetter(PAGE_VIEW_TOPIC, eventKey, eventTime, PAGE_VIEW_PAYLOAD);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(joinEngine, processedInputMapper);
    }

    private void verifyInvalidDeadLetter(String topic, String eventKey, Instant eventTime, String payload) {
        verify(deadLetterEventMapper).insertIfAbsent(
                eq(topic), eq(PARTITION), eq(OFFSET), eq(topic), eq(eventKey), eq(eventTime),
                eq(payload), eq("INVALID_KAFKA_RECORD"), anyString(), eq(1), any(Instant.class));
    }

    private void stubInvalidDeadLetter(boolean inserted) {
        when(deadLetterEventMapper.insertIfAbsent(
                anyString(), anyInt(), anyLong(), anyString(), nullable(String.class), nullable(Instant.class),
                anyString(), anyString(), anyString(), anyInt(), any(Instant.class)))
                .thenReturn(inserted);
    }

    private ConsumerRecord<String, String> record(String topic, String payload) {
        return new ConsumerRecord<>(topic, PARTITION, OFFSET, "key-1", payload);
    }

    private AdClickEvent validClick() {
        return AdClickEvent.builder()
                .clickId("click-1")
                .userId("user-1")
                .campaignId("campaign-1")
                .eventTime(EVENT_TIME)
                .build();
    }

    private PageViewEvent validPageView() {
        return PageViewEvent.builder()
                .eventId("event-1")
                .userId("user-1")
                .url("https://example.com/product")
                .eventTime(EVENT_TIME)
                .build();
    }

    private JsonProcessingException jsonProcessingException(String message) {
        return new JsonProcessingException(message) {
        };
    }
}
