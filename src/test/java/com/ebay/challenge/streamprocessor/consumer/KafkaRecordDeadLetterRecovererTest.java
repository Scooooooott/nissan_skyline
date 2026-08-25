package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.mapper.DeadLetterEventMapper;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputMapper;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.support.KafkaHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.ByteBuffer;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaRecordDeadLetterRecovererTest {

    private static final String AD_CLICK_TOPIC = "ad_clicks";
    private static final String PAGE_VIEW_TOPIC = "page_views";
    private static final String AD_CLICK_PAYLOAD = "{\"click_id\":\"click-1\"}";
    private static final String PAGE_VIEW_PAYLOAD = "{\"event_id\":\"event-1\"}";
    private static final int PARTITION = 1;
    private static final long OFFSET = 99L;
    private static final Instant EVENT_TIME = Instant.parse("2026-08-24T10:00:00Z");

    @Mock
    private DeadLetterEventMapper deadLetterEventMapper;

    @Mock
    private ProcessedInputMapper processedInputMapper;

    @Mock
    private ObjectMapper objectMapper;

    private KafkaRecordDeadLetterRecoverer recoverer;

    @BeforeEach
    void setUp() {
        recoverer = new KafkaRecordDeadLetterRecoverer(
                deadLetterEventMapper,
                processedInputMapper,
                objectMapper
        );
        ReflectionTestUtils.setField(recoverer, "adClicksTopic", AD_CLICK_TOPIC);
        ReflectionTestUtils.setField(recoverer, "pageViewsTopic", PAGE_VIEW_TOPIC);
        ReflectionTestUtils.setField(recoverer, "maxRetries", 3L);
    }

    // Ad click topic with valid payload, persist parsed metadata to both records
    @Test
    void persistsAdClickDeadLetterWithParsedMetadata() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        AdClickEvent click = validClick();
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class)).thenReturn(click);

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", click.getClickId(), EVENT_TIME,
                AD_CLICK_PAYLOAD, "processing failed", 4);
        verifyProcessedInput("ad_clicks", click.getClickId(), EVENT_TIME, 4);
    }

    // Page view topic with valid payload, persist parsed metadata to both records
    @Test
    void persistsPageViewDeadLetterWithParsedMetadata() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD);
        PageViewEvent pageView = validPageView();
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class)).thenReturn(pageView);

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyDeadLetter(PAGE_VIEW_TOPIC, "page_views", pageView.getEventId(), EVENT_TIME,
                PAGE_VIEW_PAYLOAD, "processing failed", 4);
        verifyProcessedInput("page_views", pageView.getEventId(), EVENT_TIME, 4);
    }

    // Unknown topic, preserve raw topic and leave event metadata empty
    @Test
    void usesRawTopicAndNullMetadataForUnknownTopic() {
        String unknownTopic = "other_events";
        ConsumerRecord<String, String> record = record(unknownTopic, "{}");

        recoverer.accept(record, new IllegalStateException("unsupported topic"));

        verifyDeadLetter(unknownTopic, unknownTopic, null, null, "{}", "unsupported topic", 4);
        verifyProcessedInput(unknownTopic, null, null, 4);
        verifyNoInteractions(objectMapper);
    }

    // Null payload, persist dead letter without event metadata
    @Test
    void handlesNullPayload() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, null);

        recoverer.accept(record, new IllegalStateException("empty payload"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null, null, "empty payload", 4);
        verifyProcessedInput("ad_clicks", null, null, 4);
        verifyNoInteractions(objectMapper);
    }

    // Blank payload, persist dead letter without event metadata
    @Test
    void handlesBlankPayload() {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, "   ");

        recoverer.accept(record, new IllegalStateException("blank payload"));

        verifyDeadLetter(PAGE_VIEW_TOPIC, "page_views", null, null, "   ", "blank payload", 4);
        verifyProcessedInput("page_views", null, null, 4);
        verifyNoInteractions(objectMapper);
    }

    // Malformed ad click payload, preserve raw payload and persist null metadata
    @Test
    void handlesMalformedAdClickPayload() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, AD_CLICK_PAYLOAD);
        when(objectMapper.readValue(AD_CLICK_PAYLOAD, AdClickEvent.class))
                .thenThrow(jsonProcessingException("invalid ad click"));

        recoverer.accept(record, new IllegalStateException("retry exhausted"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null,
                AD_CLICK_PAYLOAD, "retry exhausted", 4);
        verifyProcessedInput("ad_clicks", null, null, 4);
    }

    // Malformed page view payload, preserve raw payload and persist null metadata
    @Test
    void handlesMalformedPageViewPayload() throws JsonProcessingException {
        ConsumerRecord<String, String> record = record(PAGE_VIEW_TOPIC, PAGE_VIEW_PAYLOAD);
        when(objectMapper.readValue(PAGE_VIEW_PAYLOAD, PageViewEvent.class))
                .thenThrow(jsonProcessingException("invalid page view"));

        recoverer.accept(record, new IllegalStateException("retry exhausted"));

        verifyDeadLetter(PAGE_VIEW_TOPIC, "page_views", null, null,
                PAGE_VIEW_PAYLOAD, "retry exhausted", 4);
        verifyProcessedInput("page_views", null, null, 4);
    }

    // Valid delivery-attempt header, use the header value
    @Test
    void usesDeliveryAttemptHeader() {
        ConsumerRecord<String, String> record = recordWithDeliveryAttempt(AD_CLICK_TOPIC, " ", 7);

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null, " ", "processing failed", 7);
        verifyProcessedInput("ad_clicks", null, null, 7);
    }

    // Missing delivery-attempt header, use max retries plus one
    @Test
    void fallsBackWhenDeliveryAttemptHeaderIsMissing() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null, " ", "processing failed", 4);
    }

    // Null delivery-attempt header value, use max retries plus one
    @Test
    void fallsBackWhenDeliveryAttemptHeaderValueIsNull() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        record.headers().add(new RecordHeader(KafkaHeaders.DELIVERY_ATTEMPT, null));

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null, " ", "processing failed", 4);
    }

    // Invalid delivery-attempt header length, use max retries plus one
    @Test
    void fallsBackWhenDeliveryAttemptHeaderLengthIsInvalid() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        record.headers().add(new RecordHeader(KafkaHeaders.DELIVERY_ATTEMPT, new byte[]{1, 2}));

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null, " ", "processing failed", 4);
    }

    // Max retries at integer limit, cap fallback attempt count
    @Test
    void capsFallbackAttemptCountAtIntegerMaxValue() {
        ReflectionTestUtils.setField(recoverer, "maxRetries", (long) Integer.MAX_VALUE);
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null, " ", "processing failed",
                Integer.MAX_VALUE);
    }

    // Nested processing exception, persist the deepest root-cause message
    @Test
    void persistsDeepestRootCauseMessage() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        IllegalArgumentException rootCause = new IllegalArgumentException("root cause");
        RuntimeException exception = new RuntimeException("outer cause", rootCause);

        recoverer.accept(record, exception);

        verifyDeadLetter(AD_CLICK_TOPIC, "ad_clicks", null, null, " ", "root cause", 4);
    }

    // Existing dead-letter record, still write the processed-input marker
    @Test
    void writesProcessedInputWhenDeadLetterAlreadyExists() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        when(deadLetterEventMapper.insertIfAbsent(
                eq(AD_CLICK_TOPIC), eq(PARTITION), eq(OFFSET), eq("ad_clicks"),
                eq((String) null), eq((Instant) null), eq(" "),
                eq("PROCESSING_RETRY_EXHAUSTED"), eq("processing failed"), eq(4), any(Instant.class)))
                .thenReturn(false);

        recoverer.accept(record, new IllegalStateException("processing failed"));

        verifyProcessedInput("ad_clicks", null, null, 4);
    }

    // Dead-letter persistence failure, do not write the processed-input marker
    @Test
    void propagatesDeadLetterPersistenceFailure() {
        ConsumerRecord<String, String> record = record(AD_CLICK_TOPIC, " ");
        RuntimeException failure = new RuntimeException("dead letter unavailable");
        when(deadLetterEventMapper.insertIfAbsent(
                eq(AD_CLICK_TOPIC), eq(PARTITION), eq(OFFSET), eq("ad_clicks"),
                eq((String) null), eq((Instant) null), eq(" "),
                eq("PROCESSING_RETRY_EXHAUSTED"), eq("processing failed"), eq(4), any(Instant.class)))
                .thenThrow(failure);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> recoverer.accept(record, new IllegalStateException("processing failed")));

        assertSame(failure, actual);
        verifyNoInteractions(processedInputMapper);
    }

    private void verifyDeadLetter(String topic, String eventType, String eventKey, Instant eventTime,
                                  String payload, String errorMessage, int attemptCount) {
        verify(deadLetterEventMapper).insertIfAbsent(
                eq(topic), eq(PARTITION), eq(OFFSET), eq(eventType), eq(eventKey), eq(eventTime),
                eq(payload), eq("PROCESSING_RETRY_EXHAUSTED"), eq(errorMessage), eq(attemptCount),
                any(Instant.class));
    }

    private void verifyProcessedInput(String eventType, String eventKey, Instant eventTime, int attemptCount) {
        verify(processedInputMapper).insertDeadLetterRecord(
                eq(eventType.equals(AD_CLICK_TOPIC) ? AD_CLICK_TOPIC :
                        eventType.equals(PAGE_VIEW_TOPIC) ? PAGE_VIEW_TOPIC : eventType),
                eq(PARTITION), eq(OFFSET), eq(eventType), eq(eventKey), eq(eventTime), eq(attemptCount));
    }

    private ConsumerRecord<String, String> record(String topic, String payload) {
        return new ConsumerRecord<>(topic, PARTITION, OFFSET, "key-1", payload);
    }

    private ConsumerRecord<String, String> recordWithDeliveryAttempt(String topic, String payload, int attempt) {
        ConsumerRecord<String, String> record = record(topic, payload);
        record.headers().add(new RecordHeader(
                KafkaHeaders.DELIVERY_ATTEMPT,
                ByteBuffer.allocate(Integer.BYTES).putInt(attempt).array()));
        return record;
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
