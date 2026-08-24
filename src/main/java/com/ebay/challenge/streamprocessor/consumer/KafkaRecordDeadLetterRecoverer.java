package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.mapper.DeadLetterEventMapper;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputMapper;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.ByteBuffer;
import java.time.Instant;

/**
 * Kafka msg retries 3 times and still error, save to db
 * - insert into dead_letter_event
 * - insert into processed_input=DEAD_LETTER
 */
@Component
@RequiredArgsConstructor
public class KafkaRecordDeadLetterRecoverer implements ConsumerRecordRecoverer {

    private final DeadLetterEventMapper deadLetterEventMapper;
    private final ProcessedInputMapper processedInputMapper;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.ad-clicks:ad_clicks}")
    private String adClicksTopic;

    @Value("${kafka.topics.page-views:page_views}")
    private String pageViewsTopic;

    @Value("${kafka.consumer.max-retries:3}")
    private long maxRetries;

    @Override
    @Transactional
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        String eventType = eventType(record.topic());
        String payload = record.value() == null ? null : record.value().toString();
        EventMetadata metadata = parseMetadata(eventType, payload);
        int attemptCount = deliveryAttempt(record);
        Instant createdAt = Instant.now();
        Throwable rootCause = rootCause(exception);

        deadLetterEventMapper.insertIfAbsent(record.topic(), record.partition(), record.offset(),
                eventType, metadata.eventKey(), metadata.eventTime(), payload, "PROCESSING_RETRY_EXHAUSTED",
                rootCause.getMessage(), attemptCount, createdAt);

        processedInputMapper.insertDeadLetterRecord(record.topic(), record.partition(),
                record.offset(), eventType, metadata.eventKey(), metadata.eventTime(), attemptCount);
    }

    private String eventType(String topic) {
        if (adClicksTopic.equals(topic)) {
            return "ad_clicks";
        }

        if (pageViewsTopic.equals(topic)) {
            return "page_views";
        }

        return topic;
    }

    private EventMetadata parseMetadata(String eventType, String payload) {
        if (payload == null || payload.isBlank()) {
            return new EventMetadata(null, null);
        }

        try {
            if ("ad_clicks".equals(eventType)) {
                AdClickEvent click = objectMapper.readValue(payload, AdClickEvent.class);
                return new EventMetadata(click.getClickId(), click.getEventTime());
            }

            if ("page_views".equals(eventType)) {
                PageViewEvent pageView = objectMapper.readValue(payload, PageViewEvent.class);
                return new EventMetadata(pageView.getEventId(), pageView.getEventTime());
            }
        } catch (JsonProcessingException ignored) {
            // Keep the raw payload and Kafka metadata when the payload cannot be parsed.
        }

        return new EventMetadata(null, null);
    }

    private int deliveryAttempt(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        if (header != null && header.value() != null && header.value().length == Integer.BYTES) {
            return ByteBuffer.wrap(header.value()).getInt();
        }

        return maxRetries >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxRetries + 1;
    }

    private Throwable rootCause(Exception exception) {
        Throwable rootCause = exception;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }

        return rootCause;
    }

    private record EventMetadata(String eventKey, Instant eventTime) {
    }
}
