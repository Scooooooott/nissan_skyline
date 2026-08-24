package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.infrastructure.InvalidKafkaRecordException;
import com.ebay.challenge.streamprocessor.mapper.ClickStateMapper;
import com.ebay.challenge.streamprocessor.mapper.DeadLetterEventMapper;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputMapper;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.common.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.time.Instant;


/**
 * Kafka consumer that processes page view and ad click events.
 * <p>
 * Uses Spring Kafka's concurrent message listener containers for partition-aware processing.
 * Implements manual offset commit after successful processing for at-least-once delivery.
 *
 * Only handle msg consuming and does not participate in any business operations.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamConsumer {

    private final JoinEngine joinEngine;
    private final ObjectMapper objectMapper;

    private final DeadLetterEventMapper deadLetterEventMapper;
    private final ProcessedInputMapper processedInputMapper;

    private static final String EVENT_TYPE_AD_CLICK = "ad_clicks";
    private static final String EVENT_TYPE_PAGE_VIEW = "page_views";
    private static final String ERROR_TYPE_INVALID_KAFKA_RECORD = "INVALID_KAFKA_RECORD";

    /**
     * Consume ad click events from Kafka.
     * <p>
     * Each partition is processed by a dedicated thread (configured via concurrency).
     * Offsets are committed manually after successful processing to ensure at-least-once delivery.
     * <p>
     * - Parse JSON to AdClickEvent
     * - Set partition and offset metadata
     * - Process through joinEngine
     * - Acknowledge offset on success
     * - Handle errors appropriately
     */
    @KafkaListener(
            id = "adClickListener",
            topics = "${kafka.topics.ad-clicks:ad_clicks}",
            groupId = "${kafka.consumer.group-id:stream-processor-group}",
            containerFactory = "adClickListenerContainerFactory",
            autoStartup = "false"
    )
    public void consumeAdClick(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        AdClickEvent click = new AdClickEvent();
        try {
            // check null, throw InvalidKafkaRecordException when null
            if (ObjectUtils.isEmpty(record) || StringUtils.isBlank(record.value())) {
                throw new InvalidKafkaRecordException("StreamConsumer.consumeAdClick: empty Kafka record!");
            }

            log.debug("Received ad click from partition {} at offset {}", record.partition(), record.offset());

            // parse to Dto, throw InvalidKafkaRecordException when cannot parse
            try {
                click = objectMapper.readValue(record.value(), AdClickEvent.class);
            } catch (JsonProcessingException e) {
                throw new InvalidKafkaRecordException("StreamConsumer.consumeAdClick: JSON parse error at:"
                        + " partition=" + record.partition() + ", offset=" + record.offset()
                        + " record: " + record, e);
            }

            // check null field, throw InvalidKafkaRecordException when null field
            if (ObjectUtils.isEmpty(click) || StringUtils.isBlank(click.getClickId()) || StringUtils.isBlank(click.getUserId())
                    || StringUtils.isBlank(click.getCampaignId()) || ObjectUtils.isEmpty(click.getEventTime())){
                throw new InvalidKafkaRecordException("StreamConsumer.consumeAdClick: null field error at:"
                        + " partition=" + record.partition() + ", offset=" + record.offset()
                        + " record: " + record);
            }

            // Set partition and offset metadata on the event
            click.setPartition(record.partition());
            click.setOffset(record.offset());

            // Process the click through the join engine
            String processingStatus = joinEngine.processClick(click);
            if (ClickStateMapper.CONFLICT.equals(processingStatus)) {
                persistClickConflict(record, click);
            }

            // Acknowledge the offset after successful processing
            acknowledgment.acknowledge();

            log.debug("Successfully processed ad click from partition {} offset {}", record.partition(), record.offset());
        } catch (InvalidKafkaRecordException e){
            // for InvalidKafkaRecordException (Kafka msg can not be consumed), log and acknowledge
            log.error("StreamConsumer.consumeAdClick: Invalid ad_click Kafka record: ", e);
            // error into db
            persistInvalidRecord(record, EVENT_TYPE_AD_CLICK,
                    click == null ? null : click.getClickId(),
                    click == null ? null : click.getEventTime(), e);
            // acknowledge, no retry
            acknowledgment.acknowledge();
        } catch (Exception e) {
            // other exceptions
            log.error("StreamConsumer.consumeAdClick: Error processing ad_click, record:{}, exception:", record, e);
            // Don't acknowledge -> retry 3 times, if doesnot work, save dead letter into db.
            throw new RuntimeException("Failed to process ad click", e);
        }
    }

    /**
     * Consume page view events from Kafka.
     * <p>
     * Each partition is processed by a dedicated thread (configured via concurrency).
     * Offsets are committed manually after successful processing to ensure at-least-once delivery.
     * <p>
     * - Parse JSON to PageViewEvent
     * - Set partition and offset metadata
     * - Process through joinEngine
     * - Acknowledge offset on success
     * - Handle errors appropriately
     */
    @KafkaListener(
            id = "pageViewListener",
            topics = "${kafka.topics.page-views:page_views}",
            groupId = "${kafka.consumer.group-id:stream-processor-group}",
            containerFactory = "pageViewListenerContainerFactory",
            autoStartup = "false"
    )
    public void consumePageView(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        PageViewEvent pageView = new PageViewEvent();
        try {
            // check null, throw InvalidKafkaRecordException when null
            if (ObjectUtils.isEmpty(record) || StringUtils.isBlank(record.value())) {
                throw new InvalidKafkaRecordException("StreamConsumer.consumePageView: empty Kafka record!");
            }

            log.debug("Received page view from partition {} at offset {}", record.partition(), record.offset());

            // parse to Dto, throw InvalidKafkaRecordException when cannot parse
            try {
                pageView = objectMapper.readValue(record.value(), PageViewEvent.class);
            } catch (JsonProcessingException e) {
                throw new InvalidKafkaRecordException("StreamConsumer.consumePageView: JSON parse error at:"
                        + " partition=" + record.partition() + ", offset=" + record.offset()
                        + " record: " + record, e);
            }

            // check null field, throw InvalidKafkaRecordException when null field
            if (ObjectUtils.isEmpty(pageView) || StringUtils.isBlank(pageView.getEventId()) || StringUtils.isBlank(pageView.getUserId())
                    || StringUtils.isBlank(pageView.getUrl()) || ObjectUtils.isEmpty(pageView.getEventTime())){
                throw new InvalidKafkaRecordException("StreamConsumer.consumePageView: null field error at:"
                        + " partition=" + record.partition() + ", offset=" + record.offset()
                        + " record: " + record);
            }

            // Set partition and offset metadata on the event
            pageView.setPartition(record.partition());
            pageView.setOffset(record.offset());

            // Process the page view through the join engine
            joinEngine.processPageView(pageView);

            // Acknowledge the offset after successful processing
            acknowledgment.acknowledge();

            log.debug("Successfully processed page view from partition {} offset {}",
                record.partition(), record.offset());

        } catch(InvalidKafkaRecordException e){
            // for InvalidKafkaRecordException (Kafka msg can not be consumed), log and acknowledge
            log.error("StreamConsumer.consumePageView: Invalid page_view Kafka record: ", e);
            // error into db
            persistInvalidRecord(record, EVENT_TYPE_PAGE_VIEW,
                    pageView == null ? null : pageView.getEventId(),
                    pageView == null ? null : pageView.getEventTime(), e);
            // acknowledge, no retry
            acknowledgment.acknowledge();
        } catch (Exception e) {
            // other exceptions
            log.error("StreamConsumer.consumePageView: Error processing page_view, record:{}, exception:", record, e);
            // Don't acknowledge -> retry 3 times, if doesnot work, save dead letter into db.
            throw new RuntimeException("Failed to process page view", e);
        }
    }

    private void persistInvalidRecord(ConsumerRecord<String, String> record, String eventType, String eventKey,
                                      Instant eventTime, InvalidKafkaRecordException exception) {
        // empty msg
        if (ObjectUtils.isEmpty(record)) {
            return;
        }

        String payload = record.value();
        // insert error msg
        boolean inserted = deadLetterEventMapper.insertIfAbsent(record.topic(), record.partition(), record.offset(),
                eventType, eventKey, eventTime, payload, ERROR_TYPE_INVALID_KAFKA_RECORD, exception.getMessage(),
                1, Instant.now());

        if (inserted) {
            log.warn("Invalid Kafka record persisted to dead letter: topic={}, partition={}, offset={}, eventType={}",
                    record.topic(), record.partition(), record.offset(), eventType);
        } else {
            log.warn("Invalid Kafka record already exists in dead letter: topic={}, partition={}, offset={}, eventType={}",
                    record.topic(), record.partition(), record.offset(), eventType);
        }
    }


    /**
     * When click conflict (same click_id, different body), save records into db
     * - one into processed record, marked as consumed
     * - one into dead letter, as error
     * */
    private void persistClickConflict(ConsumerRecord<String, String> record, AdClickEvent click) {
        // insert into dead_letter
        deadLetterEventMapper.insertIfAbsent(record.topic(), record.partition(), record.offset(),
                EVENT_TYPE_AD_CLICK, click.getClickId(), click.getEventTime(), record.value(),
                "CLICK_ID_CONFLICT", "The same click_id already exists with different content", 1, Instant.now());

        processedInputMapper.insertDeadLetterRecord(record.topic(), record.partition(), record.offset(),
                EVENT_TYPE_AD_CLICK, click.getClickId(), click.getEventTime(), 1);

        log.warn("Conflicting click saved to dead letter: topic={}, partition={}, offset={}, clickId={}",
                record.topic(), record.partition(), record.offset(), click.getClickId());
    }
}
