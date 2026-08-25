package com.ebay.challenge.streamprocessor.config;

import com.ebay.challenge.streamprocessor.consumer.KafkaRecordDeadLetterRecoverer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerConfigTest {

    private static final String BOOTSTRAP_SERVERS = "kafka-test:29092";
    private static final String GROUP_ID = "test-stream-processor";
    private static final int CONCURRENCY = 4;
    private static final long MAX_RETRIES = 5;
    private static final long RETRY_BACKOFF_MS = 250;

    @Mock
    private KafkaRecordDeadLetterRecoverer recoverer;

    private KafkaConsumerConfig kafkaConsumerConfig;

    @BeforeEach
    void setUp() {
        kafkaConsumerConfig = new KafkaConsumerConfig();
        ReflectionTestUtils.setField(kafkaConsumerConfig, "bootstrapServers", BOOTSTRAP_SERVERS);
        ReflectionTestUtils.setField(kafkaConsumerConfig, "groupId", GROUP_ID);
        ReflectionTestUtils.setField(kafkaConsumerConfig, "concurrency", CONCURRENCY);
        ReflectionTestUtils.setField(kafkaConsumerConfig, "maxRetries", MAX_RETRIES);
        ReflectionTestUtils.setField(kafkaConsumerConfig, "retryBackoffMs", RETRY_BACKOFF_MS);
    }

    // Object mapper, register Java time support
    @Test
    void registersJavaTimeModule() {
        ObjectMapper mapper = kafkaConsumerConfig.objectMapper();

        assertTrue(mapper.getRegisteredModuleIds().stream()
                .anyMatch(moduleId -> moduleId.toString().contains("jsr310")));
        assertDoesNotThrow(() -> mapper.writeValueAsString(Instant.parse("2026-08-24T10:00:00Z")));
    }

    // Consumer factory, configure shared connection and reliability properties
    @Test
    void createsConsumerFactoryWithReliabilityProperties() {
        DefaultKafkaConsumerFactory<String, String> factory = consumerFactory(
                kafkaConsumerConfig.adClickConsumerFactory());
        Map<String, Object> properties = factory.getConfigurationProperties();

        assertEquals(BOOTSTRAP_SERVERS, properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(GROUP_ID, properties.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(StringDeserializer.class, properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class, properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals(false, properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals("earliest", properties.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("read_committed", properties.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        assertEquals(100, properties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    // Two input topics, use the same base consumer configuration
    @Test
    void usesSameBaseConsumerPropertiesForPageViewAndAdClick() {
        Map<String, Object> adClickProperties = consumerFactory(
                kafkaConsumerConfig.adClickConsumerFactory()).getConfigurationProperties();
        Map<String, Object> pageViewProperties = consumerFactory(
                kafkaConsumerConfig.pageViewConsumerFactory()).getConfigurationProperties();

        assertEquals(adClickProperties, pageViewProperties);
    }

    // Ad click listener, use configured concurrency and manual immediate acknowledgment
    @Test
    void configuresAdClickListenerForManualImmediateAcknowledgment() {
        DefaultErrorHandler errorHandler = kafkaConsumerConfig.kafkaErrorHandler(recoverer);
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                kafkaConsumerConfig.adClickListenerContainerFactory(errorHandler);

        assertSame(errorHandler, ReflectionTestUtils.getField(factory, "commonErrorHandler"));
        assertEquals(CONCURRENCY, ReflectionTestUtils.getField(factory, "concurrency"));
        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE,
                factory.getContainerProperties().getAckMode());
    }

    // Page view listener, use configured concurrency and manual immediate acknowledgment
    @Test
    void configuresPageViewListenerForManualImmediateAcknowledgment() {
        DefaultErrorHandler errorHandler = kafkaConsumerConfig.kafkaErrorHandler(recoverer);
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                kafkaConsumerConfig.pageViewListenerContainerFactory(errorHandler);

        assertSame(errorHandler, ReflectionTestUtils.getField(factory, "commonErrorHandler"));
        assertEquals(CONCURRENCY, ReflectionTestUtils.getField(factory, "concurrency"));
        assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE,
                factory.getContainerProperties().getAckMode());
    }

    // Listener containers, enable delivery attempt headers and preserve missing-topic behavior
    @Test
    void configuresListenerDeliveryAttemptAndMissingTopicBehavior() {
        DefaultErrorHandler errorHandler = kafkaConsumerConfig.kafkaErrorHandler(recoverer);
        ConcurrentKafkaListenerContainerFactory<String, String> adClickFactory =
                kafkaConsumerConfig.adClickListenerContainerFactory(errorHandler);
        ConcurrentKafkaListenerContainerFactory<String, String> pageViewFactory =
                kafkaConsumerConfig.pageViewListenerContainerFactory(errorHandler);

        assertTrue(adClickFactory.getContainerProperties().isDeliveryAttemptHeader());
        assertTrue(pageViewFactory.getContainerProperties().isDeliveryAttemptHeader());
        assertFalse(adClickFactory.getContainerProperties().isMissingTopicsFatal());
        assertFalse(pageViewFactory.getContainerProperties().isMissingTopicsFatal());
    }

    // Error handler, use configured retry backoff and retry count
    @Test
    void configuresRetryHandlerWithConfiguredBackoff() {
        DefaultErrorHandler errorHandler = kafkaConsumerConfig.kafkaErrorHandler(recoverer);
        Object failureTracker = ReflectionTestUtils.getField(errorHandler, "failureTracker");
        FixedBackOff backOff = (FixedBackOff) ReflectionTestUtils.getField(failureTracker, "backOff");

        assertEquals(RETRY_BACKOFF_MS, backOff.getInterval());
        assertEquals(MAX_RETRIES, backOff.getMaxAttempts());
    }

    // Recovered record, commit the recovered offset and acknowledge the handled record
    @Test
    void configuresRecoveredRecordCommitAndAcknowledgment() {
        DefaultErrorHandler errorHandler = kafkaConsumerConfig.kafkaErrorHandler(recoverer);

        assertEquals(true, ReflectionTestUtils.getField(errorHandler, "commitRecovered"));
        assertTrue(errorHandler.isAckAfterHandle());
    }

    @SuppressWarnings("unchecked")
    private static DefaultKafkaConsumerFactory<String, String> consumerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        return (DefaultKafkaConsumerFactory<String, String>) consumerFactory;
    }
}
