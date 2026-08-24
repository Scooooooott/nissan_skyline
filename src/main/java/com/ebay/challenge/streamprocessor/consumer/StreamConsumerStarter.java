package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

/**
 * Starter for recovering from application drop.
 * Kafka_auto_start=false.
 * After application being ready, this starter restores the memory for joinEngine
 * and then explicitly wakes up Kafka Listeners.
 * */
@Component
@RequiredArgsConstructor
public class StreamConsumerStarter {

    private final JoinEngine joinEngine;
    private final KafkaListenerEndpointRegistry registry;

    @EventListener(ApplicationReadyEvent.class)
    public void startAfterRestore() {

        joinEngine.restoreState();

        registry.getListenerContainer("adClickListener").start();
        registry.getListenerContainer("pageViewListener").start();
    }
}
