package com.ebay.challenge.streamprocessor.consumer;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.InOrder;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamConsumerStarterTest {

    @Mock
    private JoinEngine joinEngine;

    @Mock
    private KafkaListenerEndpointRegistry registry;

    @Mock
    private MessageListenerContainer adClickContainer;

    @Mock
    private MessageListenerContainer pageViewContainer;

    private StreamConsumerStarter starter;

    @BeforeEach
    void setUp() {
        starter = new StreamConsumerStarter(joinEngine, registry);
    }

    // Restore state before starting either Kafka listener
    @Test
    void restoresStateBeforeStartingListeners() {
        when(registry.getListenerContainer("adClickListener")).thenReturn(adClickContainer);
        when(registry.getListenerContainer("pageViewListener")).thenReturn(pageViewContainer);

        starter.startAfterRestore();

        InOrder inOrder = inOrder(joinEngine, registry, adClickContainer, pageViewContainer);
        inOrder.verify(joinEngine).restoreState();
        inOrder.verify(registry).getListenerContainer("adClickListener");
        inOrder.verify(adClickContainer).start();
        inOrder.verify(registry).getListenerContainer("pageViewListener");
        inOrder.verify(pageViewContainer).start();
    }

    // State restoration failure, do not start Kafka listeners
    @Test
    void doesNotStartListenersWhenStateRestoreFails() {
        RuntimeException failure = new RuntimeException("state restore failed");
        doThrow(failure).when(joinEngine).restoreState();

        RuntimeException actual = assertThrows(RuntimeException.class, starter::startAfterRestore);

        assertSame(failure, actual);
        verifyNoInteractions(registry, adClickContainer, pageViewContainer);
    }

    // First listener startup failure, stop the remaining startup flow
    @Test
    void stopsStartupWhenAdClickListenerFailsToStart() {
        RuntimeException failure = new RuntimeException("listener start failed");
        when(registry.getListenerContainer("adClickListener")).thenReturn(adClickContainer);
        doThrow(failure).when(adClickContainer).start();

        RuntimeException actual = assertThrows(RuntimeException.class, starter::startAfterRestore);

        assertSame(failure, actual);
        verify(joinEngine).restoreState();
        verify(adClickContainer).start();
        verifyNoInteractions(pageViewContainer);
    }
}
