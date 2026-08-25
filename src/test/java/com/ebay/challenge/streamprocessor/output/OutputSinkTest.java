package com.ebay.challenge.streamprocessor.output;

import com.ebay.challenge.streamprocessor.infrastructure.OutputSinkException;
import com.ebay.challenge.streamprocessor.mapper.OutputMapper;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutputSinkTest {

    private static final Instant EVENT_TIME = Instant.parse("2026-08-24T10:00:00Z");

    @Mock
    private OutputMapper outputMapper;

    private OutputSink outputSink;

    @BeforeEach
    void setUp() {
        outputSink = new OutputSink(outputMapper);
    }

    // New attributed page view, persist successfully
    @Test
    void writesNewRecordWhenInsertSucceeds() {
        AttributedPageView record = validRecord();
        when(outputMapper.insertIfAbsent(record)).thenReturn(true);

        outputSink.write(record);

        verify(outputMapper).insertIfAbsent(record);
    }

    // Duplicate page view with identical content, keep the operation idempotent
    @Test
    void acceptsDuplicateRecordWhenStoredOutputMatches() {
        AttributedPageView record = validRecord();
        when(outputMapper.insertIfAbsent(record)).thenReturn(false);
        when(outputMapper.findByPageViewId(record.getPageViewId())).thenReturn(record);

        outputSink.write(record);

        verify(outputMapper).insertIfAbsent(record);
        verify(outputMapper).findByPageViewId(record.getPageViewId());
    }

    // Same page view ID with different content, reject the output conflict
    @Test
    void rejectsConflictingRecordWithSamePageViewId() {
        AttributedPageView record = validRecord();
        AttributedPageView existingRecord = validRecord();
        existingRecord.setUrl("https://example.com/other-product");
        when(outputMapper.insertIfAbsent(record)).thenReturn(false);
        when(outputMapper.findByPageViewId(record.getPageViewId())).thenReturn(existingRecord);

        OutputSinkException exception = assertThrows(OutputSinkException.class,
                () -> outputSink.write(record));

        assertTrue(exception.getMessage().contains(record.getPageViewId()));
        verify(outputMapper).insertIfAbsent(record);
        verify(outputMapper).findByPageViewId(record.getPageViewId());
    }

    // Insert reports conflict but the existing output cannot be loaded
    @Test
    void rejectsConflictWhenExistingRecordCannotBeLoaded() {
        AttributedPageView record = validRecord();
        when(outputMapper.insertIfAbsent(record)).thenReturn(false);
        when(outputMapper.findByPageViewId(record.getPageViewId())).thenReturn(null);

        OutputSinkException exception = assertThrows(OutputSinkException.class,
                () -> outputSink.write(record));

        assertTrue(exception.getMessage().contains(record.getPageViewId()));
        verify(outputMapper).insertIfAbsent(record);
        verify(outputMapper).findByPageViewId(record.getPageViewId());
    }

    // Null output record, reject before mapper access
    @Test
    void rejectsNullRecord() {
        assertThrows(OutputSinkException.class, () -> outputSink.write(null));

        verifyNoInteractions(outputMapper);
    }

    // Missing page view ID, reject the output
    @Test
    void rejectsBlankPageViewId() {
        AttributedPageView record = validRecord();
        record.setPageViewId(" ");

        assertThrows(OutputSinkException.class, () -> outputSink.write(record));

        verifyNoInteractions(outputMapper);
    }

    // Missing user ID, reject the output
    @Test
    void rejectsBlankUserId() {
        AttributedPageView record = validRecord();
        record.setUserId("");

        assertThrows(OutputSinkException.class, () -> outputSink.write(record));

        verifyNoInteractions(outputMapper);
    }

    // Missing URL, reject the output
    @Test
    void rejectsBlankUrl() {
        AttributedPageView record = validRecord();
        record.setUrl(" ");

        assertThrows(OutputSinkException.class, () -> outputSink.write(record));

        verifyNoInteractions(outputMapper);
    }

    // Missing event time, reject the output
    @Test
    void rejectsMissingEventTime() {
        AttributedPageView record = validRecord();
        record.setEventTime(null);

        assertThrows(OutputSinkException.class, () -> outputSink.write(record));

        verifyNoInteractions(outputMapper);
    }

    // Campaign ID without click ID, reject incomplete attribution
    @Test
    void rejectsCampaignWithoutClickId() {
        AttributedPageView record = validRecord();
        record.setAttributedClickId(null);

        assertThrows(OutputSinkException.class, () -> outputSink.write(record));

        verifyNoInteractions(outputMapper);
    }

    // Click ID without campaign ID, reject incomplete attribution
    @Test
    void rejectsClickIdWithoutCampaignId() {
        AttributedPageView record = validRecord();
        record.setAttributedCampaignId(null);

        assertThrows(OutputSinkException.class, () -> outputSink.write(record));

        verifyNoInteractions(outputMapper);
    }

    // Page view without a matching click, accept paired null attribution fields
    @Test
    void acceptsRecordWithoutAttribution() {
        AttributedPageView record = validRecord();
        record.setAttributedCampaignId(null);
        record.setAttributedClickId(null);
        when(outputMapper.insertIfAbsent(record)).thenReturn(true);

        outputSink.write(record);

        verify(outputMapper).insertIfAbsent(record);
    }

    // Page view with a matching click, accept paired attribution fields
    @Test
    void acceptsRecordWithAttribution() {
        AttributedPageView record = validRecord();
        when(outputMapper.insertIfAbsent(record)).thenReturn(true);

        outputSink.write(record);

        verify(outputMapper).insertIfAbsent(record);
    }

    // Output mapper runtime failure, wrap it as an OutputSinkException
    @Test
    void wrapsUnexpectedMapperFailure() {
        AttributedPageView record = validRecord();
        IllegalStateException failure = new IllegalStateException("database unavailable");
        when(outputMapper.insertIfAbsent(record)).thenThrow(failure);

        OutputSinkException exception = assertThrows(OutputSinkException.class,
                () -> outputSink.write(record));

        assertSame(failure, exception.getCause());
        assertTrue(exception.getMessage().contains(record.getPageViewId()));
        verify(outputMapper).insertIfAbsent(record);
    }

    private AttributedPageView validRecord() {
        return AttributedPageView.builder()
                .pageViewId("pv-1")
                .userId("user-1")
                .eventTime(EVENT_TIME)
                .url("https://example.com/product")
                .attributedCampaignId("campaign-1")
                .attributedClickId("click-1")
                .build();
    }
}
