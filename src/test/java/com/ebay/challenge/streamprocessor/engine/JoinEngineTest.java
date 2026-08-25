package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.infrastructure.StateAccessLock;
import com.ebay.challenge.streamprocessor.mapper.ClickStateMapper;
import com.ebay.challenge.streamprocessor.mapper.PendingPageViewMapper;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputMapper;
import com.ebay.challenge.streamprocessor.mapper.WatermarkStateMapper;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.MemoryStateChanges;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.model.PendingPageview;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class JoinEngineTest {

    private static final int PARTITION = 3;
    private static final Instant BASE_TIME = Instant.parse("2026-08-24T12:00:00Z");

    private ClickStateStore clickStore;
    private WatermarkTracker watermarkTracker;
    private OutputSink outputSink;
    private StateAccessLock stateAccessLock;
    private ClickStateMapper clickStateMapper;
    private WatermarkStateMapper watermarkStateMapper;
    private PendingPageViewMapper pendingPageViewMapper;
    private ProcessedInputMapper processedInputMapper;
    private TransactionTemplate transactionTemplate;
    private Lock readLock;
    private Lock writeLock;

    private JoinEngine engine;

    @BeforeEach
    void setUp() {
        clickStore = mock(ClickStateStore.class);
        watermarkTracker = mock(WatermarkTracker.class);
        outputSink = mock(OutputSink.class);
        stateAccessLock = mock(StateAccessLock.class);
        clickStateMapper = mock(ClickStateMapper.class);
        watermarkStateMapper = mock(WatermarkStateMapper.class);
        pendingPageViewMapper = mock(PendingPageViewMapper.class);
        processedInputMapper = mock(ProcessedInputMapper.class);
        transactionTemplate = mock(TransactionTemplate.class);
        readLock = mock(Lock.class);
        writeLock = mock(Lock.class);

        when(stateAccessLock.readLock()).thenReturn(readLock);
        when(stateAccessLock.writeLock()).thenReturn(writeLock);
        when(watermarkTracker.getAllowedLateness()).thenReturn(Duration.ofMinutes(2));
        when(watermarkTracker.getWatermark(anyInt())).thenReturn(Instant.MIN);
        when(watermarkStateMapper.findAll()).thenReturn(List.of());
        when(clickStateMapper.findAllActive()).thenReturn(List.of());
        when(pendingPageViewMapper.findAllPending()).thenReturn(List.of());
        when(processedInputMapper.findByOffset(anyString(), anyInt(), anyLong()))
                .thenReturn(Optional.empty());
        when(clickStateMapper.classifyExisting(any(AdClickEvent.class)))
                .thenReturn(Optional.empty());
        when(clickStateMapper.insertIfAbsent(anyString(), any(AdClickEvent.class)))
                .thenReturn(ClickStateMapper.INSERTED);
        when(clickStore.findAttributableClick(anyString(), any(Instant.class)))
                .thenReturn(null);
        when(pendingPageViewMapper.insertIfAbsent(anyString(), any(PageViewEvent.class)))
                .thenReturn(true);
        when(clickStore.evictOldClicks(any(Instant.class))).thenReturn(0);

        engine = new JoinEngine(
                clickStore,
                watermarkTracker,
                outputSink,
                stateAccessLock,
                clickStateMapper,
                watermarkStateMapper,
                pendingPageViewMapper,
                processedInputMapper,
                transactionTemplate
        );
    }

    /**
     * getPartitionLock()
     *
     * 1.1 lock null -> create
     * 1.2 lock not null -> return lock (multiple times -> same lock)
     * 2.1 different partition -> different lock
     * */

    @Test
    void getPartitionLock_missingLock_createsLock() {
        ReentrantLock lock = invoke(
                "getPartitionLock",
                new Class<?>[]{int.class},
                PARTITION
        );

        assertNotNull(lock);
    }

    @Test
    void getPartitionLock_existingLock_returnsSameLock() {
        ReentrantLock first = invoke(
                "getPartitionLock",
                new Class<?>[]{int.class},
                PARTITION
        );
        ReentrantLock second = invoke(
                "getPartitionLock",
                new Class<?>[]{int.class},
                PARTITION
        );

        assertSame(first, second);
    }

    /**
     * restoreState()
     *
     * 1.1 data empty -> no restore
     * 1.2 data not empty -> restore
     * 2.1 pending data -> restore + find attribution
     * */

    @Test
    void restoreState_emptyData_skipsRestore() {
        engine.restoreState();

        verifyNoInteractions(watermarkTracker, clickStore, outputSink);
    }

    @Test
    void restoreState_nonEmptyData_restoresState() {
        WatermarkStateMapper.WatermarkState state = new WatermarkStateMapper.WatermarkState(
                PARTITION,
                BASE_TIME,
                "OBSERVED",
                BASE_TIME,
                BASE_TIME
        );
        AdClickEvent click = click("click-restore", BASE_TIME.minusSeconds(30));
        PageViewEvent first = pageView("pv-restore-1", BASE_TIME.plusSeconds(1));
        PageViewEvent second = pageView("pv-restore-2", BASE_TIME.plusSeconds(2));

        when(watermarkStateMapper.findAll()).thenReturn(List.of(state));
        when(clickStateMapper.findAllActive()).thenReturn(List.of(click));
        when(pendingPageViewMapper.findAllPending()).thenReturn(List.of(first, second));

        engine.restoreState();

        verify(watermarkTracker).restoreWatermark(PARTITION, BASE_TIME);
        verify(clickStore).restoreClick(click);
        assertEquals(List.of(first, second), pendingViews()
                .get(PARTITION).getPageviews().stream().toList());
        verifyNoInteractions(outputSink);
    }

    /**
     * processClick()
     *
     * 1.1 transaction success -> apply memory changes
     * 1.2 transaction failure -> skip memory changes
     * */

    @Test
    void processClick_transactionSuccess_appliesMemoryChanges() {
        AdClickEvent click = click("click-1", BASE_TIME);
        when(watermarkTracker.getWatermark(PARTITION)).thenReturn(BASE_TIME);
        doAnswer(invocation -> "processed")
                .when(transactionTemplate).execute(any(TransactionCallback.class));

        assertEquals("processed", engine.processClick(click));

        verify(readLock).lock();
        verify(readLock).unlock();
    }

    @Test
    void processClick_transactionFailure_skipsMemoryChanges() {
        AdClickEvent click = click("click-1", BASE_TIME);
        RuntimeException failure = new RuntimeException("transaction failed");
        when(watermarkTracker.getWatermark(PARTITION)).thenReturn(BASE_TIME);
        doAnswer(invocation -> {
            throw failure;
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> engine.processClick(click));

        assertSame(failure, actual);
        verify(readLock).lock();
        verify(readLock).unlock();
    }

    /**
     * processClickInTransaction()
     *
     * 1.1 processed input exists -> return empty
     * 1.2 no processed input -> continue
     * 2.1 existing click status exists -> handleExistingClick()
     * 2.2 no existing click status -> continue
     * 3.1 event_time < watermark -> late record, return
     * 3.2 event_time >= watermark -> insert
     * 4.1 insert status != INSERTED -> handleExistingClick()
     * 4.2 INSERTED -> stage click/watermark
     * 5.1 watermarkBefore > eventWatermark -> keep old
     * 5.2 watermarkBefore <= eventWatermark -> use new
     * */

    @Test
    void processClickInTransaction_processedInputExists_returnsEmpty() {
        AdClickEvent click = click("click-processed", BASE_TIME);
        when(processedInputMapper.findByOffset("ad_clicks", PARTITION, click.getOffset()))
                .thenReturn(Optional.of(processedInput(click)));

        String result = invokeClickTransaction(click, BASE_TIME, mock(MemoryStateChanges.class));

        assertEquals("", result);
        verifyNoInteractions(clickStateMapper, watermarkStateMapper, clickStore);
    }

    @Test
    void processClickInTransaction_noProcessedInput_continues() {
        AdClickEvent click = click("click-no-processed", BASE_TIME);

        String result = invokeClickTransaction(click, BASE_TIME, mock(MemoryStateChanges.class));

        assertEquals("", result);
        verify(processedInputMapper).findByOffset("ad_clicks", PARTITION, click.getOffset());
        verify(clickStateMapper).classifyExisting(click);
    }

    @Test
    void processClickInTransaction_existingClickStatusExists_handlesExistingClick() {
        AdClickEvent click = click("click-existing", BASE_TIME);
        when(clickStateMapper.classifyExisting(click))
                .thenReturn(Optional.of(ClickStateMapper.REPLAY));

        String result = invokeClickTransaction(click, BASE_TIME, mock(MemoryStateChanges.class));

        assertEquals(ClickStateMapper.REPLAY, result);
        verify(processedInputMapper).insertProcessedRecord(
                "ad_clicks", PARTITION, click.getOffset(),
                "ad_clicks", click.getClickId(), click.getEventTime()
        );
    }

    @Test
    void processClickInTransaction_noExistingClickStatus_continues() {
        AdClickEvent click = click("click-no-existing", BASE_TIME);
        when(clickStateMapper.insertIfAbsent("ad_clicks", click))
                .thenReturn(ClickStateMapper.CONFLICT);

        String result = invokeClickTransaction(click, BASE_TIME, mock(MemoryStateChanges.class));

        assertEquals(ClickStateMapper.CONFLICT, result);
        verify(clickStateMapper).classifyExisting(click);
        verify(clickStateMapper).insertIfAbsent("ad_clicks", click);
    }

    @Test
    void processClickInTransaction_lateClick_returnsAfterRecordingLateInput() {
        AdClickEvent click = click("click-late", BASE_TIME.minusSeconds(1));

        String result = invokeClickTransaction(click, BASE_TIME, mock(MemoryStateChanges.class));

        assertEquals("", result);
        verify(processedInputMapper).insertLateRecord(
                "ad_clicks", PARTITION, click.getOffset(),
                "ad_clicks", click.getClickId(), click.getEventTime()
        );
        verify(clickStateMapper, org.mockito.Mockito.never())
                .insertIfAbsent(anyString(), any(AdClickEvent.class));
    }

    @Test
    void processClickInTransaction_onTimeClick_inserts() {
        AdClickEvent click = click("click-on-time", BASE_TIME);
        MemoryStateChanges memoryChanges = mock(MemoryStateChanges.class);

        String result = invokeClickTransaction(click, BASE_TIME, memoryChanges);

        assertEquals("", result);
        verify(clickStateMapper).insertIfAbsent("ad_clicks", click);
        verify(memoryChanges).stageClick(click);
        verify(memoryChanges).stageWatermark(click.getEventTime());
    }

    @Test
    void processClickInTransaction_nonInsertedStatus_handlesExistingClick() {
        AdClickEvent click = click("click-non-inserted", BASE_TIME);
        when(clickStateMapper.insertIfAbsent("ad_clicks", click))
                .thenReturn(ClickStateMapper.CONFLICT);

        String result = invokeClickTransaction(click, BASE_TIME, mock(MemoryStateChanges.class));

        assertEquals(ClickStateMapper.CONFLICT, result);
        verify(processedInputMapper, org.mockito.Mockito.never())
                .insertProcessedRecord(anyString(), anyInt(), anyLong(),
                        anyString(), anyString(), any(Instant.class));
    }

    @Test
    void processClickInTransaction_insertedStatus_stagesClickAndWatermark() {
        AdClickEvent click = click("click-inserted", BASE_TIME);
        MemoryStateChanges memoryChanges = mock(MemoryStateChanges.class);

        invokeClickTransaction(click, minusMinutes(BASE_TIME, 1), memoryChanges);

        verify(watermarkStateMapper).upsertObserved(
                eq(PARTITION), eq(click.getEventTime()), any(Instant.class)
        );
        verify(memoryChanges).stageClick(click);
        verify(memoryChanges).stageWatermark(click.getEventTime());
        verify(processedInputMapper).insertProcessedRecord(
                "ad_clicks", PARTITION, click.getOffset(),
                "ad_clicks", click.getClickId(), click.getEventTime()
        );
    }

    @Test
    void processClickInTransaction_olderEventWatermark_keepsOldWatermark() {
        AdClickEvent click = click("click-old-watermark", BASE_TIME);
        PageViewEvent pending = pageView("pv-old-watermark", BASE_TIME.minusSeconds(90));
        pendingViews().put(PARTITION, pendingState(pending));

        invokeClickTransaction(click, BASE_TIME, mock(MemoryStateChanges.class));

        verify(outputSink).write(any(AttributedPageView.class));
        verify(pendingPageViewMapper).markEmitted(pending.getEventId());
    }

    @Test
    void processClickInTransaction_newerOrEqualEventWatermark_usesNewWatermark() {
        AdClickEvent click = click("click-new-watermark", BASE_TIME);
        PageViewEvent pending = pageView("pv-new-watermark", BASE_TIME.minusSeconds(90));
        pendingViews().put(PARTITION, pendingState(pending));

        invokeClickTransaction(click, minusMinutes(BASE_TIME, 3), mock(MemoryStateChanges.class));

        verifyNoInteractions(outputSink);
        assertTrue(pendingViews().get(PARTITION).getPageviews().contains(pending));
    }

    /**
     * handleExistingClick()
     *
     * 1.1 status == REPLAY/DUPLICATE -> processed record, return status
     * 1.2 status == CONFLICT -> return conflict
     * 1.3 status other -> throw
     * */

    @Test
    void handleExistingClick_replayOrDuplicate_recordsProcessedAndReturnsStatus() {
        AdClickEvent replay = click("click-replay", BASE_TIME);
        AdClickEvent duplicate = click("click-duplicate", BASE_TIME.plusSeconds(1));

        assertEquals(ClickStateMapper.REPLAY,
                invokeHandleExisting(ClickStateMapper.REPLAY, replay));
        assertEquals(ClickStateMapper.DUPLICATE,
                invokeHandleExisting(ClickStateMapper.DUPLICATE, duplicate));

        verify(processedInputMapper).insertProcessedRecord(
                "ad_clicks", PARTITION, replay.getOffset(),
                "ad_clicks", replay.getClickId(), replay.getEventTime()
        );
        verify(processedInputMapper).insertProcessedRecord(
                "ad_clicks", PARTITION, duplicate.getOffset(),
                "ad_clicks", duplicate.getClickId(), duplicate.getEventTime()
        );
    }

    @Test
    void handleExistingClick_conflict_returnsConflict() {
        String result = invokeHandleExisting(
                ClickStateMapper.CONFLICT,
                click("click-conflict", BASE_TIME)
        );

        assertEquals(ClickStateMapper.CONFLICT, result);
        verifyNoInteractions(processedInputMapper);
    }

    @Test
    void handleExistingClick_unsupportedStatus_throws() {
        assertThrows(IllegalStateException.class,
                () -> invokeHandleExisting("UNSUPPORTED", click("click-unsupported", BASE_TIME)));
    }

    /**
     * processPageView()
     *
     * 1.1 transaction success -> apply memory changes
     * 1.2 transaction failure -> skip memory changes
     * */

    @Test
    void processPageView_transactionSuccess_appliesMemoryChanges() {
        PageViewEvent pageView = pageView("pv-1", BASE_TIME);
        when(watermarkTracker.getWatermark(PARTITION)).thenReturn(BASE_TIME);

        org.mockito.Mockito.doNothing().when(transactionTemplate)
                .executeWithoutResult(any(java.util.function.Consumer.class));

        engine.processPageView(pageView);

        verify(readLock).lock();
        verify(readLock).unlock();
    }

    @Test
    void processPageView_transactionFailure_skipsMemoryChanges() {
        PageViewEvent pageView = pageView("pv-1", BASE_TIME);
        RuntimeException failure = new RuntimeException("transaction failed");
        when(watermarkTracker.getWatermark(PARTITION)).thenReturn(BASE_TIME);
        org.mockito.Mockito.doThrow(failure).when(transactionTemplate)
                .executeWithoutResult(any(java.util.function.Consumer.class));

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> engine.processPageView(pageView));

        assertSame(failure, actual);
        verify(readLock).lock();
        verify(readLock).unlock();
    }

    /**
     * processPageViewInTransaction()
     *
     * 1.1 event_time < watermark -> late record, return
     * 1.2 event_time >= watermark -> insert pending
     * 2.1 watermarkBefore > eventWatermark -> keep old
     * 2.2 watermarkBefore <= eventWatermark -> use new
     * */

    @Test
    void processPageViewInTransaction_latePageView_returnsAfterRecordingLateInput() {
        PageViewEvent pageView = pageView("pv-late", BASE_TIME.minusSeconds(1));

        invokePageViewTransaction(pageView, BASE_TIME, mock(MemoryStateChanges.class));

        verify(processedInputMapper).insertLateRecord(
                "page_views", PARTITION, pageView.getOffset(),
                "page_views", pageView.getEventId(), pageView.getEventTime()
        );
        verify(pendingPageViewMapper, org.mockito.Mockito.never())
                .insertIfAbsent(anyString(), any(PageViewEvent.class));
    }

    @Test
    void processPageViewInTransaction_onTimePageView_insertsPendingPageView() {
        PageViewEvent pageView = pageView("pv-on-time", BASE_TIME);
        MemoryStateChanges memoryChanges = mock(MemoryStateChanges.class);

        invokePageViewTransaction(pageView, Instant.MIN, memoryChanges);

        verify(pendingPageViewMapper).insertIfAbsent("page_views", pageView);
        verify(watermarkStateMapper).upsertObserved(
                eq(PARTITION), eq(pageView.getEventTime()), any(Instant.class)
        );
        verify(memoryChanges).stagePendingPageView(pageView);
        verify(memoryChanges).stageWatermark(pageView.getEventTime());
        verify(processedInputMapper).insertProcessedRecord(
                "page_views", PARTITION, pageView.getOffset(),
                "page_views", pageView.getEventId(), pageView.getEventTime()
        );
    }

    @Test
    void processPageViewInTransaction_olderEventWatermark_keepsOldWatermark() {
        PageViewEvent pageView = pageView("pv-old-watermark", BASE_TIME.minusSeconds(90));

        invokePageViewTransaction(pageView, minusSeconds(BASE_TIME, 90),
                mock(MemoryStateChanges.class));

        verify(outputSink).write(any(AttributedPageView.class));
        verify(pendingPageViewMapper).markEmitted(pageView.getEventId());
    }

    @Test
    void processPageViewInTransaction_newerOrEqualEventWatermark_usesNewWatermark() {
        PageViewEvent pageView = pageView("pv-new-watermark", BASE_TIME.minusSeconds(90));

        invokePageViewTransaction(pageView, minusMinutes(BASE_TIME, 3),
                mock(MemoryStateChanges.class));

        verifyNoInteractions(outputSink);
    }

    /**
     * findAttributionForPendingViews()
     *
     * 1.1 pending == null && pageViewToAdd == null -> return
     * 1.2 pending != null || pageViewToAdd != null -> continue
     * 2.1 pending != null -> add pending page_views
     * 2.2 pending == null -> skip pending page_views
     * 3.1 pageViewToAdd != null -> add current page_view
     * 3.2 pageViewToAdd == null -> skip current page_view
     * 4.1 candidates empty -> return
     * 4.2 candidates != empty -> loop
     * 5.1 page_view time > watermark -> break
     * 5.2 page_view time <= watermark -> attribution/output
     * 6.1 clickToAdd candidate -> check newer click
     * 6.2 clickToAdd not candidate -> keep stored attribution
     * 7.1 clickToAdd newer / no stored click -> use new click
     * 7.2 clickToAdd <= stored click -> keep stored click
     * 8.1 attributableClick == null -> null fields
     * 8.2 attributableClick != null -> campaign/click IDs
     * 9.1 memoryChanges != null -> stage remove
     * 9.2 memoryChanges == null -> remove live pending
     * 9.3 pending != null -> remove page_view
     * 9.4 pending == null -> skip remove
     * 10.1 pending != null && empty -> remove partition
     * 10.2 pending == null || not empty -> keep partition
     * */

    @Test
    void findAttributionForPendingViews_noPendingAndNoCurrentPageView_returns() {
        invokeFind(PARTITION, BASE_TIME, null, null, null);

        verifyNoInteractions(clickStore, outputSink, pendingPageViewMapper);
    }

    @Test
    void findAttributionForPendingViews_pendingState_addsPendingPageViews() {
        PageViewEvent pending = pageView("pv-pending", BASE_TIME.minusSeconds(1));
        pendingViews().put(PARTITION, pendingState(pending));
        MemoryStateChanges memoryChanges = mock(MemoryStateChanges.class);

        invokeFind(PARTITION, BASE_TIME, memoryChanges, null, null);

        verify(clickStore).findAttributableClick("user-1", pending.getEventTime());
        verify(outputSink).write(any(AttributedPageView.class));
        verify(memoryChanges).stageRemovePendingPageView(pending);
    }

    @Test
    void findAttributionForPendingViews_currentPageViewExists_addsCurrentPageView() {
        PageViewEvent current = pageView("pv-current", BASE_TIME.minusSeconds(1));
        MemoryStateChanges memoryChanges = mock(MemoryStateChanges.class);

        invokeFind(PARTITION, BASE_TIME, memoryChanges, current, null);

        verify(clickStore).findAttributableClick("user-1", current.getEventTime());
        verify(outputSink).write(any(AttributedPageView.class));
        verify(memoryChanges).stageRemovePendingPageView(current);
    }

    @Test
    void findAttributionForPendingViews_emptyCandidates_returns() {
        pendingViews().put(PARTITION, new PendingPageview());

        invokeFind(PARTITION, BASE_TIME, null, null, null);

        verifyNoInteractions(clickStore, outputSink, pendingPageViewMapper);
    }

    @Test
    void findAttributionForPendingViews_pageViewAfterWatermark_breaksLoop() {
        PageViewEvent future = pageView("pv-future", BASE_TIME.plusSeconds(1));

        invokeFind(PARTITION, BASE_TIME, mock(MemoryStateChanges.class), future, null);

        verifyNoInteractions(clickStore, outputSink, pendingPageViewMapper);
    }

    @Test
    void findAttributionForPendingViews_pageViewAtOrBeforeWatermark_writesAttribution() {
        PageViewEvent pageView = pageView("pv-at-watermark", BASE_TIME);
        MemoryStateChanges memoryChanges = mock(MemoryStateChanges.class);

        invokeFind(PARTITION, BASE_TIME, memoryChanges, pageView, null);

        ArgumentCaptor<AttributedPageView> output = captureOutput();
        assertEquals(pageView.getEventId(), output.getValue().getPageViewId());
        assertNull(output.getValue().getAttributedCampaignId());
        assertNull(output.getValue().getAttributedClickId());
        verify(pendingPageViewMapper).markEmitted(pageView.getEventId());
        verify(memoryChanges).stageRemovePendingPageView(pageView);
    }

    @Test
    void findAttributionForPendingViews_newClickIsCandidate_checksNewerClick() {
        PageViewEvent pageView = pageView("pv-new-candidate", BASE_TIME);
        AdClickEvent stored = click("click-stored", minusMinutes(BASE_TIME, 10));
        AdClickEvent incoming = click("click-incoming", minusMinutes(BASE_TIME, 5));
        when(clickStore.findAttributableClick("user-1", BASE_TIME)).thenReturn(stored);

        invokeFind(PARTITION, BASE_TIME, mock(MemoryStateChanges.class), pageView, incoming);

        ArgumentCaptor<AttributedPageView> output = captureOutput();
        assertEquals(incoming.getClickId(), output.getValue().getAttributedClickId());
    }

    @Test
    void findAttributionForPendingViews_newClickIsNotCandidate_keepsStoredAttribution() {
        PageViewEvent pageView = pageView("pv-not-candidate", BASE_TIME);
        AdClickEvent stored = click("click-stored", minusMinutes(BASE_TIME, 5));
        AdClickEvent incoming = click("click-other-user", "user-2", minusMinutes(BASE_TIME, 1), "campaign-2");
        when(clickStore.findAttributableClick("user-1", BASE_TIME)).thenReturn(stored);

        invokeFind(PARTITION, BASE_TIME, mock(MemoryStateChanges.class), pageView, incoming);

        ArgumentCaptor<AttributedPageView> output = captureOutput();
        assertEquals(stored.getClickId(), output.getValue().getAttributedClickId());
    }

    @Test
    void findAttributionForPendingViews_newClickIsNewerOrStoredClickMissing_usesNewClick() {
        PageViewEvent pageView = pageView("pv-no-stored", BASE_TIME);
        AdClickEvent incoming = click("click-incoming", minusMinutes(BASE_TIME, 1));

        invokeFind(PARTITION, BASE_TIME, mock(MemoryStateChanges.class), pageView, incoming);

        ArgumentCaptor<AttributedPageView> output = captureOutput();
        assertEquals(incoming.getClickId(), output.getValue().getAttributedClickId());
    }

    @Test
    void findAttributionForPendingViews_newClickIsOlderOrEqual_keepsStoredClick() {
        PageViewEvent pageView = pageView("pv-equal-click", BASE_TIME);
        AdClickEvent stored = click("click-stored", minusMinutes(BASE_TIME, 5));
        AdClickEvent incoming = click("click-incoming", minusMinutes(BASE_TIME, 5));
        when(clickStore.findAttributableClick("user-1", BASE_TIME)).thenReturn(stored);

        invokeFind(PARTITION, BASE_TIME, mock(MemoryStateChanges.class), pageView, incoming);

        ArgumentCaptor<AttributedPageView> output = captureOutput();
        assertEquals(stored.getClickId(), output.getValue().getAttributedClickId());
    }

    @Test
    void findAttributionForPendingViews_noAttributableClick_writesNullFields() {
        PageViewEvent pageView = pageView("pv-no-click", BASE_TIME);

        invokeFind(PARTITION, BASE_TIME, mock(MemoryStateChanges.class), pageView, null);

        ArgumentCaptor<AttributedPageView> output = captureOutput();
        assertNull(output.getValue().getAttributedCampaignId());
        assertNull(output.getValue().getAttributedClickId());
    }

    @Test
    void findAttributionForPendingViews_attributableClickExists_writesCampaignAndClickIds() {
        PageViewEvent pageView = pageView("pv-with-click", BASE_TIME);
        AdClickEvent stored = click("click-stored", minusMinutes(BASE_TIME, 5));
        when(clickStore.findAttributableClick("user-1", BASE_TIME)).thenReturn(stored);

        invokeFind(PARTITION, BASE_TIME, mock(MemoryStateChanges.class), pageView, null);

        ArgumentCaptor<AttributedPageView> output = captureOutput();
        assertEquals(stored.getCampaignId(), output.getValue().getAttributedCampaignId());
        assertEquals(stored.getClickId(), output.getValue().getAttributedClickId());
    }

    @Test
    void findAttributionForPendingViews_memoryChangesExists_stagesRemoval() {
        PageViewEvent pageView = pageView("pv-stage-remove", BASE_TIME);
        MemoryStateChanges memoryChanges = mock(MemoryStateChanges.class);

        invokeFind(PARTITION, BASE_TIME, memoryChanges, pageView, null);

        verify(memoryChanges).stageRemovePendingPageView(pageView);
        verify(pendingPageViewMapper).markEmitted(pageView.getEventId());
    }

    @Test
    void findAttributionForPendingViews_memoryChangesMissing_removesLivePendingPageView() {
        PageViewEvent handled = pageView("pv-handled", BASE_TIME.minusSeconds(1));
        PageViewEvent retained = pageView("pv-retained", BASE_TIME.plusSeconds(1));
        pendingViews().put(PARTITION, pendingState(handled, retained));

        invokeFind(PARTITION, BASE_TIME, null, null, null);

        assertTrue(pendingViews().containsKey(PARTITION));
        assertEquals(List.of(retained), pendingViews().get(PARTITION)
                .getPageviews().stream().toList());
    }

    @Test
    void findAttributionForPendingViews_pendingStateMissing_skipsRemoval() {
        PageViewEvent current = pageView("pv-no-pending", BASE_TIME);

        invokeFind(PARTITION, BASE_TIME, null, current, null);

        assertFalse(pendingViews().containsKey(PARTITION));
        verify(outputSink).write(any(AttributedPageView.class));
    }

    @Test
    void findAttributionForPendingViews_pendingStateEmpty_removesPartition() {
        PageViewEvent handled = pageView("pv-last", BASE_TIME);
        pendingViews().put(PARTITION, pendingState(handled));

        invokeFind(PARTITION, BASE_TIME, null, null, null);

        assertFalse(pendingViews().containsKey(PARTITION));
    }

    /**
     * evictOldClicks()
     *
     * 1.1 global watermark == Instant.MIN -> return
     * 1.2 global watermark != Instant.MIN -> calculate cutoff
     * 2.1 transaction success -> mark database clicks, evict memory clicks
     * 2.2 transaction failure -> skip memory eviction
     * */

    @Test
    void evictOldClicks_globalWatermarkUninitialized_returns() {
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(Instant.MIN);

        engine.evictOldClicks();

        verify(writeLock).lock();
        verify(writeLock).unlock();
        verifyNoInteractions(transactionTemplate, clickStateMapper, clickStore);
    }

    @Test
    void evictOldClicks_globalWatermarkInitialized_calculatesCutoff() {
        Instant globalWatermark = BASE_TIME;
        Instant expectedCutoff = BASE_TIME.minus(Duration.ofMinutes(32));
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(globalWatermark);
        executeTransactionCallback();

        engine.evictOldClicks();

        verify(clickStateMapper).markOlderThanEvicted(expectedCutoff);
        verify(clickStore).evictOldClicks(expectedCutoff);
    }

    @Test
    void evictOldClicks_transactionSuccess_marksDatabaseAndEvictsMemory() {
        Instant globalWatermark = plusMinutes(BASE_TIME, 1);
        Instant expectedCutoff = globalWatermark.minus(Duration.ofMinutes(32));
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(globalWatermark);
        executeTransactionCallback();
        when(clickStore.evictOldClicks(expectedCutoff)).thenReturn(2);

        engine.evictOldClicks();

        verify(clickStateMapper).markOlderThanEvicted(expectedCutoff);
        verify(clickStore).evictOldClicks(expectedCutoff);
        verify(writeLock).unlock();
    }

    @Test
    void evictOldClicks_transactionFailure_skipsMemoryEviction() {
        Instant globalWatermark = BASE_TIME;
        RuntimeException failure = new RuntimeException("eviction failed");
        when(watermarkTracker.getGlobalMinimumWatermark()).thenReturn(globalWatermark);
        doAnswer(invocation -> {
            throw failure;
        }).when(transactionTemplate).execute(any(TransactionCallback.class));

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> engine.evictOldClicks());

        assertSame(failure, actual);
        verify(clickStore, org.mockito.Mockito.never()).evictOldClicks(any(Instant.class));
        verify(writeLock).unlock();
    }

    private String invokeClickTransaction(AdClickEvent click, Instant watermarkBefore,
                                          MemoryStateChanges memoryChanges) {
        return invoke(
                "processClickInTransaction",
                new Class<?>[]{int.class, AdClickEvent.class, Instant.class, MemoryStateChanges.class},
                PARTITION, click, watermarkBefore, memoryChanges
        );
    }

    private void invokePageViewTransaction(PageViewEvent pageView, Instant watermarkBefore,
                                            MemoryStateChanges memoryChanges) {
        invoke(
                "processPageViewInTransaction",
                new Class<?>[]{int.class, PageViewEvent.class, Instant.class, MemoryStateChanges.class},
                PARTITION, pageView, watermarkBefore, memoryChanges
        );
    }

    private String invokeHandleExisting(String status, AdClickEvent click) {
        return invoke(
                "handleExistingClick",
                new Class<?>[]{String.class, AdClickEvent.class},
                status, click
        );
    }

    private void invokeFind(int partition, Instant watermark, MemoryStateChanges memoryChanges,
                            PageViewEvent pageViewToAdd, AdClickEvent clickToAdd) {
        invoke(
                "findAttributionForPendingViews",
                new Class<?>[]{int.class, Instant.class, MemoryStateChanges.class,
                        PageViewEvent.class, AdClickEvent.class},
                partition, watermark, memoryChanges, pageViewToAdd, clickToAdd
        );
    }

    @SuppressWarnings("unchecked")
    private <T> T invoke(String methodName, Class<?>[] parameterTypes, Object... args) {
        try {
            Method method = JoinEngine.class.getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return (T) method.invoke(engine, args);
        } catch (InvocationTargetException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new AssertionError(cause);
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<Integer, PendingPageview> pendingViews() {
        return (ConcurrentHashMap<Integer, PendingPageview>) ReflectionTestUtils
                .getField(engine, "pendingPageView");
    }

    private ArgumentCaptor<AttributedPageView> captureOutput() {
        ArgumentCaptor<AttributedPageView> output = ArgumentCaptor.forClass(AttributedPageView.class);
        verify(outputSink).write(output.capture());
        return output;
    }

    private void executeTransactionCallback() {
        doAnswer(invocation -> {
            TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.doInTransaction(null);
        }).when(transactionTemplate).execute(any(TransactionCallback.class));
    }

    private static PendingPageview pendingState(PageViewEvent... pageViews) {
        PendingPageview pending = new PendingPageview();
        for (PageViewEvent pageView : pageViews) {
            pending.add(pageView);
        }
        return pending;
    }

    private static ProcessedInputMapper.ProcessedInput processedInput(AdClickEvent click) {
        return new ProcessedInputMapper.ProcessedInput(
                "ad_clicks",
                click.getPartition(),
                click.getOffset(),
                "ad_click",
                click.getClickId(),
                click.getEventTime(),
                "PROCESSED",
                1,
                BASE_TIME,
                BASE_TIME
        );
    }

    private static AdClickEvent click(String clickId, Instant eventTime) {
        return click(clickId, "user-1", eventTime, "campaign-1");
    }

    private static AdClickEvent click(String clickId, String userId, Instant eventTime,
                                      String campaignId) {
        AdClickEvent click = AdClickEvent.builder()
                .clickId(clickId)
                .userId(userId)
                .eventTime(eventTime)
                .campaignId(campaignId)
                .build();
        click.setPartition(PARTITION);
        click.setOffset(10L);
        return click;
    }

    private static PageViewEvent pageView(String eventId, Instant eventTime) {
        PageViewEvent pageView = PageViewEvent.builder()
                .eventId(eventId)
                .userId("user-1")
                .eventTime(eventTime)
                .url("https://example.com/" + eventId)
                .build();
        pageView.setPartition(PARTITION);
        pageView.setOffset(20L);
        return pageView;
    }

    private static Instant plusMinutes(Instant instant, long minutes) {
        return instant.plusSeconds(minutes * 60);
    }

    private static Instant minusMinutes(Instant instant, long minutes) {
        return instant.minusSeconds(minutes * 60);
    }

    private static Instant minusSeconds(Instant instant, long seconds) {
        return instant.minusSeconds(seconds);
    }
}
