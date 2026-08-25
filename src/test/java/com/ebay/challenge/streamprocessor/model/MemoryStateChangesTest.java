package com.ebay.challenge.streamprocessor.model;

import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.mockito.InOrder;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class MemoryStateChangesTest {

    private static final int PARTITION = 3;
    private static final Instant BASE_TIME = Instant.parse("2026-08-24T12:00:00Z");

    /**
     * MemoryStateChanges()
     *
     * 1.1 stores the partition used for later watermark and pending-page-view updates
     * */

    @Test
    void constructor_storesPartitionForLaterApplication() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        WatermarkTracker watermarkTracker = new WatermarkTracker(0);

        changes.stageWatermark(BASE_TIME);
        changes.applyToMemory(new ClickStateStore(), watermarkTracker, new ConcurrentHashMap<>());

        assertEquals(BASE_TIME, watermarkTracker.getWatermark(PARTITION));
        assertEquals(Instant.MIN, watermarkTracker.getWatermark(PARTITION + 1));
    }

    /**
     * stageClick()
     *
     * 1.1 stage one click for later application
     * 1.2 stage multiple clicks and apply them in staging order
     * */

    @Test
    void stageClick_stagesOneClickForLaterApplication() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        ClickStateStore clickStore = new ClickStateStore();
        AdClickEvent click = click("click-1", BASE_TIME);

        changes.stageClick(click);
        changes.applyToMemory(clickStore, new WatermarkTracker(0), new ConcurrentHashMap<>());

        assertEquals(1, clickStore.getTotalClickCount());
        assertSame(click, clickStore.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void stageClick_stagesMultipleClicksInOrder() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        ClickStateStore clickStore = mock(ClickStateStore.class);
        WatermarkTracker watermarkTracker = mock(WatermarkTracker.class);
        AdClickEvent first = click("click-1", BASE_TIME);
        AdClickEvent second = click("click-2", plusSeconds(BASE_TIME, 1));

        changes.stageClick(first);
        changes.stageClick(second);
        changes.applyToMemory(clickStore, watermarkTracker, new ConcurrentHashMap<>());

        InOrder inOrder = inOrder(clickStore);
        inOrder.verify(clickStore).addClick(first);
        inOrder.verify(clickStore).addClick(second);
    }

    /**
     * stageWatermark()
     *
     * 1.1 null event time is ignored
     * 2.1 event_time -> watermark
     * 2.2 earlier event_time doesnot move watermark
     * */

    @Test
    void stageWatermark_nullEventTimeIsIgnored() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        WatermarkTracker watermarkTracker = mock(WatermarkTracker.class);

        changes.stageWatermark(null);
        changes.applyToMemory(mock(ClickStateStore.class), watermarkTracker, new ConcurrentHashMap<>());

        verifyNoInteractions(watermarkTracker);
    }

    @Test
    void stageWatermark_eventTimeBecomesWatermark() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        WatermarkTracker watermarkTracker = new WatermarkTracker(0);

        changes.stageWatermark(BASE_TIME);
        changes.applyToMemory(new ClickStateStore(), watermarkTracker, new ConcurrentHashMap<>());

        assertEquals(BASE_TIME, watermarkTracker.getWatermark(PARTITION));
    }

    @Test
    void stageWatermark_earlierEventTimeDoesNotMoveWatermark() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        WatermarkTracker watermarkTracker = new WatermarkTracker(0);

        changes.stageWatermark(plusSeconds(BASE_TIME, 10));
        changes.stageWatermark(BASE_TIME);
        changes.applyToMemory(new ClickStateStore(), watermarkTracker, new ConcurrentHashMap<>());

        assertEquals(plusSeconds(BASE_TIME, 10), watermarkTracker.getWatermark(PARTITION));
    }

    /**
     * stagePendingPageView()
     *
     * 1.1 stage 1 pending page view
     * 1.2 stage multiple page views
     * 1.3 duplicate page views are ordered
     * */

    @Test
    void stagePendingPageView_stagesOnePageView() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        PageViewEvent pageView = pageView("pv-1", BASE_TIME);
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = new ConcurrentHashMap<>();

        changes.stagePendingPageView(pageView);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertTrue(pendingPageViews.containsKey(PARTITION));
        assertEquals(List.of(pageView), pendingPageViews.get(PARTITION).getPageviews().stream().toList());
    }

    @Test
    void stagePendingPageView_stagesMultiplePageViews() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        PageViewEvent first = pageView("pv-1", BASE_TIME);
        PageViewEvent second = pageView("pv-2", plusSeconds(BASE_TIME, 1));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = new ConcurrentHashMap<>();

        changes.stagePendingPageView(first);
        changes.stagePendingPageView(second);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertEquals(List.of(first, second), pendingPageViews.get(PARTITION).getPageviews().stream().toList());
    }

    @Test
    void stagePendingPageView_duplicatePageViewsAreHandledByPendingState() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        PageViewEvent later = pageView("pv-b", BASE_TIME);
        PageViewEvent duplicate = pageView("pv-b", BASE_TIME);
        PageViewEvent sameTimeEarlierId = pageView("pv-a", BASE_TIME);
        PageViewEvent earlier = pageView("pv-early", minusSeconds(BASE_TIME, 1));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = new ConcurrentHashMap<>();

        changes.stagePendingPageView(later);
        changes.stagePendingPageView(duplicate);
        changes.stagePendingPageView(sameTimeEarlierId);
        changes.stagePendingPageView(earlier);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertEquals(List.of(earlier, sameTimeEarlierId, later),
                pendingPageViews.get(PARTITION).getPageviews().stream().toList());
    }

    /**
     * stageRemovePendingPageView(
     *
     * 1.1 one page_view -> remove
     * 1.2 multiple page_view -> remove all matching pending page views
     * 1.3 not remove unrelated pending page views
     * */

    @Test
    void stageRemovePendingPageView_stagesOnePageViewForRemoval() {
        PageViewEvent pageView = pageView("pv-1", BASE_TIME);
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = pendingMap(pageView);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.stageRemovePendingPageView(pageView);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertFalse(pendingPageViews.containsKey(PARTITION));
    }

    @Test
    void stageRemovePendingPageView_stagesMultiplePageViewsForRemoval() {
        PageViewEvent first = pageView("pv-1", BASE_TIME);
        PageViewEvent second = pageView("pv-2", plusSeconds(BASE_TIME, 1));
        PageViewEvent retained = pageView("pv-3", plusSeconds(BASE_TIME, 2));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = pendingMap(first, second, retained);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.stageRemovePendingPageView(first);
        changes.stageRemovePendingPageView(second);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertEquals(List.of(retained), pendingPageViews.get(PARTITION).getPageviews().stream().toList());
    }

    @Test
    void stageRemovePendingPageView_doesNotRemoveUnrelatedPageViews() {
        PageViewEvent retained = pageView("pv-1", BASE_TIME);
        PageViewEvent unrelated = pageView("pv-unrelated", plusSeconds(BASE_TIME, 1));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = pendingMap(retained);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.stageRemovePendingPageView(unrelated);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertEquals(List.of(retained), pendingPageViews.get(PARTITION).getPageviews().stream().toList());
    }

    /**
     * applyToMemory()
     * */

    @Test
    void applyToMemory_appliesStagedClicksToClickStore() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        ClickStateStore clickStore = new ClickStateStore();
        AdClickEvent click = click("click-1", BASE_TIME);

        changes.stageClick(click);
        changes.applyToMemory(clickStore, new WatermarkTracker(0), new ConcurrentHashMap<>());

        assertSame(click, clickStore.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void applyToMemory_appliesStagedWatermarkToConfiguredPartition() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        WatermarkTracker watermarkTracker = new WatermarkTracker(0);

        changes.stageWatermark(BASE_TIME);
        changes.applyToMemory(new ClickStateStore(), watermarkTracker, new ConcurrentHashMap<>());

        assertEquals(BASE_TIME, watermarkTracker.getWatermark(PARTITION));
    }

    @Test
    void applyToMemory_withoutStagedWatermarkSkipsWatermarkUpdate() {
        WatermarkTracker watermarkTracker = new WatermarkTracker(0);
        watermarkTracker.updateWatermark(PARTITION, BASE_TIME);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.applyToMemory(new ClickStateStore(), watermarkTracker, new ConcurrentHashMap<>());

        assertEquals(BASE_TIME, watermarkTracker.getWatermark(PARTITION));
    }

    @Test
    void applyToMemory_createsPendingStateWhenPartitionIsAbsent() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        PageViewEvent pageView = pageView("pv-1", BASE_TIME);
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = new ConcurrentHashMap<>();

        changes.stagePendingPageView(pageView);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertTrue(pendingPageViews.containsKey(PARTITION));
        assertSame(pageView, pendingPageViews.get(PARTITION).getPageviews().first());
    }

    @Test
    void applyToMemory_addsPageViewsToExistingPendingState() {
        PageViewEvent existing = pageView("pv-existing", BASE_TIME);
        PageViewEvent added = pageView("pv-added", plusSeconds(BASE_TIME, 1));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = pendingMap(existing);
        PendingPageview pending = pendingPageViews.get(PARTITION);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.stagePendingPageView(added);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertSame(pending, pendingPageViews.get(PARTITION));
        assertEquals(List.of(existing, added), pending.getPageviews().stream().toList());
    }

    @Test
    void applyToMemory_removesMatchingPendingPageViews() {
        PageViewEvent removed = pageView("pv-removed", BASE_TIME);
        PageViewEvent retained = pageView("pv-retained", plusSeconds(BASE_TIME, 1));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = pendingMap(removed, retained);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.stageRemovePendingPageView(removed);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertTrue(pendingPageViews.containsKey(PARTITION));
        assertEquals(List.of(retained), pendingPageViews.get(PARTITION).getPageviews().stream().toList());
    }

    @Test
    void applyToMemory_removesPartitionWhenLastPendingPageViewIsRemoved() {
        PageViewEvent removed = pageView("pv-removed", BASE_TIME);
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = pendingMap(removed);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.stageRemovePendingPageView(removed);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertFalse(pendingPageViews.containsKey(PARTITION));
    }

    @Test
    void applyToMemory_retainsPartitionAndUnrelatedPageViewsAfterPartialRemoval() {
        PageViewEvent removed = pageView("pv-removed", BASE_TIME);
        PageViewEvent retained = pageView("pv-retained", plusSeconds(BASE_TIME, 1));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = pendingMap(removed, retained);
        PendingPageview pending = pendingPageViews.get(PARTITION);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);

        changes.stageRemovePendingPageView(removed);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertSame(pending, pendingPageViews.get(PARTITION));
        assertEquals(List.of(retained), pending.getPageviews().stream().toList());
    }

    @Test
    void applyToMemory_removalWithoutPendingPartitionHasNoEffect() {
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        changes.stageRemovePendingPageView(pageView("pv-1", BASE_TIME));
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = new ConcurrentHashMap<>();

        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertTrue(pendingPageViews.isEmpty());
    }

    @Test
    void applyToMemory_appliesPendingPageViewAdditionsBeforeRemovals() {
        PageViewEvent pageView = pageView("pv-1", BASE_TIME);
        MemoryStateChanges changes = new MemoryStateChanges(PARTITION);
        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = new ConcurrentHashMap<>();

        changes.stagePendingPageView(pageView);
        changes.stageRemovePendingPageView(pageView);
        changes.applyToMemory(new ClickStateStore(), new WatermarkTracker(0), pendingPageViews);

        assertFalse(pendingPageViews.containsKey(PARTITION));
    }

    private static AdClickEvent click(String clickId, Instant eventTime) {
        return AdClickEvent.builder()
                .clickId(clickId)
                .userId("user-1")
                .eventTime(eventTime)
                .campaignId("campaign-1")
                .build();
    }

    private static PageViewEvent pageView(String eventId, Instant eventTime) {
        return PageViewEvent.builder()
                .eventId(eventId)
                .userId("user-1")
                .eventTime(eventTime)
                .url("https://example.com/" + eventId)
                .build();
    }

    private static ConcurrentHashMap<Integer, PendingPageview> pendingMap(PageViewEvent... pageViews) {
        PendingPageview pending = new PendingPageview();
        for (PageViewEvent pageView : pageViews) {
            pending.add(pageView);
        }

        ConcurrentHashMap<Integer, PendingPageview> pendingPageViews = new ConcurrentHashMap<>();
        pendingPageViews.put(PARTITION, pending);
        return pendingPageViews;
    }

    private static Instant plusSeconds(Instant instant, long seconds) {
        return instant.plusSeconds(seconds);
    }

    private static Instant minusSeconds(Instant instant, long seconds) {
        return instant.minusSeconds(seconds);
    }
}
