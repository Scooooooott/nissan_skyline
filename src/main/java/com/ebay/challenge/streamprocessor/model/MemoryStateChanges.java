package com.ebay.challenge.streamprocessor.model;

import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryStateChanges {

    private final int partition;

    private Instant watermarkEventTime;

    private final List<AdClickEvent> clicksToAdd = new ArrayList<>();

    private final List<PageViewEvent> pageViewsToAdd = new ArrayList<>();

    private final Set<String> pageViewIdsToRemove = new HashSet<>();

    public MemoryStateChanges(int partition) {
        this.partition = partition;
    }

    public void stageClick(AdClickEvent click) {
        clicksToAdd.add(click);
    }

    public void stageWatermark(Instant eventTime) {
        if (eventTime == null) {
            return;
        }

        if (watermarkEventTime == null
                || eventTime.isAfter(watermarkEventTime)) {
            watermarkEventTime = eventTime;
        }
    }

    public void stagePendingPageView(PageViewEvent pageView) {
        pageViewsToAdd.add(pageView);
    }

    public void stageRemovePendingPageView(PageViewEvent pageView) {
        pageViewIdsToRemove.add(pageView.getEventId());
    }

    /**
     * After db transaction successfully executed, call this to apply changes to memory
     */
    public void applyToMemory(ClickStateStore clickStore, WatermarkTracker watermarkTracker,
            ConcurrentHashMap<Integer, PendingPageview> pendingPageView) {
        // click
        for (AdClickEvent click : clicksToAdd) {
            clickStore.addClick(click);
        }

        // watermark
        if (watermarkEventTime != null) {
            watermarkTracker.updateWatermark(partition, watermarkEventTime);
        }

        // add new pending page view
        PendingPageview pending = pendingPageView.get(partition);

        if (!pageViewsToAdd.isEmpty()) {
            if (pending == null) {
                pending = new PendingPageview();
                pendingPageView.put(partition, pending);
            }

            for (PageViewEvent pageView : pageViewsToAdd) {
                pending.add(pageView);
            }
        }

        // move penging page view with attribution
        if (pending != null && !pageViewIdsToRemove.isEmpty()) {
            pending.getPageviews().removeIf(pageView ->
                            pageViewIdsToRemove.contains(pageView.getEventId()));

            if (pending.getPageviews().isEmpty()) {
                pendingPageView.remove(partition, pending);
            }
        }
    }
}
