package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.infrastructure.StateAccessLock;
import com.ebay.challenge.streamprocessor.mapper.ClickStateMapper;
import com.ebay.challenge.streamprocessor.mapper.PendingPageViewMapper;
import com.ebay.challenge.streamprocessor.mapper.ProcessedInputMapper;
import com.ebay.challenge.streamprocessor.mapper.WatermarkStateMapper;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.model.PendingPageview;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Core join engine that performs windowed attribution joins between page views and ad clicks.
 * <p>
 * Join semantics:
 * - For each page_view, find the most recent ad_click for the same user
 *   within 30 minutes before the page view (in event time)
 * - Handle out-of-order arrivals through watermark tracking
 *
 * TODO: logs and re-check
 * TODO: add lock to partition to ensure sequential?
 * TODO: or separate the partition of click and pageview (click1, pageview1) instead of partition1?
 * <p>
 * ! IMPORTANT !
 * Based on the interface declaration and data_generator.py: This implementation assumes that both topics(click and view)
 * use the same count of partitions and the same partitioning algorithm on (and only on) user_id. Therefore, events from
 * the same user are routed to the same partition number in both topics.
 * Under this occasion, the hashmaps of [Click, PendingView, Watermark] use the keys [user_id, partition_no, partition_no].
 * <p>
 * Otherwise, if this assumption does not hold, the keys can not be set this way.
 * The implementation should:
 * use a cross-partition watermark coordination strategy (including inter-partition time watermark communication, idle
 * partition, or other methods)
 * OR
 * use [user_id, user_id, user_id] as the hashmap key. (For users who don't frequently click into the relevant pages,
 * doing so might lose some attributions about their last several minutes' pageview.)
 * Without one of these alter methods, this key setting may cause incorrect attribution or some pending page views could
 * not be finalized.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;

    private final ConcurrentHashMap<Integer, PendingPageview> pendingPageView = new ConcurrentHashMap<>();

    private final StateAccessLock stateAccessLock;

    private final ClickStateMapper clickStateMapper;
    private final WatermarkStateMapper watermarkStateMapper;
    private final PendingPageViewMapper pendingPageViewMapper;
    private final ProcessedInputMapper processedInputMapper;

    // lock for each partition
    // IMPORTANT: get stateAccessLock, then this partitionLock
    private final ConcurrentHashMap<Integer, ReentrantLock> partitionLocks = new ConcurrentHashMap<>();
    private ReentrantLock getPartitionLock(int partitionNo) {
        return partitionLocks.computeIfAbsent(partitionNo, lockNo -> new ReentrantLock(true));
    }

    /**
     * restores the memory for joinEngine
     * CALLED ONLY BY StreamConsumerStarter
     * */
    public void restoreState(){
        // todo: restore watermarks
        // todo: restore clicks later than watermarks
        // todo: restore pageviews
    }

    /**
     * Process an ad click event.
     * Store the click in state for future attribution.
     *
     * - Check if event is too late using watermarkTracker
     * - Store the click in clickStore
     * - Update watermark for the partition
     *
     * when watermark advances, find attribution for pending pageviews with event_time <= new_watermark,
     * and remove them from pending after attribution(no matter gets attribution or not)
     * @param click the ad click event
     */
    @Transactional
    public void processClick(AdClickEvent click) {
        // todo: maybe a file output for too_late_clisks?

        log.debug("Processing click: {}", click.getClickId());

        int partitionNo = click.getPartition();

        Lock readLock = stateAccessLock.readLock();
        Lock partitionLock = getPartitionLock(partitionNo);
        readLock.lock();
        partitionLock.lock();

        try {
            boolean isTooLate = watermarkTracker.isTooLate(partitionNo, click.getEventTime());
            if (isTooLate){
                log.info("Click {} is too late for partition {} to process. The newest watermark for this partition is {}. Click information is: {}",
                        click.getClickId(), partitionNo, watermarkTracker.getWatermark(partitionNo), click);
                // todo:processedInputMapper.insertTerminalRecord();
                return;
            }

            clickStateMapper.insertIfAbsent("ad_clicks", click);
            watermarkStateMapper.upsertObserved(partitionNo, click.getEventTime(), Instant.now());

            clickStore.addClick(click);
            watermarkTracker.updateWatermark(partitionNo, click.getEventTime());

            // handling pageview
            findAttributionForPendingViews(partitionNo);
        } finally {
            partitionLock.unlock();
            readLock.unlock();
        }

    }

    /**
     * Process a page view event.
     * Find matching click and emit attributed page view.
     *
     * TODO: Implement page view processing logic
     * - Check if event is too late using watermarkTracker
     * - Find attributable click from clickStore
     * - Create and emit AttributedPageView
     * - Update watermark for the partition
     *
     * @param pageView the page view event
     */
    @Transactional
    public void processPageView(PageViewEvent pageView) {
        // TODO: Keep watermark semantics consistent with the shared
// click/page-view partition watermark policy.
        log.debug("Processing page view: {}", pageView.getEventId());

        int partitionNo = pageView.getPartition();
        Lock readLock = stateAccessLock.readLock();
        Lock partitionLock = getPartitionLock(partitionNo);
        readLock.lock();
        partitionLock.lock();

        try {
            boolean isTooLate = watermarkTracker.isTooLate(partitionNo, pageView.getEventTime());
            if (isTooLate){
                log.info("Page view {} is too late for partition {}. Watermark: {}", pageView.getEventId(), partitionNo, watermarkTracker.getWatermark(partitionNo));
                // todo:processedInputMapper.insertTerminalRecord();
                return;
            }

            pendingPageViewMapper.insertIfAbsent("page_views", pageView);
            watermarkStateMapper.upsertObserved(partitionNo, pageView.getEventTime(), Instant.now());

            PendingPageview pending = pendingPageView.computeIfAbsent(partitionNo, key -> {
                return new PendingPageview();
            });

            pending.add(pageView);

            watermarkTracker.updateWatermark(partitionNo, pageView.getEventTime());

            // handling pageview
            findAttributionForPendingViews(partitionNo);
        } finally {
            partitionLock.unlock();
            readLock.unlock();
        }

        log.debug("Page view processed: {}", pageView.getEventId());
    }

    /**
     * Scheduled task to evict old clicks from state.
     * Runs every 30 seconds to prevent unbounded memory growth.
     *
     * - Evict clicks older than the watermark cutoff
     * - Use clickStore.evictOldClicks() with appropriate cutoff time
     *
     *
     * clickStore.evictOldClicks() is a cross-partition deletion, watermark storage is partition-based,
     * I choose to use global_minimum_watermark to prevent asynchronous watermark updates between partitions
     * to prevent to delete of events that should have been retained.
     * A lazy flag could be considered to apply for each partition, thus global_minimum_watermark would only
     * select the minimum watermark of the ACTIVE partition as the basis for evict.
     *
     * For large data, time-based indexes or sth can be used to reduce the cost of iterating every 30 seconds.
     */
    @Scheduled(fixedRate = 30000)
    public void evictOldClicks() {
        // TODO: getGlobalMinimumWatermark() returns raw maximum event_time.
// Subtract allowed lateness and the 30-minute retention window
// before passing the cutoff to ClickStateStore.

        log.debug("Running state eviction");

        Lock writeLock = stateAccessLock.writeLock();
        writeLock.lock();

        try {
            Instant globalWatermark = watermarkTracker.getGlobalMinimumWatermark();

            if (globalWatermark.equals(Instant.MIN)) {
                log.debug("State eviction skipped since no watermark is initialized.");
                return;
            }

            Instant cutoff = globalWatermark.minus(watermarkTracker.getAllowedLateness()).minus(Duration.ofMinutes(30));

            int evictedCount = clickStore.evictOldClicks(cutoff);
            log.debug("State eviction done. Global watermark: {}, cutoff: {}, evicted clicks: {}", globalWatermark, cutoff, evictedCount);

        } finally {
            writeLock.unlock();
        }

    }




    /**
     * Calculate the pageviews before watermark in the current partition.
     * insert handled data into db
     * */
    private void findAttributionForPendingViews(int partitionNo) {
        PendingPageview pending = pendingPageView.get(partitionNo);

        // no pending pageviews
        if (pending == null || CollectionUtils.isEmpty(pending.getPageviews())) {
            return;
        }

        Instant watermark = watermarkTracker.getWatermark(partitionNo);

        // for list, handle
        Iterator<PageViewEvent> iterator = pending.getPageviews().iterator();
        while (iterator.hasNext()) {
            PageViewEvent pageView = iterator.next();

            // PendingPageview ordered by event_time, stop when after watermark
            if (pageView.getEventTime().isAfter(watermark)) {
                break;
            }

            AdClickEvent attributableClick = clickStore.findAttributableClick(pageView.getUserId(), pageView.getEventTime());

            AttributedPageView attributedPageView = AttributedPageView.builder()
                            .pageViewId(pageView.getEventId())
                            .userId(pageView.getUserId())
                            .eventTime(pageView.getEventTime())
                            .url(pageView.getUrl())
                            .attributedCampaignId(ObjectUtils.isEmpty(attributableClick) ? null : attributableClick.getCampaignId())
                            .attributedClickId(ObjectUtils.isEmpty(attributableClick) ? null : attributableClick.getClickId())
                            .build();

            // outputted -> remove from pending
            outputSink.write(attributedPageView);
            pendingPageViewMapper.markEmitted(pageView.getEventId());
            iterator.remove();

            log.debug("JoinEngine page_view {} found its click {}", pageView.getEventId(),
                    attributableClick == null ? null : attributableClick.getClickId());
        }

        // no page_view left for this partition, remove it
        if (pending.getPageviews().isEmpty()) {
            pendingPageView.remove(partitionNo, pending);
        }
    }
}
