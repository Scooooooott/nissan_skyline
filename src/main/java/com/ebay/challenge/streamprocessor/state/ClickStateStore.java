package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import io.micrometer.common.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stores ad click events partitioned by user_id for efficient windowed joins.
 * <p>
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 * <p>
 // TODO:logs and checks
 */
@Slf4j
@Component
public class ClickStateStore {

    // Attribution window: clicks within last 30 minutes can be attributed
    private static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    // TreeSet ordered by event_time then click_id
    // todo: make it a DTO to use hashmap for checking duplicated clicks in o(1)?
    private final ConcurrentHashMap<String, TreeSet<AdClickEvent>> clickHashMap = new ConcurrentHashMap<>();


    private final AtomicLong currentClickCount = new AtomicLong(0);

    /**
     * Add a click event to the state store.
     * <p>
     * Implemented thread-safe click storage
     * - Use locks for thread safety
     * - Store clicks sorted by event time (most recent first)
     * - Handle concurrent access properly
     *
     * @param click the ad click event
     */
    public void addClick(AdClickEvent click) {
        // empty check
        if (ObjectUtils.isEmpty(click) || StringUtils.isBlank(click.getUserId())
                || StringUtils.isBlank(click.getClickId()) || ObjectUtils.isEmpty(click.getEventTime())) {
            // todo: dead letter db
            log.warn("Invalid click info! {}", click);
            return;
        }

        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());

        String userId = click.getUserId();

        // concurrent execution, lock for this certain user
        clickHashMap.compute(userId, (key, clicks) -> {
            if (ObjectUtils.isEmpty(clicks)){
                // init TreeSet
                clicks = new TreeSet<>(Comparator.comparing(AdClickEvent::getEventTime)
                        .thenComparing(AdClickEvent::getClickId));
            }

            // check if exist duplicated click_id, can be optimized by hashmap
            AdClickEvent existingClick = null;
            for (AdClickEvent storedClick : clicks) {
                if (click.getClickId().equals(storedClick.getClickId())) {
                    existingClick = storedClick;
                }
            }

            // Deduplicate to keep idempotency
            if (!ObjectUtils.isEmpty(existingClick)) {
                if (existingClick.getEventTime().equals(click.getEventTime())
                    && existingClick.getCampaignId().equals(click.getCampaignId())) {
                    // if same click duplicate-added, ignore it
                    log.debug("Duplicate click {} for user {} ignored", click.getClickId(), userId);
                    // todo: dead letter db
                } else {
                    // if same click_id but different time, log it
                    log.warn("Conflicting click {} for user {} ignored. Existing: {}, Incoming: {}",
                            click.getClickId(), userId, existingClick, click);
                    // todo: dead letter db
                }

                // return without adding click
                return clicks;
            }

            // can be added, add the new one
            boolean inserted = clicks.add(click);
            if (inserted){
                currentClickCount.incrementAndGet();
            }

            return clicks;
        });

        log.debug("Adding click {} for user {} finished.", click.getClickId(), click.getUserId());
    }

    /**
     * Find the most recent click for a user within the attribution window.
     * <p>
     * Implemented attribution logic
     * - Search for clicks in window: [pageViewTime - 30 minutes, pageViewTime]
     * - Return the most recent click within the window
     * - Return null if no click found
     *
     * @param userId the user ID
     * @param pageViewTime the page view event time
     * @return the most recent click within 30 minutes before the page view, or null if none found
     */
    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        if (ObjectUtils.isEmpty(userId) || ObjectUtils.isEmpty(pageViewTime)) {
            return null;
        }

        log.debug("Finding attributable click for user {} at time {}", userId, pageViewTime);

        AtomicReference<AdClickEvent> resClick = new AtomicReference<>();
        Instant windowStart = pageViewTime.minus(ATTRIBUTION_WINDOW);

        clickHashMap.computeIfPresent(userId, (key, clicks) -> {
            // find adClickEvent backward
            Iterator<AdClickEvent> iterator = clicks.descendingIterator();

            while (iterator.hasNext()) {
                AdClickEvent click = iterator.next();
                Instant clickTime = click.getEventTime();

                // Search for clicks in window: [pageViewTime - 30 minutes, pageViewTime]
                // if time after pageViewTime, skip, find earlier
                if (clickTime.isAfter(pageViewTime)) {
                    continue;
                }
                // if time before (pageViewTime - 30 minutes), no more usable data, break
                if (clickTime.isBefore(windowStart)) {
                    break;
                }

                // find the first one (ordered)
                resClick.set(click);
                break;
            }

            return clicks;
        });

        log.debug("The attributable click for user {} at time {} is {}", userId, pageViewTime, resClick.get());
        return resClick.get();
    }

    /**
     * Evict old clicks that are beyond the retention window.
     * Prevents unbounded memory growth.
     * <p>
     * - Remove clicks older than the cutoff time
     * - Clean up empty user entries
     * - Return count of evicted clicks
     *
     * @param cutoffTime clicks older than this time should be evicted
     * @return number of clicks evicted
     */
    public int evictOldClicks(Instant cutoffTime) {
        log.debug("Evicting clicks older than {}", cutoffTime);
        if (ObjectUtils.isEmpty(clickHashMap)){
            log.debug("No clicks for evicting");
            return 0;
        }

        AtomicInteger evictedCount = new AtomicInteger(0);

        for (String userId : clickHashMap.keySet()){
            clickHashMap.computeIfPresent(userId, (key, clicks) -> {
                if (CollectionUtils.isEmpty(clicks)){
                    return null;
                }

                Iterator<AdClickEvent> iterator = clicks.iterator();

                while(iterator.hasNext()){
                    AdClickEvent click = iterator.next();

                    if (click.getEventTime().isBefore(cutoffTime)){
                        iterator.remove();
                        currentClickCount.decrementAndGet();
                        evictedCount.incrementAndGet();
                    } else{
                        break;
                    }
                }

                return clicks.isEmpty() ? null : clicks;
            });
        }

        log.debug("Evicted {} clicks older than {}", evictedCount.get(), cutoffTime);

        return evictedCount.get();
    }

    /**
     * Get the total number of clicks currently in state.
     *
     * @return total click count across all users
     */
    public long getTotalClickCount() {
        return currentClickCount.get();
    }


    public void restoreClick(AdClickEvent click) {
        if (ObjectUtils.isEmpty(click) || StringUtils.isBlank(click.getUserId())
                || StringUtils.isBlank(click.getClickId()) || ObjectUtils.isEmpty(click.getEventTime())) {
            throw new IllegalArgumentException("Cannot restore invalid click state: " + click);
        }

        String userId = click.getUserId();

        clickHashMap.compute(userId, (key, clicks) -> {
            if (clicks == null) {
                clicks = new TreeSet<>(Comparator.comparing(AdClickEvent::getEventTime)
                                                .thenComparing(AdClickEvent::getClickId));
            }

            if (clicks.add(click)) {
                currentClickCount.incrementAndGet();
            }

            return clicks;
        });
    }
}
