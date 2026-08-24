package com.ebay.challenge.streamprocessor.state;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks watermarks per partition to handle out-of-order events.
 * <p>
 * Watermark represents the point in event-time up to which we believe we have seen all events.
 * Events arriving with event_time < watermark - allowedLateness are considered too late.
 * <p>
 * The watermark map stores the latest observed event_time
 * use [watermark - allowedLateness] to calculate effective cutoff
 * Watermarks are event-driven and independent for each partition.
 * Watermark is driven from the maximum observed event_time.Therefore, an idle partition has no
 * automatic watermark progression, and pending page views on that partition remain pending
 * until a later event advances the watermark. Idle-partition detection could be considered as TODO.
 * <p>
 * */
@Slf4j
@Component
public class WatermarkTracker {

    private final Duration allowedLateness;

    // store latest observed event_time for each partition
    private final ConcurrentHashMap<Integer, Instant> watermarkMap = new ConcurrentHashMap<>();

    public WatermarkTracker(@Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes){
        if ((allowedLatenessMinutes < 0) || (allowedLatenessMinutes > 15)){
            throw new IllegalArgumentException("WatermarkTracker Error! " +
                    "allowedLateness can not be negative or bigger than 15(mins)! Now is: " + allowedLatenessMinutes);
        }
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        log.info("Initialized WatermarkTracker with allowed lateness: {} minutes", allowedLatenessMinutes);
    }

    /**
     * Update watermark for a partition based on observed event time.
     * Watermark advances monotonically (never goes backward).
     * <p>
     * - Update partition watermark if event time is later than current watermark
     * - Ensure watermark never goes backward
     * - Handle concurrent updates
     * <p>
     * watermark = latest event_time
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     */
    public void updateWatermark(int partition, Instant eventTime) {
        log.debug("Updating watermark for partition {} with event time {}", partition, eventTime);
        if (ObjectUtils.isEmpty(eventTime)){
            return;
        }

        Instant updatedWm = watermarkMap.compute(partition, (key, currentWatermark)->{
            if (ObjectUtils.isEmpty(currentWatermark) || eventTime.isAfter(currentWatermark)){
                return eventTime;
            }
            return currentWatermark;
        });
        log.debug("Watermark for partition {} has been updated to {}", partition, updatedWm);
    }

    /**
     * Get current watermark for a partition.
     * <p>
     * - Return current watermark for the partition
     * - Return Instant.MIN if partition has no watermark yet
     * <p>
     * - the latest observed event_time for the partition
     *
     * @param partition the partition ID
     * @return the current watermark, or Instant.MIN if not yet initialized
     */
    public Instant getWatermark(int partition) {
        Instant watermark = watermarkMap.get(partition);
        return ObjectUtils.isEmpty(watermark) ? Instant.MIN : watermark.minus(allowedLateness);
    }

    /**
     * Check if an event is too late (beyond allowed lateness).
     * <p>
     * - Calculate cutoff time as: [watermark - allowedLateness]
     * - Return true if event is before cutoff time
     * - Handle case when watermark is not yet initialized
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     * @return true if the event is too late and should be dropped
     */
    public boolean isTooLate(int partition, Instant eventTime) {
        Instant watermark = watermarkMap.get(partition);

        if (ObjectUtils.isEmpty(eventTime) || ObjectUtils.isEmpty(watermark) || (Instant.MIN.equals(watermark))){
            return false;
        }

        Instant cutoffTime = watermark.minus(allowedLateness);

        return eventTime.isBefore(cutoffTime);
    }

    /**
     * Get the allowed lateness duration.
     *
     * @return the allowed lateness duration
     */
    public Duration getAllowedLateness() {
        return allowedLateness;
    }

    /**
     * Get the global minimum value of watermark
     *
     * @return the smallest watermark
     * */
    public Instant getGlobalMinimumWatermark(){
        if (ObjectUtils.isEmpty(watermarkMap)){
            return Instant.MIN;
        }

        Instant minEventTime = Instant.MAX;

        for (Instant eventTime : watermarkMap.values()){
            if (eventTime.isBefore(minEventTime)){
                minEventTime = eventTime;
            }
        }

        return minEventTime;
    }

    public void restoreWatermark(int partition, Instant maxEventTime) {
        if (maxEventTime == null) {
            return;
        }

        watermarkMap.merge(partition, maxEventTime, (current, restored) ->
                        restored.isAfter(current) ? restored : current);
    }


}
