package com.ebay.challenge.streamprocessor.state;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WatermarkTrackerTest {

    private static final Instant BASE_TIME = Instant.parse("2026-08-24T12:00:00Z");
    private static final int PARTITION = 1;

    /**
     * WatermarkTracker()
     *
     * 1.1 allowed lateness 0-15 min
     * 1.2 <0 allowed lateness throws e
     * 1.3 >15 allowed lateness throws e
     * */

    @Test
    void constructor_acceptsAllowedLatenessFromZeroToFifteenMinutes() {
        assertEquals(Duration.ZERO, new WatermarkTracker(0).getAllowedLateness());
        assertEquals(Duration.ofMinutes(15), new WatermarkTracker(15).getAllowedLateness());
    }

    @Test
    void constructor_negativeAllowedLatenessThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new WatermarkTracker(-1));
    }

    @Test
    void constructor_allowedLatenessAboveFifteenMinutesThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new WatermarkTracker(16));
    }

    /**
     * updateWatermark()
     *
     * 1.1 null event_time -> ignored
     * 2.1 for events: init -> update -> update -> ...
     * 2.2 earlier event_time dos nothing
     * 3.1 different partitions maintain independent watermarks
     * 3.2 concurrent updates -> take the maximum event_time
     * */

    @Test
    void updateWatermark_nullEventTimeIsIgnored() {
        WatermarkTracker tracker = new WatermarkTracker(0);

        tracker.updateWatermark(PARTITION, null);

        assertEquals(Instant.MIN, tracker.getWatermark(PARTITION));
    }

    @Test
    void updateWatermark_initializesAndAdvancesPartitionWatermark() {
        WatermarkTracker tracker = new WatermarkTracker(0);
        Instant first = BASE_TIME;
        Instant second = plusSeconds(BASE_TIME, 60);

        tracker.updateWatermark(PARTITION, first);
        assertEquals(first, tracker.getWatermark(PARTITION));

        tracker.updateWatermark(PARTITION, second);

        assertEquals(second, tracker.getWatermark(PARTITION));
    }

    @Test
    void updateWatermark_earlierEventTimeDoesNothing() {
        WatermarkTracker tracker = new WatermarkTracker(0);
        Instant latest = plusSeconds(BASE_TIME, 60);

        tracker.updateWatermark(PARTITION, latest);
        tracker.updateWatermark(PARTITION, BASE_TIME);
        tracker.updateWatermark(PARTITION, latest);

        assertEquals(latest, tracker.getWatermark(PARTITION));
    }

    @Test
    void updateWatermark_differentPartitionsAreIndependent() {
        WatermarkTracker tracker = new WatermarkTracker(0);
        Instant firstPartitionTime = BASE_TIME;
        Instant secondPartitionTime = plusSeconds(BASE_TIME, 60);

        tracker.updateWatermark(1, firstPartitionTime);
        tracker.updateWatermark(2, secondPartitionTime);

        assertEquals(firstPartitionTime, tracker.getWatermark(1));
        assertEquals(secondPartitionTime, tracker.getWatermark(2));
    }

    @Test
    void updateWatermark_concurrentUpdatesKeepMaximumEventTime() throws Exception {
        WatermarkTracker tracker = new WatermarkTracker(0);
        int eventCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>(eventCount);

        try {
            for (int index = 0; index < eventCount; index++) {
                int eventIndex = index;
                tasks.add(executor.submit(() -> {
                    start.await();
                    tracker.updateWatermark(PARTITION, plusSeconds(BASE_TIME, eventIndex));
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> task : tasks) {
                task.get();
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(plusSeconds(BASE_TIME, eventCount - 1), tracker.getWatermark(PARTITION));
    }

    /**
     * getWatermark()
     *
     * 1.1 uninit-ed -> Instant.MIN
     * 2.1 normal: returns [(max event_time) - allowed_lateness]
     * 2.2 allowed_lateness = 0
     * 3.1 multi partitions return independently
     * */

    @Test
    void getWatermark_uninitializedPartitionReturnsInstantMin() {
        WatermarkTracker tracker = new WatermarkTracker(2);

        assertEquals(Instant.MIN, tracker.getWatermark(PARTITION));
    }

    @Test
    void getWatermark_returnsMaximumEventTimeMinusAllowedLateness() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        Instant observed = plusSeconds(BASE_TIME, 600);

        tracker.updateWatermark(PARTITION, observed);

        assertEquals(minusSeconds(observed, 120), tracker.getWatermark(PARTITION));
    }

    @Test
    void getWatermark_zeroAllowedLatenessReturnsObservedEventTime() {
        WatermarkTracker tracker = new WatermarkTracker(0);

        tracker.updateWatermark(PARTITION, BASE_TIME);

        assertEquals(BASE_TIME, tracker.getWatermark(PARTITION));
    }

    @Test
    void getWatermark_multiplePartitionsReturnIndependently() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        Instant firstPartitionTime = plusSeconds(BASE_TIME, 600);
        Instant secondPartitionTime = plusSeconds(BASE_TIME, 1200);

        tracker.updateWatermark(1, firstPartitionTime);
        tracker.updateWatermark(2, secondPartitionTime);

        assertEquals(minusSeconds(firstPartitionTime, 120), tracker.getWatermark(1));
        assertEquals(minusSeconds(secondPartitionTime, 120), tracker.getWatermark(2));
    }

    /**
     * isTooLate()
     *
     * 1.1 null event time, uninitialized partition, or Instant.MIN watermark -> false
     * 2.1 event < (event_time - allowed_lateness) -> true
     * 2.2 event = (event_time - allowed_lateness) -> false
     * 2.3 event > (event_time - allowed_lateness) -> false
     * 3.1 shouldn't be influenced by other partition's watermark
     * */

    @Test
    void isTooLate_nullEventTimeOrUninitializedPartitionReturnsFalse() {
        WatermarkTracker tracker = new WatermarkTracker(2);

        assertFalse(tracker.isTooLate(PARTITION, null));
        assertFalse(tracker.isTooLate(PARTITION, BASE_TIME));

        tracker.updateWatermark(2, Instant.MIN);
        assertFalse(tracker.isTooLate(2, BASE_TIME));
    }

    @Test
    void isTooLate_eventBeforeEffectiveCutoffReturnsTrue() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        Instant observed = plusSeconds(BASE_TIME, 600);
        tracker.updateWatermark(PARTITION, observed);

        assertEquals(true, tracker.isTooLate(PARTITION, plusSeconds(BASE_TIME, 479)));
    }

    @Test
    void isTooLate_eventAtEffectiveCutoffReturnsFalse() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        Instant observed = plusSeconds(BASE_TIME, 600);
        tracker.updateWatermark(PARTITION, observed);

        assertFalse(tracker.isTooLate(PARTITION, plusSeconds(BASE_TIME, 480)));
    }

    @Test
    void isTooLate_eventAfterEffectiveCutoffReturnsFalse() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        Instant observed = plusSeconds(BASE_TIME, 600);
        tracker.updateWatermark(PARTITION, observed);

        assertFalse(tracker.isTooLate(PARTITION, plusSeconds(BASE_TIME, 481)));
    }

    @Test
    void isTooLate_otherPartitionWatermarkDoesNotAffectResult() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        tracker.updateWatermark(1, plusSeconds(BASE_TIME, 1200));
        tracker.updateWatermark(2, plusSeconds(BASE_TIME, 300));

        assertEquals(true, tracker.isTooLate(1, plusSeconds(BASE_TIME, 600)));
        assertFalse(tracker.isTooLate(2, plusSeconds(BASE_TIME, 600)));
    }

    /**
     * getAllowedLateness()
     *
     * 1.1 return
     * */

    @Test
    void getAllowedLateness_returnsConfiguredDuration() {
        WatermarkTracker tracker = new WatermarkTracker(2);

        assertEquals(Duration.ofMinutes(2), tracker.getAllowedLateness());
    }

    /**
     * getGlobalMinimumWatermark()
     *
     * 1.1 no partitions -> Instant.MIN
     * 2.1 one partition -> watermark for that partition
     * 2.2 multi partitions -> smallest among init-ed partitions
     * 2.3 updates -> watermark updates or not based on the min partition
     * */

    @Test
    void getGlobalMinimumWatermark_withoutPartitionsReturnsInstantMin() {
        WatermarkTracker tracker = new WatermarkTracker(2);

        assertEquals(Instant.MIN, tracker.getGlobalMinimumWatermark());
    }

    @Test
    void getGlobalMinimumWatermark_onePartitionReturnsItsWatermark() {
        WatermarkTracker tracker = new WatermarkTracker(2);

        tracker.updateWatermark(PARTITION, BASE_TIME);

        assertEquals(BASE_TIME, tracker.getGlobalMinimumWatermark());
    }

    @Test
    void getGlobalMinimumWatermark_multiplePartitionsReturnsSmallestWatermark() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        Instant earliest = BASE_TIME;
        Instant latest = plusSeconds(BASE_TIME, 600);

        tracker.updateWatermark(1, latest);
        tracker.updateWatermark(2, earliest);

        assertEquals(earliest, tracker.getGlobalMinimumWatermark());
    }

    @Test
    void getGlobalMinimumWatermark_updatesOnlyWhenTheMinimumPartitionChanges() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        Instant firstMinimum = BASE_TIME;
        Instant secondMinimum = plusSeconds(BASE_TIME, 600);
        Instant nonMinimumUpdate = plusSeconds(BASE_TIME, 1200);

        tracker.updateWatermark(1, firstMinimum);
        tracker.updateWatermark(2, secondMinimum);
        tracker.updateWatermark(2, nonMinimumUpdate);
        assertEquals(firstMinimum, tracker.getGlobalMinimumWatermark());

        tracker.updateWatermark(1, plusSeconds(BASE_TIME, 1800));

        assertEquals(nonMinimumUpdate, tracker.getGlobalMinimumWatermark());
    }

    /**
     * restoreWatermark()
     *
     * 1.1 null -> ignored
     * 2.1 restore a new partition watermark
     * 2.2 restore a later watermark advances the current value
     * 2.3 restore an earlier  watermark -> do nothing
     * 3.1 watermarks independent across partitions
     * */

    @Test
    void restoreWatermark_nullEventTimeIsIgnored() {
        WatermarkTracker tracker = new WatermarkTracker(0);

        tracker.restoreWatermark(PARTITION, null);

        assertEquals(Instant.MIN, tracker.getWatermark(PARTITION));
    }

    @Test
    void restoreWatermark_initializesNewPartition() {
        WatermarkTracker tracker = new WatermarkTracker(0);

        tracker.restoreWatermark(PARTITION, BASE_TIME);

        assertEquals(BASE_TIME, tracker.getWatermark(PARTITION));
    }

    @Test
    void restoreWatermark_laterEventTimeAdvancesCurrentValue() {
        WatermarkTracker tracker = new WatermarkTracker(0);
        Instant later = plusSeconds(BASE_TIME, 600);

        tracker.restoreWatermark(PARTITION, BASE_TIME);
        tracker.restoreWatermark(PARTITION, later);

        assertEquals(later, tracker.getWatermark(PARTITION));
    }

    @Test
    void restoreWatermark_earlierEventTimeDoesNothing() {
        WatermarkTracker tracker = new WatermarkTracker(0);
        Instant later = plusSeconds(BASE_TIME, 600);

        tracker.restoreWatermark(PARTITION, later);
        tracker.restoreWatermark(PARTITION, BASE_TIME);

        assertEquals(later, tracker.getWatermark(PARTITION));
    }

    @Test
    void restoreWatermark_differentPartitionsAreIndependent() {
        WatermarkTracker tracker = new WatermarkTracker(0);
        Instant firstPartitionTime = BASE_TIME;
        Instant secondPartitionTime = plusSeconds(BASE_TIME, 600);

        tracker.restoreWatermark(1, firstPartitionTime);
        tracker.restoreWatermark(2, secondPartitionTime);

        assertEquals(firstPartitionTime, tracker.getWatermark(1));
        assertEquals(secondPartitionTime, tracker.getWatermark(2));
    }

    private static Instant plusSeconds(Instant instant, long seconds) {
        return instant.plusSeconds(seconds);
    }

    private static Instant minusSeconds(Instant instant, long seconds) {
        return instant.minusSeconds(seconds);
    }
}
