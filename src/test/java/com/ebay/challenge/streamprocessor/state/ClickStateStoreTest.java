package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClickStateStoreTest {

    private static final Instant BASE_TIME = Instant.parse("2026-08-24T12:00:00Z");

    private ClickStateStore store;

    @BeforeEach
    void setUp() {
        store = new ClickStateStore();
    }

    /**
     * addClick(AdClickEvent click)
     *
     * 1.1 empty check, return (click == null)
     * 1.2 empty check, return (click != null, userId == "")
     * 2.1 new click for new user, add a new Treeset
     * 2.2 new click for exist user, add to exist Treeset
     * 2.3 multiple clicks(effective), count ++
     * 3.1 same click_id, same content
     * 3.2 same click_id, different content
     * 3.3 conflict clicks, count stays the same
     *
     * */

    @Test
    void addClick_nullClick_ignoresInput() {
        store.addClick(null);

        assertEquals(0, store.getTotalClickCount());
    }

    @Test
    void addClick_validClickForNewUser_storesClickAndIncrementsCount() {
        AdClickEvent click = click("click-1", "user-1", BASE_TIME, "campaign-1");

        store.addClick(click);

        assertEquals(1, store.getTotalClickCount());
        assertSame(click, store.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void addClick_validClickForExistingUser_storesAnotherClickAndIncrementsCount() {
        AdClickEvent first = click("click-1", "user-1", minusMinutes(BASE_TIME, 1), "campaign-1");
        AdClickEvent second = click("click-2", "user-1", BASE_TIME, "campaign-2");

        store.addClick(first);
        store.addClick(second);

        assertEquals(2, store.getTotalClickCount());
        assertSame(second, store.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void addClick_sameClickIdAndSameContent_isIgnoredIdempotently() {
        AdClickEvent original = click("click-1", "user-1", BASE_TIME, "campaign-1");
        AdClickEvent duplicate = click("click-1", "user-1", BASE_TIME, "campaign-1");

        store.addClick(original);
        store.addClick(duplicate);

        assertEquals(1, store.getTotalClickCount());
        assertSame(original, store.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void addClick_sameClickIdWithDifferentContent_isIgnoredAsConflict() {
        AdClickEvent original = click("click-1", "user-1", BASE_TIME, "campaign-1");
        AdClickEvent conflict = click("click-1", "user-1", BASE_TIME, "campaign-2");

        store.addClick(original);
        store.addClick(conflict);

        assertEquals(1, store.getTotalClickCount());
        assertSame(original, store.findAttributableClick("user-1", BASE_TIME));
    }

    /**
     * findAttributableClick()
     *
     * 1.1 empty input, userId or pageViewTime == null
     * 1.2 no click list for input user, return null
     * 2.1 click > pageview, skip this one
     * 2.2 click < windowStart, click too late, end attribution
     * 2.3 click == pageView, attributed
     * 2.4 windowStart <= click < pageView, attributed
     * 2.5 windowStart <= multiple clicks < pageView, attributed the latest one
     * */

    @Test
    void findAttributableClick_invalidInput_returnsNull() {
        assertNull(store.findAttributableClick(null, BASE_TIME));
    }

    @Test
    void findAttributableClick_unknownUser_returnsNull() {
        store.addClick(click("click-1", "user-1", BASE_TIME, "campaign-1"));

        assertNull(store.findAttributableClick("unknown-user", BASE_TIME));
    }

    @Test
    void findAttributableClick_skipsFutureClickAndReturnsMostRecentEligibleClick() {
        AdClickEvent future = click("future", "user-1", plusMinutes(BASE_TIME, 1), "campaign-future");
        AdClickEvent olderEligible = click("older", "user-1", minusMinutes(BASE_TIME, 10), "campaign-older");
        AdClickEvent latestEligible = click("latest", "user-1", minusMinutes(BASE_TIME, 1), "campaign-latest");

        store.addClick(future);
        store.addClick(olderEligible);
        store.addClick(latestEligible);

        assertSame(latestEligible, store.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void findAttributableClick_clickOlderThanThirtyMinutes_returnsNull() {
        store.addClick(click("old", "user-1", minusMinutes(BASE_TIME, 31), "campaign-old"));

        assertNull(store.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void findAttributableClick_clickAtPageViewTime_isAttributable() {
        AdClickEvent click = click("exact-page-time", "user-1", BASE_TIME, "campaign-1");
        store.addClick(click);

        assertSame(click, store.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void findAttributableClick_clickAtWindowStart_isAttributable() {
        AdClickEvent click = click("exact-window-start", "user-1",
                minusMinutes(BASE_TIME, 30), "campaign-1");
        store.addClick(click);

        assertSame(click, store.findAttributableClick("user-1", BASE_TIME));
    }

    /**
     * evictOldClicks()
     *
     * 1.1 empty input, clickHashMap is empty, return 0
     * 2.1 all clicks > cutoff, return 0
     * 2.2 for 1 user, several clicks < cutoff, evict and return counts
     * 2.3 for multiple users, several (including 0, 1 and more) clicks < cutoff, evict and return counts
     * 2.4 click_time == cutoff, return 0
     * 2.5 for certain user, when all clicks evicted, clear k-v in map
     * 3.1 for duplicate executions, should return (evicted, 0, 0, 0, 0, ...)
     * */

    @Test
    void evictOldClicks_emptyStore_returnsZero() {
        assertEquals(0, store.evictOldClicks(BASE_TIME));
        assertEquals(0, store.getTotalClickCount());
    }

    @Test
    void evictOldClicks_clicksAtOrAfterCutoffAreRetained() {
        AdClickEvent exactCutoff = click("exact", "user-1", BASE_TIME, "campaign-exact");
        AdClickEvent newer = click("newer", "user-1", BASE_TIME.plusSeconds(1), "campaign-newer");
        store.addClick(exactCutoff);
        store.addClick(newer);

        assertEquals(0, store.evictOldClicks(BASE_TIME));
        assertEquals(2, store.getTotalClickCount());
        assertSame(exactCutoff, store.findAttributableClick("user-1", BASE_TIME));
        assertSame(newer, store.findAttributableClick("user-1", BASE_TIME.plusSeconds(1)));
    }

    @Test
    void evictOldClicks_oneUser_removesOnlyClicksBeforeCutoff() {
        AdClickEvent old = click("old", "user-1", minusMinutes(BASE_TIME, 1), "campaign-old");
        AdClickEvent retained = click("retained", "user-1", plusMinutes(BASE_TIME, 1), "campaign-retained");
        store.addClick(old);
        store.addClick(retained);

        assertEquals(1, store.evictOldClicks(BASE_TIME));
        assertEquals(1, store.getTotalClickCount());
        assertSame(retained,
                store.findAttributableClick("user-1", plusMinutes(BASE_TIME, 1)));
    }

    @Test
    void evictOldClicks_multipleUsers_returnsTotalEvictedCount() {
        store.addClick(click("user-1-old-1", "user-1", minusMinutes(BASE_TIME, 3), "campaign-1"));
        store.addClick(click("user-1-old-2", "user-1", minusMinutes(BASE_TIME, 2), "campaign-2"));
        store.addClick(click("user-2-old", "user-2", minusMinutes(BASE_TIME, 1), "campaign-3"));
        AdClickEvent user2Retained = click("user-2-retained", "user-2",
                plusMinutes(BASE_TIME, 1), "campaign-4");
        AdClickEvent user3Retained = click("user-3-retained", "user-3",
                plusMinutes(BASE_TIME, 2), "campaign-5");
        store.addClick(user2Retained);
        store.addClick(user3Retained);

        assertEquals(3, store.evictOldClicks(BASE_TIME));
        assertEquals(2, store.getTotalClickCount());
        assertNull(store.findAttributableClick("user-1", BASE_TIME));
        assertSame(user2Retained,
                store.findAttributableClick("user-2", plusMinutes(BASE_TIME, 1)));
        assertSame(user3Retained,
                store.findAttributableClick("user-3", plusMinutes(BASE_TIME, 2)));
    }

    @Test
    void evictOldClicks_isIdempotent() {
        store.addClick(click("old-1", "user-1", minusMinutes(BASE_TIME, 2), "campaign-1"));
        store.addClick(click("old-2", "user-1", minusMinutes(BASE_TIME, 1), "campaign-2"));

        assertEquals(2, store.evictOldClicks(BASE_TIME));
        assertEquals(0, store.getTotalClickCount());
        assertEquals(0, store.evictOldClicks(BASE_TIME));
        assertEquals(0, store.getTotalClickCount());
    }

    /**
     * getTotalClickCount()
     * 2.1 get()
     * */

    /**
     * restoreClick()
     * 1.1 empty input, throw
     * 2.1 empty clicks, create empty Treeset
     * 2.2 clicks.add(click) == false
     * 2.3 clicks.add(click) == true
     * */

    @Test
    void restoreClick_invalidClick_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> store.restoreClick(null));
        assertEquals(0, store.getTotalClickCount());
    }

    @Test
    void restoreClick_distinctClicksIncreaseCountAndComparatorDuplicateDoesNot() {
        AdClickEvent first = click("restore-1", "user-1", minusMinutes(BASE_TIME, 2), "campaign-1");
        AdClickEvent second = click("restore-2", "user-1", minusMinutes(BASE_TIME, 1), "campaign-2");
        AdClickEvent comparatorDuplicate = click("restore-1", "user-1",
                minusMinutes(BASE_TIME, 2), "campaign-1");

        store.restoreClick(first);
        store.restoreClick(second);
        store.restoreClick(comparatorDuplicate);

        assertEquals(2, store.getTotalClickCount());
        assertSame(second, store.findAttributableClick("user-1", BASE_TIME));
    }

    @Test
    void addClick_concurrentDistinctClicksForSameUser_preservesAllClicks() throws Exception {
        int clickCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>(clickCount);

        try {
            for (int index = 0; index < clickCount; index++) {
                int clickIndex = index;
                tasks.add(executor.submit(() -> {
                    start.await();
                    store.addClick(click("concurrent-" + clickIndex, "user-1",
                            BASE_TIME.plusSeconds(clickIndex), "campaign-" + clickIndex));
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

        assertEquals(clickCount, store.getTotalClickCount());
        assertEquals("concurrent-" + (clickCount - 1),
                store.findAttributableClick("user-1", BASE_TIME.plusSeconds(clickCount - 1))
                        .getClickId());
    }

    private static AdClickEvent click(String clickId, String userId, Instant eventTime,
                                      String campaignId) {
        return AdClickEvent.builder()
                .clickId(clickId)
                .userId(userId)
                .eventTime(eventTime)
                .campaignId(campaignId)
                .build();
    }

    private static Instant plusMinutes(Instant instant, long minutes) {
        return instant.plusSeconds(minutes * 60);
    }

    private static Instant minusMinutes(Instant instant, long minutes) {
        return instant.minusSeconds(minutes * 60);
    }
}
