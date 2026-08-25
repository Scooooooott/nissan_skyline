package com.ebay.challenge.streamprocessor.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PendingPageviewTest {

    @Test
    void startsWithAnEmptyPageViewSet() {
        PendingPageview pending = new PendingPageview();

        assertTrue(pending.getPageviews().isEmpty());
    }

    @Test
    void addFirstPageView_returnsTrueAndStoresThePageView() {
        PendingPageview pending = new PendingPageview();
        PageViewEvent pageView = pageView("pv-1", Instant.parse("2026-08-24T10:00:00Z"));

        assertTrue(pending.add(pageView));
        assertEquals(1, pending.getPageviews().size());
        assertTrue(pending.getPageviews().contains(pageView));
    }

    @Test
    void addDuplicateByEventTimeAndId_returnsFalse() {
        PendingPageview pending = new PendingPageview();
        PageViewEvent first = pageView("pv-1", Instant.parse("2026-08-24T10:00:00Z"));
        PageViewEvent duplicate = pageView("pv-1", Instant.parse("2026-08-24T10:00:00Z"));

        assertTrue(pending.add(first));
        assertFalse(pending.add(duplicate));
        assertEquals(1, pending.getPageviews().size());
    }

    @Test
    void keepsPageViewsWithSameEventTimeAndDifferentIds() {
        PendingPageview pending = new PendingPageview();
        Instant eventTime = Instant.parse("2026-08-24T10:00:00Z");

        assertTrue(pending.add(pageView("pv-1", eventTime)));
        assertTrue(pending.add(pageView("pv-2", eventTime)));

        assertEquals(2, pending.getPageviews().size());
    }

    @Test
    void ordersPageViewsByEventTimeThenEventId() {
        PendingPageview pending = new PendingPageview();
        Instant base = Instant.parse("2026-08-24T10:00:00Z");

        pending.add(pageView("pv-b", base));
        pending.add(pageView("pv-a", base));
        pending.add(pageView("pv-early", base.minusSeconds(1)));
        pending.add(pageView("pv-late", base.plusSeconds(1)));

        assertEquals(List.of("pv-early", "pv-a", "pv-b", "pv-late"),
                pending.getPageviews().stream().map(PageViewEvent::getEventId).toList());
    }

    @Test
    void treatsSameIdWithDifferentEventTimeAsDistinctEntries() {
        PendingPageview pending = new PendingPageview();

        assertTrue(pending.add(pageView("pv-1", Instant.parse("2026-08-24T10:00:00Z"))));
        assertTrue(pending.add(pageView("pv-1", Instant.parse("2026-08-24T10:01:00Z"))));

        assertEquals(2, pending.getPageviews().size());
    }

    @Test
    void supportsConcurrentAddsWithoutLosingPageViews() {
        PendingPageview pending = new PendingPageview();
        int pageViewCount = 100;

        IntStream.range(0, pageViewCount).parallel().forEach(index ->
                pending.add(pageView("pv-" + index, Instant.parse("2026-08-24T10:00:00Z")
                        .plusSeconds(index))));

        Set<String> ids = ConcurrentHashMap.newKeySet();
        pending.getPageviews().forEach(pageView -> ids.add(pageView.getEventId()));

        assertEquals(pageViewCount, pending.getPageviews().size());
        assertEquals(pageViewCount, ids.size());
    }

    private static PageViewEvent pageView(String eventId, Instant eventTime) {
        return PageViewEvent.builder()
                .eventId(eventId)
                .userId("user-1")
                .eventTime(eventTime)
                .url("https://example.com/" + eventId)
                .build();
    }
}
