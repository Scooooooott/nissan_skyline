package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PendingPageViewMapperTest extends MapperTestSupport {

    private PendingPageViewMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new PendingPageViewMapper(jdbcTemplate);
    }

    @Test
    void insertIfAbsent_whenPageViewIsNew_insertsAndReturnsTrue() {
        PageViewEvent pageView = pageView("pv-1", "user-1", BASE_TIME, "https://example.com/1", 2, 10L);

        assertTrue(mapper.insertIfAbsent("page_views", pageView));
        assertEquals("PENDING", jdbcTemplate.queryForObject(
                "SELECT pending_status FROM pending_page_view", String.class));
    }

    @Test
    void insertIfAbsent_whenPageViewIdExists_returnsFalse() {
        PageViewEvent pageView = pageView("pv-1", "user-1", BASE_TIME, "https://example.com/1", 2, 10L);
        insertPendingPageView(pageView, "PENDING");

        assertEquals(false, mapper.insertIfAbsent("page_views", pageView));
    }

    @Test
    void insertIfAbsent_doesNotOverwriteExistingPageView() {
        PageViewEvent original = pageView("pv-1", "user-1", BASE_TIME, "https://example.com/1", 2, 10L);
        PageViewEvent duplicate = pageView("pv-1", "user-2", plusMinutes(1),
                "https://example.com/other", 3, 11L);
        insertPendingPageView(original, "PENDING");

        assertEquals(false, mapper.insertIfAbsent("page_views", duplicate));
        assertEquals("user-1", jdbcTemplate.queryForObject(
                "SELECT user_id FROM pending_page_view WHERE page_view_id = ?", String.class, "pv-1"));
    }

    @Test
    void findAllPending_returnsOnlyPendingRows() {
        insertPendingPageView(pageView("pending", "user-1", BASE_TIME, "url-1", 1, 1L), "PENDING");
        insertPendingPageView(pageView("emitted", "user-1", plusMinutes(1), "url-2", 1, 2L), "EMITTED");

        assertEquals(List.of("pending"), mapper.findAllPending().stream()
                .map(PageViewEvent::getEventId).toList());
    }

    @Test
    void findAllPending_ordersByEventTimeAndPageViewId() {
        insertPendingPageView(pageView("b", "user-1", BASE_TIME, "url-b", 1, 2L), "PENDING");
        insertPendingPageView(pageView("a", "user-1", BASE_TIME, "url-a", 1, 1L), "PENDING");
        insertPendingPageView(pageView("early", "user-1", minusMinutes(1), "url-e", 1, 3L), "PENDING");

        assertEquals(List.of("early", "a", "b"), mapper.findAllPending().stream()
                .map(PageViewEvent::getEventId).toList());
    }

    @Test
    void findAllPending_mapsPartitionAndOffsetMetadata() {
        insertPendingPageView(pageView("pv-1", "user-1", BASE_TIME, "url", 7, 99L), "PENDING");

        PageViewEvent restored = mapper.findAllPending().getFirst();

        assertEquals(7, restored.getPartition());
        assertEquals(99L, restored.getOffset());
    }

    @Test
    void findAllPending_whenNoRows_returnsEmptyList() {
        assertTrue(mapper.findAllPending().isEmpty());
    }

    @Test
    void markEmitted_whenPageViewIsPending_updatesStatusAndReturnsOne() {
        insertPendingPageView(pageView("pv-1", "user-1", BASE_TIME, "url", 1, 1L), "PENDING");

        assertEquals(1, mapper.markEmitted("pv-1"));
        assertEquals("EMITTED", jdbcTemplate.queryForObject(
                "SELECT pending_status FROM pending_page_view WHERE page_view_id = ?", String.class, "pv-1"));
    }

    @Test
    void markEmitted_whenPageViewIsAlreadyEmitted_returnsZero() {
        insertPendingPageView(pageView("pv-1", "user-1", BASE_TIME, "url", 1, 1L), "EMITTED");

        assertEquals(0, mapper.markEmitted("pv-1"));
    }

    @Test
    void markEmitted_whenPageViewDoesNotExist_returnsZero() {
        assertEquals(0, mapper.markEmitted("missing"));
    }
}
