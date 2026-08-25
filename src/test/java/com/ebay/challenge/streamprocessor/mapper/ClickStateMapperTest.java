package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClickStateMapperTest extends MapperTestSupport {

    private static final String TOPIC = "ad_clicks";
    private ClickStateMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new ClickStateMapper(jdbcTemplate);
    }

    @Test
    void insertIfAbsent_whenClickIsNew_insertsAndReturnsInserted() {
        AdClickEvent click = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);

        assertEquals(ClickStateMapper.INSERTED, mapper.insertIfAbsent(TOPIC, click));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_state", Integer.class));
        assertEquals("campaign-1", jdbcTemplate.queryForObject(
                "SELECT campaign_id FROM click_state WHERE click_id = ?", String.class, "click-1"));
        assertEquals(BASE_TIME.toEpochMilli(), jdbcTemplate.queryForObject(
                "SELECT event_time_epoch_ms FROM click_state WHERE click_id = ?", Long.class, "click-1"));
    }

    @Test
    void insertIfAbsent_whenSameOffsetAndSameBusinessContent_returnsReplay() {
        AdClickEvent click = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);
        insertClickState(click, "ACTIVE");

        assertEquals(ClickStateMapper.REPLAY, mapper.insertIfAbsent(TOPIC, click));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_state", Integer.class));
    }

    @Test
    void insertIfAbsent_whenSameBusinessContentFromDifferentKafkaSource_returnsDuplicate() {
        AdClickEvent stored = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);
        AdClickEvent incoming = click("click-1", "user-1", BASE_TIME, "campaign-1", 3, 11L);
        insertClickState(stored, "ACTIVE");

        assertEquals(ClickStateMapper.DUPLICATE, mapper.insertIfAbsent(TOPIC, incoming));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_state", Integer.class));
    }

    @Test
    void insertIfAbsent_whenSameClickIdHasDifferentBusinessContent_returnsConflict() {
        AdClickEvent stored = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);
        AdClickEvent incoming = click("click-1", "user-2", BASE_TIME.plusSeconds(1), "campaign-2", 2, 11L);
        insertClickState(stored, "ACTIVE");

        assertEquals(ClickStateMapper.CONFLICT, mapper.insertIfAbsent(TOPIC, incoming));
        assertEquals("user-1", jdbcTemplate.queryForObject(
                "SELECT user_id FROM click_state WHERE click_id = ?", String.class, "click-1"));
    }

    @Test
    void insertIfAbsent_whenConflictRowDisappearsAfterInsert_throwsException() {
        JdbcTemplate mockedJdbcTemplate = mock(JdbcTemplate.class);
        ClickStateMapper mockedMapper = new ClickStateMapper(mockedJdbcTemplate);
        AdClickEvent click = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);

        when(mockedJdbcTemplate.query(anyString(), any(RowMapper.class), eq("click-1")))
                .thenReturn(List.of());
        when(mockedJdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(0);

        assertThrows(IllegalStateException.class,
                () -> mockedMapper.insertIfAbsent(TOPIC, click));
    }

    @Test
    void classifyExisting_whenClickDoesNotExist_returnsEmpty() {
        AdClickEvent click = click("missing", "user-1", BASE_TIME, "campaign-1", 1, 1L);

        assertTrue(mapper.classifyExisting(click).isEmpty());
    }

    @Test
    void classifyExisting_returnsReplayForSameSourceAndContent() {
        AdClickEvent click = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);
        insertClickState(click, "ACTIVE");

        assertEquals(Optional.of(ClickStateMapper.REPLAY), mapper.classifyExisting(click));
    }

    @Test
    void classifyExisting_returnsDuplicateForSameContent() {
        AdClickEvent stored = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);
        AdClickEvent incoming = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 11L);
        insertClickState(stored, "ACTIVE");

        assertEquals(Optional.of(ClickStateMapper.DUPLICATE), mapper.classifyExisting(incoming));
    }

    @Test
    void classifyExisting_returnsConflictForDifferentContent() {
        AdClickEvent stored = click("click-1", "user-1", BASE_TIME, "campaign-1", 2, 10L);
        AdClickEvent incoming = click("click-1", "user-1", BASE_TIME.plusSeconds(1), "campaign-1", 2, 10L);
        insertClickState(stored, "ACTIVE");

        assertEquals(Optional.of(ClickStateMapper.CONFLICT), mapper.classifyExisting(incoming));
    }

    @Test
    void findByClickIdIncludingInactive_readsActiveClickFromClickState() {
        AdClickEvent click = click("click-1", "user-1", BASE_TIME, "campaign-1", 4, 20L);
        insertClickState(click, "ACTIVE");

        ClickStateMapper.StoredClick stored = mapper.findByClickIdIncludingInactive("click-1").orElseThrow();
        assertEquals("ACTIVE", stored.stateStatus());
        assertEquals(click, stored.click());
    }

    @Test
    void findByClickIdIncludingInactive_readsArchivedClickFromClickHistory() {
        AdClickEvent click = click("click-1", "user-1", BASE_TIME, "campaign-1", 4, 20L);
        insertClickHistory(click);

        ClickStateMapper.StoredClick stored = mapper.findByClickIdIncludingInactive("click-1").orElseThrow();
        assertEquals("ARCHIVED", stored.stateStatus());
        assertEquals(click, stored.click());
    }

    @Test
    void findByClickIdIncludingInactive_whenMissing_returnsEmpty() {
        assertTrue(mapper.findByClickIdIncludingInactive("missing").isEmpty());
    }

    @Test
    void findAllActive_excludesEvictedClicks() {
        insertClickState(click("active", "user-1", BASE_TIME, "campaign-1", 1, 1L), "ACTIVE");
        insertClickState(click("evicted", "user-1", plusMinutes(1), "campaign-2", 1, 2L), "EVICTED");

        List<AdClickEvent> active = mapper.findAllActive();

        assertEquals(List.of("active"), active.stream().map(AdClickEvent::getClickId).toList());
    }

    @Test
    void findAllActive_ordersByUserEventTimeAndClickId() {
        insertClickState(click("b", "user-2", BASE_TIME, "campaign-b", 1, 2L), "ACTIVE");
        insertClickState(click("a", "user-1", plusMinutes(1), "campaign-a", 1, 1L), "ACTIVE");
        insertClickState(click("a-early", "user-1", BASE_TIME, "campaign-a", 1, 3L), "ACTIVE");

        assertEquals(List.of("a-early", "a", "b"), mapper.findAllActive().stream()
                .map(AdClickEvent::getClickId).toList());
    }

    @Test
    void findAllActive_restoresPartitionAndOffsetMetadata() {
        insertClickState(click("click-1", "user-1", BASE_TIME, "campaign-1", 7, 99L), "ACTIVE");

        AdClickEvent restored = mapper.findAllActive().getFirst();

        assertEquals(7, restored.getPartition());
        assertEquals(99L, restored.getOffset());
    }

    @Test
    void markOlderThanEvicted_marksOnlyActiveClicksBeforeCutoff() {
        insertClickState(click("old", "user-1", minusMinutes(2), "campaign-1", 1, 1L), "ACTIVE");
        insertClickState(click("new", "user-1", plusMinutes(2), "campaign-2", 1, 2L), "ACTIVE");

        assertEquals(1, mapper.markOlderThanEvicted(BASE_TIME));
        assertEquals("EVICTED", jdbcTemplate.queryForObject(
                "SELECT state_status FROM click_state WHERE click_id = ?", String.class, "old"));
        assertEquals("ACTIVE", jdbcTemplate.queryForObject(
                "SELECT state_status FROM click_state WHERE click_id = ?", String.class, "new"));
    }

    @Test
    void markOlderThanEvicted_doesNotMarkClickAtExactCutoff() {
        insertClickState(click("exact", "user-1", BASE_TIME, "campaign-1", 1, 1L), "ACTIVE");

        assertEquals(0, mapper.markOlderThanEvicted(BASE_TIME));
        assertEquals("ACTIVE", jdbcTemplate.queryForObject(
                "SELECT state_status FROM click_state WHERE click_id = ?", String.class, "exact"));
    }

    @Test
    void markOlderThanEvicted_doesNotChangeAlreadyEvictedClicks() {
        insertClickState(click("evicted", "user-1", minusMinutes(2), "campaign-1", 1, 1L), "EVICTED");

        assertEquals(0, mapper.markOlderThanEvicted(BASE_TIME));
        assertEquals("EVICTED", jdbcTemplate.queryForObject(
                "SELECT state_status FROM click_state WHERE click_id = ?", String.class, "evicted"));
    }
}
