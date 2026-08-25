package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClickHistoryMapperTest extends MapperTestSupport {

    private ClickHistoryMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new ClickHistoryMapper(jdbcTemplate);
    }

    @Test
    void migrateNonActiveOlderThan_whenBatchSizeIsInvalid_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> mapper.migrateNonActiveOlderThan(BASE_TIME, 0));
        assertThrows(IllegalArgumentException.class,
                () -> mapper.migrateNonActiveOlderThan(BASE_TIME, -1));
    }

    @Test
    void migrateNonActiveOlderThan_whenNoEligibleClick_returnsZero() {
        insertClickState(click("active", "user-1", minusMinutes(1), "campaign-1", 1, 1L), "ACTIVE");

        assertEquals(0, mapper.migrateNonActiveOlderThan(BASE_TIME, 10));
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_history", Integer.class));
    }

    @Test
    void migrateNonActiveOlderThan_migratesEvictedClicksBeforeCutoff() {
        insertClickState(click("old", "user-1", minusMinutes(1), "campaign-1", 1, 1L), "EVICTED");

        assertEquals(1, mapper.migrateNonActiveOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_history", Integer.class));
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_state", Integer.class));
    }

    @Test
    void migrateNonActiveOlderThan_excludesActiveClicks() {
        insertClickState(click("active", "user-1", minusMinutes(1), "campaign-1", 1, 1L), "ACTIVE");

        assertEquals(0, mapper.migrateNonActiveOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_state", Integer.class));
    }

    @Test
    void migrateNonActiveOlderThan_excludesClickAtExactCutoff() {
        insertClickState(click("exact", "user-1", BASE_TIME, "campaign-1", 1, 1L), "EVICTED");

        assertEquals(0, mapper.migrateNonActiveOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM click_state", Integer.class));
    }

    @Test
    void migrateNonActiveOlderThan_respectsBatchSizeAndOrdering() {
        insertClickState(click("later", "user-1", minusMinutes(3), "campaign-1", 1, 1L), "EVICTED");
        insertClickState(click("earlier", "user-1", minusMinutes(5), "campaign-2", 1, 2L), "EVICTED");
        insertClickState(click("same-time-b", "user-1", minusMinutes(1), "campaign-3", 1, 3L), "EVICTED");
        insertClickState(click("same-time-a", "user-1", minusMinutes(1), "campaign-4", 1, 4L), "EVICTED");

        assertEquals(2, mapper.migrateNonActiveOlderThan(BASE_TIME, 2));
        assertEquals(List.of("earlier", "later"), jdbcTemplate.query(
                "SELECT click_id FROM click_history ORDER BY event_time_epoch_ms, click_id",
                (rs, rowNum) -> rs.getString(1)));
    }

    @Test
    void migrateNonActiveOlderThan_copiesAllClickFieldsToHistory() {
        AdClickEvent click = click("click-1", "user-1", minusMinutes(1), "campaign-1", 7, 99L);
        insertClickState(click, "EVICTED");

        mapper.migrateNonActiveOlderThan(BASE_TIME, 10);

        AdClickEvent restored = jdbcTemplate.queryForObject("""
                SELECT user_id, event_time_epoch_ms, campaign_id, click_id,
                       partition_no, offset_no
                FROM click_history WHERE click_id = ?
                """, (rs, rowNum) -> click(
                rs.getString("click_id"), rs.getString("user_id"),
                Instant.ofEpochMilli(rs.getLong("event_time_epoch_ms")),
                rs.getString("campaign_id"), rs.getInt("partition_no"), rs.getLong("offset_no")),
                "click-1");

        assertEquals(click, restored);
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM click_history WHERE archived_at_epoch_ms IS NOT NULL", Integer.class));
    }

    @Test
    void migrateNonActiveOlderThan_deletesMigratedSourceRows() {
        insertClickState(click("click-1", "user-1", minusMinutes(1), "campaign-1", 1, 1L), "EVICTED");

        mapper.migrateNonActiveOlderThan(BASE_TIME, 10);

        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM click_state WHERE click_id = ?", Integer.class, "click-1"));
    }

    @Test
    void migrateNonActiveOlderThan_whenHistoryAlreadyContainsClick_remainsIdempotent() {
        AdClickEvent click = click("click-1", "user-1", minusMinutes(1), "campaign-1", 1, 1L);
        insertClickState(click, "EVICTED");
        insertClickHistory(click);

        assertEquals(1, mapper.migrateNonActiveOlderThan(BASE_TIME, 10));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM click_history WHERE click_id = ?", Integer.class, "click-1"));
        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM click_state WHERE click_id = ?", Integer.class, "click-1"));
    }

    @Test
    void migrateNonActiveOlderThan_whenHistoryCountMismatches_throwsException() {
        JdbcTemplate mockedJdbcTemplate = mock(JdbcTemplate.class);
        ClickHistoryMapper mockedMapper = new ClickHistoryMapper(mockedJdbcTemplate);

        when(mockedJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of("click-1"));
        when(mockedJdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(Object[].class)))
                .thenReturn(0);

        assertThrows(IllegalStateException.class,
                () -> mockedMapper.migrateNonActiveOlderThan(BASE_TIME, 10));
    }

    @Test
    void migrateNonActiveOlderThan_whenDeleteCountMismatches_throwsException() {
        JdbcTemplate mockedJdbcTemplate = mock(JdbcTemplate.class);
        ClickHistoryMapper mockedMapper = new ClickHistoryMapper(mockedJdbcTemplate);

        when(mockedJdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of("click-1"));
        when(mockedJdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(Object[].class)))
                .thenReturn(1);
        when(mockedJdbcTemplate.update(anyString(), any(Object[].class)))
                .thenReturn(1, 0);

        assertThrows(IllegalStateException.class,
                () -> mockedMapper.migrateNonActiveOlderThan(BASE_TIME, 10));
    }
}
