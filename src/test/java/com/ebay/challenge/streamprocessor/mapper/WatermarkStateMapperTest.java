package com.ebay.challenge.streamprocessor.mapper;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarkStateMapperTest extends MapperTestSupport {

    private WatermarkStateMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new WatermarkStateMapper(jdbcTemplate);
    }

    @Test
    void upsertObserved_whenPartitionIsNew_insertsObservedState() {
        mapper.upsertObserved(2, BASE_TIME, BASE_TIME.plusSeconds(1));

        WatermarkStateMapper.WatermarkState state = mapper.findAll().getFirst();
        assertEquals(2, state.partition());
        assertEquals(BASE_TIME, state.maxEventTime());
        assertEquals("OBSERVED", state.status());
    }

    @Test
    void upsertObserved_whenNewEventTimeIsLater_advancesMaximumEventTime() {
        mapper.upsertObserved(1, BASE_TIME, BASE_TIME);
        mapper.upsertObserved(1, plusMinutes(5), plusMinutes(1));

        assertEquals(plusMinutes(5), mapper.findAll().getFirst().maxEventTime());
    }

    @Test
    void upsertObserved_whenNewEventTimeIsOlder_keepsExistingMaximum() {
        mapper.upsertObserved(1, plusMinutes(5), BASE_TIME);
        mapper.upsertObserved(1, BASE_TIME, plusMinutes(1));

        assertEquals(plusMinutes(5), mapper.findAll().getFirst().maxEventTime());
    }

    @Test
    void upsertObserved_whenEventTimeIsEqual_keepsExistingMaximum() {
        mapper.upsertObserved(1, BASE_TIME, BASE_TIME);
        mapper.upsertObserved(1, BASE_TIME, plusMinutes(1));

        assertEquals(BASE_TIME, mapper.findAll().getFirst().maxEventTime());
    }

    @Test
    void upsertObserved_alwaysUpdatesLastSeenAndUpdatedAt() {
        mapper.upsertObserved(1, plusMinutes(5), BASE_TIME);
        Instant observedAt = plusMinutes(2);
        mapper.upsertObserved(1, BASE_TIME, observedAt);

        WatermarkStateMapper.WatermarkState state = mapper.findAll().getFirst();
        assertEquals(observedAt, state.lastSeenAt());
        assertEquals(observedAt, state.updatedAt());
    }

    @Test
    void upsertObserved_setsObservedStatus() {
        insertWatermark(1, BASE_TIME, "IDLE", BASE_TIME, BASE_TIME);

        mapper.upsertObserved(1, BASE_TIME, plusMinutes(1));

        assertEquals("OBSERVED", mapper.findAll().getFirst().status());
    }

    @Test
    void findAll_returnsRowsOrderedByPartition() {
        insertWatermark(3, BASE_TIME, "OBSERVED", BASE_TIME, BASE_TIME);
        insertWatermark(1, BASE_TIME, "OBSERVED", BASE_TIME, BASE_TIME);
        insertWatermark(2, BASE_TIME, "OBSERVED", BASE_TIME, BASE_TIME);

        assertEquals(List.of(1, 2, 3), mapper.findAll().stream()
                .map(WatermarkStateMapper.WatermarkState::partition).toList());
    }

    @Test
    void findAll_mapsAllTimestampAndStatusFields() {
        Instant maxEventTime = plusMinutes(3);
        Instant lastSeenAt = plusMinutes(4);
        Instant updatedAt = plusMinutes(5);
        insertWatermark(1, maxEventTime, "RECOVERING", lastSeenAt, updatedAt);

        WatermarkStateMapper.WatermarkState state = mapper.findAll().getFirst();

        assertEquals(maxEventTime, state.maxEventTime());
        assertEquals("RECOVERING", state.status());
        assertEquals(lastSeenAt, state.lastSeenAt());
        assertEquals(updatedAt, state.updatedAt());
    }

    @Test
    void findAll_whenNoRows_returnsEmptyList() {
        assertTrue(mapper.findAll().isEmpty());
    }
}
