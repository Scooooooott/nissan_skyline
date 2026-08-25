package com.ebay.challenge.streamprocessor.mapper;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OutputMapperTest extends MapperTestSupport {

    private OutputMapper mapper;

    @Override
    void setUpDatabase() throws Exception {
        super.setUpDatabase();
        mapper = new OutputMapper(jdbcTemplate);
    }

    @Test
    void insertIfAbsent_whenOutputIsNew_insertsAndReturnsTrue() {
        AttributedPageView output = attributedPageView(
                "pv-1", "user-1", BASE_TIME, "https://example.com/1", "campaign-1", "click-1");

        assertEquals(true, mapper.insertIfAbsent(output));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM attributed_page_view", Integer.class));
    }

    @Test
    void insertIfAbsent_whenPageViewIdExists_returnsFalse() {
        AttributedPageView output = attributedPageView(
                "pv-1", "user-1", BASE_TIME, "https://example.com/1", "campaign-1", "click-1");
        insertOutput(output);

        assertEquals(false, mapper.insertIfAbsent(output));
    }

    @Test
    void insertIfAbsent_doesNotOverwriteExistingOutput() {
        AttributedPageView original = attributedPageView(
                "pv-1", "user-1", BASE_TIME, "https://example.com/1", "campaign-1", "click-1");
        AttributedPageView conflicting = attributedPageView(
                "pv-1", "user-1", BASE_TIME, "https://example.com/other", "campaign-2", "click-2");
        insertOutput(original);

        assertEquals(false, mapper.insertIfAbsent(conflicting));
        assertEquals("https://example.com/1", jdbcTemplate.queryForObject(
                "SELECT url FROM attributed_page_view WHERE page_view_id = ?", String.class, "pv-1"));
    }

    @Test
    void insertIfAbsent_preservesNullAttributionFields() {
        AttributedPageView output = attributedPageView(
                "pv-1", "user-1", BASE_TIME, "https://example.com/1", null, null);

        mapper.insertIfAbsent(output);

        assertNull(jdbcTemplate.queryForObject(
                "SELECT attributed_campaign_id FROM attributed_page_view", String.class));
        assertNull(jdbcTemplate.queryForObject(
                "SELECT attributed_click_id FROM attributed_page_view", String.class));
    }

    @Test
    void insertIfAbsent_preservesAttributedCampaignAndClick() {
        AttributedPageView output = attributedPageView(
                "pv-1", "user-1", BASE_TIME, "https://example.com/1", "campaign-1", "click-1");

        mapper.insertIfAbsent(output);

        assertEquals("campaign-1|click-1", jdbcTemplate.queryForObject("""
                SELECT attributed_campaign_id || '|' || attributed_click_id
                FROM attributed_page_view
                """, String.class));
    }

    @Test
    void findByPageViewId_whenRecordExists_mapsAllFields() {
        AttributedPageView output = attributedPageView(
                "pv-1", "user-1", BASE_TIME, "https://example.com/1", "campaign-1", "click-1");
        insertOutput(output);

        assertEquals(output, mapper.findByPageViewId("pv-1"));
    }

    @Test
    void findByPageViewId_whenRecordDoesNotExist_returnsNull() {
        assertNull(mapper.findByPageViewId("missing"));
    }
}
