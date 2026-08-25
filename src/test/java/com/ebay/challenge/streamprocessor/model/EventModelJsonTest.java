package com.ebay.challenge.streamprocessor.model;

import com.ebay.challenge.streamprocessor.config.KafkaConsumerConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventModelJsonTest {

    private final ObjectMapper objectMapper = new KafkaConsumerConfig().objectMapper();

    @Test
    void deserializesAdClickSnakeCaseFields() throws Exception {
        String json = """
                {
                  "user_id": "user-1",
                  "event_time": "2026-08-24T10:00:00",
                  "campaign_id": "campaign-1",
                  "click_id": "click-1"
                }
                """;

        AdClickEvent click = objectMapper.readValue(json, AdClickEvent.class);

        assertEquals("user-1", click.getUserId());
        assertEquals(Instant.parse("2026-08-24T10:00:00Z"), click.getEventTime());
        assertEquals("campaign-1", click.getCampaignId());
        assertEquals("click-1", click.getClickId());
    }

    @Test
    void serializesAdClickBusinessFields() throws Exception {
        AdClickEvent click = AdClickEvent.builder()
                .userId("user-1")
                .eventTime(Instant.parse("2026-08-24T10:00:00Z"))
                .campaignId("campaign-1")
                .clickId("click-1")
                .build();

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(click));

        assertEquals("user-1", json.get("user_id").asText());
        assertEquals("campaign-1", json.get("campaign_id").asText());
        assertEquals("click-1", json.get("click_id").asText());
        assertEquals("2026-08-24T10:00:00Z", json.get("event_time").asText());
    }

    @Test
    void deserializesPageViewSnakeCaseFields() throws Exception {
        String json = """
                {
                  "user_id": "user-1",
                  "event_time": "2026-08-24T10:00:00",
                  "url": "https://example.com/product",
                  "event_id": "pv-1"
                }
                """;

        PageViewEvent pageView = objectMapper.readValue(json, PageViewEvent.class);

        assertEquals("user-1", pageView.getUserId());
        assertEquals(Instant.parse("2026-08-24T10:00:00Z"), pageView.getEventTime());
        assertEquals("https://example.com/product", pageView.getUrl());
        assertEquals("pv-1", pageView.getEventId());
    }

    @Test
    void serializesPageViewBusinessFields() throws Exception {
        PageViewEvent pageView = PageViewEvent.builder()
                .userId("user-1")
                .eventTime(Instant.parse("2026-08-24T10:00:00Z"))
                .url("https://example.com/product")
                .eventId("pv-1")
                .build();

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(pageView));

        assertEquals("pv-1", json.get("event_id").asText());
        assertEquals("https://example.com/product", json.get("url").asText());
        assertEquals("2026-08-24T10:00:00Z", json.get("event_time").asText());
    }

    @Test
    void serializesAttributedPageViewWithSnakeCaseFields() throws Exception {
        AttributedPageView output = attributedPageView("pv-1", "user-1", "campaign-1", "click-1");

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(output));

        assertTrue(json.has("page_view_id"));
        assertTrue(json.has("user_id"));
        assertTrue(json.has("event_time"));
        assertTrue(json.has("attributed_campaign_id"));
        assertTrue(json.has("attributed_click_id"));
        assertFalse(json.has("pageViewId"));
        assertFalse(json.has("attributedCampaignId"));
    }

    @Test
    void serializesAttributedPageViewWithNullAttributionFields() throws Exception {
        AttributedPageView output = attributedPageView("pv-1", "user-1", null, null);

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(output));

        assertTrue(json.has("attributed_campaign_id"));
        assertTrue(json.get("attributed_campaign_id").isNull());
        assertTrue(json.has("attributed_click_id"));
        assertTrue(json.get("attributed_click_id").isNull());
    }

    @Test
    void verifiesInstantFormatAgainstTheInputOutputContract() throws Exception {
        AttributedPageView output = attributedPageView("pv-1", "user-1", "campaign-1", "click-1");

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(output));

        assertEquals("2026-08-24T10:00:00Z", json.get("event_time").asText());
    }

    @Test
    void attributedPageViewEqualityDependsOnBusinessFields() {
        AttributedPageView first = attributedPageView("pv-1", "user-1", "campaign-1", "click-1");
        AttributedPageView same = attributedPageView("pv-1", "user-1", "campaign-1", "click-1");
        AttributedPageView different = attributedPageView("pv-1", "user-1", "campaign-2", "click-2");

        assertEquals(first, same);
        assertEquals(first.hashCode(), same.hashCode());
        assertNotEquals(first, different);
    }

    private static AttributedPageView attributedPageView(String pageViewId, String userId,
                                                         String campaignId, String clickId) {
        return AttributedPageView.builder()
                .pageViewId(pageViewId)
                .userId(userId)
                .eventTime(Instant.parse("2026-08-24T10:00:00Z"))
                .url("https://example.com/product")
                .attributedCampaignId(campaignId)
                .attributedClickId(clickId)
                .build();
    }
}
