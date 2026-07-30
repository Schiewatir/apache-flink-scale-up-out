package com.example.flinkelasticity;

import com.example.flinkelasticity.ElasticityJob.AggregateResult;
import com.example.flinkelasticity.ElasticityJob.Event;
import com.example.flinkelasticity.ElasticityJob.EventWithTotal;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the pure (non-Flink-runtime) logic of {@link ElasticityJob}. */
class ElasticityJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void parseValidJsonProducesEvent() {
        Event event = ElasticityJob.parse(
            "{\"device_id\":\"dev-1\",\"event_type\":\"temp\",\"value\":21.5,\"ts\":\"2026-07-27T00:00:00Z\"}");

        assertNotNull(event);
        assertEquals("dev-1", event.deviceId());
        assertEquals("temp", event.eventType());
        assertEquals(21.5, event.value());
        assertEquals("2026-07-27T00:00:00Z", event.ts());
    }

    @Test
    void parseInvalidJsonReturnsNull() {
        // Unterminated object and missing value both trigger a Jackson parse exception -> null.
        assertNull(ElasticityJob.parse("{"));
        assertNull(ElasticityJob.parse("{\"value\":}"));
    }

    @Test
    void parseMissingFieldsUsesDefaults() {
        Event event = ElasticityJob.parse("{\"device_id\":\"dev-2\"}");

        assertNotNull(event);
        assertEquals("dev-2", event.deviceId());
        assertEquals("", event.eventType());
        assertEquals(0.0, event.value());
        assertEquals("", event.ts());
    }

    @Test
    void envOrDefaultReturnsDefaultWhenKeyAbsent() {
        String result = ElasticityJob.envOrDefault("DEFINITELY_MISSING_ENV_VAR_XYZ", "fallback");
        assertEquals("fallback", result);
    }

    @Test
    void eventWithTotalConvenienceConstructorCopiesEventFields() {
        Event event = new Event("dev-3", "humidity", 40.0, "ts-3");
        EventWithTotal withTotal = new EventWithTotal(event, 100.0);

        assertEquals("dev-3", withTotal.deviceId());
        assertEquals("humidity", withTotal.eventType());
        assertEquals(40.0, withTotal.value());
        assertEquals("ts-3", withTotal.ts());
        assertEquals(100.0, withTotal.runningTotal());
    }

    @Test
    void aggregateComputesCountAverageAndLatestTotal() {
        List<EventWithTotal> events = List.of(
            new EventWithTotal("dev-4", "t", 10.0, "ts", 10.0),
            new EventWithTotal("dev-4", "t", 20.0, "ts", 30.0),
            new EventWithTotal("dev-4", "t", 30.0, "ts", 60.0));

        AggregateResult result = ElasticityJob.aggregate("dev-4", events, 1000L, 2000L);

        assertNotNull(result);
        assertEquals("dev-4", result.deviceId());
        assertEquals(3L, result.count());
        assertEquals(20.0, result.avgValue());
        assertEquals(60.0, result.runningTotal());
        assertEquals(1000L, result.windowStart());
        assertEquals(2000L, result.windowEnd());
    }

    @Test
    void aggregateReturnsNullForEmptyWindow() {
        assertNull(ElasticityJob.aggregate("dev-5", List.of(), 0L, 1L));
    }

    @Test
    void aggregateResultToJsonContainsExpectedFields() throws Exception {
        AggregateResult result = new AggregateResult("dev-6", 5L, 12.5, 62.5, 100L, 200L);

        JsonNode node = MAPPER.readTree(result.toJson());

        assertEquals("dev-6", node.path("device_id").asText());
        assertEquals(5L, node.path("count").asLong());
        assertEquals(12.5, node.path("avg_value").asDouble());
        assertEquals(62.5, node.path("running_total").asDouble());
        assertEquals(100L, node.path("window_start").asLong());
        assertEquals(200L, node.path("window_end").asLong());
        assertTrue(node.has("emitted_at"));
    }
}
