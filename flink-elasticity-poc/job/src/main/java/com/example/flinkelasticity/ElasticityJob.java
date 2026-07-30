package com.example.flinkelasticity;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class ElasticityJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrap = envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092");
        String inTopic = envOrDefault("KAFKA_INPUT_TOPIC", "events-in");
        String outTopic = envOrDefault("KAFKA_OUTPUT_TOPIC", "events-out");
        String groupId = envOrDefault("KAFKA_GROUP_ID", "flink-elastic-consumer");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = buildKafkaSource(bootstrap, inTopic, groupId);

        DataStream<String> input = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "kafka-source"
        );

        DataStream<Event> parsed = input
            .map(raw -> parse(raw))
            .returns(TypeInformation.of(Event.class))
            .name("parse-json")
            .filter(e -> e != null)
            .name("drop-invalid");

        KeyedStream<Event, String> keyed = parsed.keyBy(Event::deviceId);

        DataStream<EventWithTotal> withTotals = keyed
            .process(new RunningTotalFunction())
            .name("running-total-state");

        DataStream<AggregateResult> aggregated = withTotals
            .keyBy(EventWithTotal::deviceId)
            .window(SlidingProcessingTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(10)))
            .process(new SlidingAggregateWindow())
            .name("windowed-aggregate");

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(outTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .build();

        aggregated
            .map(result -> result.toJson())
            .returns(Types.STRING)
            .name("to-json")
            .sinkTo(sink)
            .name("kafka-sink-events-out");

        aggregated
            .map(AggregateResult::toJson)
            .returns(Types.STRING)
            .print()
            .name("print-sink");

        env.execute("flink-elasticity-job");
    }

    /**
     * Builds the Kafka source. {@code OffsetsInitializer.committedOffsets(...)} in
     * flink-connector-kafka 5.0.0-2.2 only accepts the Kafka-client {@code OffsetResetStrategy},
     * which is deprecated upstream but is the sole option exposed by the Flink connector API.
     * The suppression is therefore scoped narrowly to this API-boundary call.
     */
    @SuppressWarnings("deprecation")
    static KafkaSource<String> buildKafkaSource(String bootstrap, String inTopic, String groupId) {
        return KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inTopic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
    }

    static Event parse(String raw) {
        try {
            JsonNode node = MAPPER.readTree(raw);
            return new Event(
                node.path("device_id").asText(),
                node.path("event_type").asText(),
                node.path("value").asDouble(),
                node.path("ts").asText()
            );
        } catch (Exception e) {
            return null;
        }
    }

    static String envOrDefault(String key, String value) {
        String found = System.getenv(key);
        return found == null || found.isBlank() ? value : found;
    }

    public static class RunningTotalFunction extends KeyedProcessFunction<String, Event, EventWithTotal> {

        private transient ValueState<Double> totalState;

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<Double> descriptor =
                new ValueStateDescriptor<>("running-total", Double.class);
            totalState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<EventWithTotal> out) throws Exception {
            Double total = totalState.value();
            if (total == null) {
                total = 0.0;
            }
            total += event.value();
            totalState.update(total);
            out.collect(new EventWithTotal(event, total));
        }
    }

    public static class SlidingAggregateWindow
        extends ProcessWindowFunction<EventWithTotal, AggregateResult, String, TimeWindow> {

        @Override
        public void process(
            String key,
            Context context,
            Iterable<EventWithTotal> events,
            Collector<AggregateResult> out
        ) {
            AggregateResult result = aggregate(
                key, events, context.window().getStart(), context.window().getEnd());
            if (result != null) {
                out.collect(result);
            }
        }
    }

    /**
     * Pure aggregation over a window's events. Returns {@code null} when the window is empty.
     * Extracted from {@link SlidingAggregateWindow} so the core math is unit-testable without a
     * Flink window-operator harness.
     */
    static AggregateResult aggregate(String key, Iterable<EventWithTotal> events, long windowStart, long windowEnd) {
        long count = 0;
        double sum = 0;
        double latestTotal = 0;
        for (EventWithTotal e : events) {
            count++;
            sum += e.value();
            latestTotal = e.runningTotal();
        }
        if (count == 0) {
            return null;
        }
        return new AggregateResult(key, count, sum / count, latestTotal, windowStart, windowEnd);
    }

    public record Event(String deviceId, String eventType, double value, String ts) {}

    public record EventWithTotal(
        String deviceId,
        String eventType,
        double value,
        String ts,
        double runningTotal
    ) {
        public EventWithTotal(Event event, double runningTotal) {
            this(event.deviceId(), event.eventType(), event.value(), event.ts(), runningTotal);
        }
    }

    public record AggregateResult(
        String deviceId,
        long count,
        double avgValue,
        double runningTotal,
        long windowStart,
        long windowEnd
    ) {
        public String toJson() {
            Map<String, Object> payload = new HashMap<>();
            payload.put("device_id", deviceId);
            payload.put("count", count);
            payload.put("avg_value", avgValue);
            payload.put("running_total", runningTotal);
            payload.put("window_start", windowStart);
            payload.put("window_end", windowEnd);
            payload.put("emitted_at", System.currentTimeMillis());
            try {
                return MAPPER.writeValueAsString(payload);
            } catch (Exception e) {
                return "{}";
            }
        }
    }
}
