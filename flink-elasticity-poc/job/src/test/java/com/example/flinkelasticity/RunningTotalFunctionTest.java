package com.example.flinkelasticity;

import com.example.flinkelasticity.ElasticityJob.Event;
import com.example.flinkelasticity.ElasticityJob.EventWithTotal;
import com.example.flinkelasticity.ElasticityJob.RunningTotalFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Stateful operator test for {@link RunningTotalFunction} using Flink's keyed process-function
 * test harness. Verifies per-key running-total accumulation across elements.
 */
class RunningTotalFunctionTest {

    @Test
    void accumulatesRunningTotalPerKey() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, EventWithTotal> harness =
                 ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                     new RunningTotalFunction(),
                     Event::deviceId,
                     Types.STRING)) {

            harness.open();

            harness.processElement(new Event("dev-a", "t", 10.0, "ts1"), 1L);
            harness.processElement(new Event("dev-a", "t", 5.0, "ts2"), 2L);
            harness.processElement(new Event("dev-b", "t", 100.0, "ts3"), 3L);
            harness.processElement(new Event("dev-a", "t", 2.5, "ts4"), 4L);

            List<EventWithTotal> output = harness.extractOutputValues();

            assertEquals(4, output.size());
            // dev-a running totals: 10.0, 15.0, 17.5
            assertEquals(10.0, output.get(0).runningTotal());
            assertEquals(15.0, output.get(1).runningTotal());
            // dev-b independent state: 100.0
            assertEquals(100.0, output.get(2).runningTotal());
            assertEquals(17.5, output.get(3).runningTotal());
        }
    }
}
