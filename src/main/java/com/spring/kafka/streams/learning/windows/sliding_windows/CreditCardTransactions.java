package com.spring.kafka.streams.learning.windows.sliding_windows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * 🔹 Walking Through Your Example:
 *
 * Events arriving at:
 * E1 = 12:01
 * E2 = 12:03
 * E3 = 12:05
 * E4 = 12:07
 *
 * Window size = 5 min
 *
 * Step 1: E1 (12:01) arrives
 * No earlier events → no pair → just sits there.
 *
 * Step 2: E2 (12:03) arrives
 * Difference from E1 = 2 min ≤ 5 min.
 * Kafka Streams creates a window: [12:01–12:06) → {E1, E2}, Count=2.
 *
 * Step 3: E3 (12:05) arrives
 * Now Kafka Streams checks:
 * Difference E3–E1 = 4 min ≤ 5 → fits same window.
 * Difference E3–E2 = 2 min ≤ 5 → also fits.
 *
 * 👉 What happens?
 *
 * The window [12:01–12:06) is still valid.
 * E3 falls inside it, so that window’s count gets updated: {E1, E2, E3}, Count=3.
 * Kafka Streams doesn’t always “create a new window” — it reuses an existing one if timestamps overlap.
 *
 * So here’s the correction to what I wrote earlier:
 * ✔ E1, E2, E3 all share the same window [12:01–12:06) → Count=3.
 *
 * Step 4: E4 (12:07) arrives
 *
 * Difference E4–E1 = 6 min → too far → ❌ not in same window.
 * Difference E4–E2 = 4 min ≤ 5 → new overlapping window [12:03–12:08) → {E2, E3, E4}, Count=3.
 * Difference E4–E3 = 2 min ≤ 5 → same [12:03–12:08) window.
 *
 * So now we have:
 * [12:01–12:06) → Count=3 (E1, E2, E3)
 * [12:03–12:08) → Count=3 (E2, E3, E4)
 *
 * 🔹 Key Takeaways
 *
 * Sliding windows overlap — multiple windows can exist simultaneously.
 * A window is defined by event timestamps, not wall-clock alignment.
 * Late events (if allowed by grace period) can update an earlier window retroactively.
 */
public class CreditCardTransactions {

    public static void main(String[] args) {

        /**
         * Properties configure the Kafka Streams app.
         *
         * APPLICATION_ID_CONFIG: unique name for this app. Kafka uses it to track offsets and state.
         * BOOTSTRAP_SERVERS_CONFIG: where the app connects to Kafka.
         * DEFAULT_KEY_SERDE_CLASS_CONFIG: default serializer/deserializer for keys (here, String).
         * DEFAULT_VALUE_SERDE_CLASS_CONFIG: default serializer/deserializer for values (here, String).
         */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "credit-card-fraud-detection-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /**
         * The DSL entry point.
         * You use the builder to define your processing topology (like SQL query plan but for streams).
         */
        StreamsBuilder builder = new StreamsBuilder();

        /**
         * Assume input topic "transactions" with key=cardId, value=amount.
         *
         * Reads from the Kafka topic transactions.
         * Each message has:
         * key = cardId (e.g., "12345").
         * value = amount (e.g., "250.00" as string).
         * Stored as a KStream<String, String>.
         */
        KStream<String, String> transactions = builder.stream("transactions");

        /**
         * Here’s the meat of the logic 🔑:
         *
         * .groupByKey()
         * Groups transactions by their cardId.
         * So now, all transactions for the same credit card are processed together.
         *
         * .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
         * Defines a sliding window of 5 minutes.
         * Meaning: for each card, we keep track of all transactions that happen within 5 minutes of each other.
         * “NoGrace” → late events are not accepted once a window closes.
         *
         * .count(Materialized.as("transactions-sliding-store"))
         * For each sliding window, count how many transactions occurred.
         * Store the counts in a local RocksDB state store called "transactions-sliding-store".
         * This makes counts queryable later (e.g., via interactive queries).
         *
         * 👉 At this point, you have a KTable:
         * Key = Windowed<String> (cardId + window time range).
         * Value = Long (count of transactions).
         */
        KTable<Windowed<String>, Long> counts = transactions
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)))
                .count(Materialized.as("transactions-sliding-store"));

        /**
         * Converts the table (KTable) into a stream (KStream).
         *
         * For every update to counts:
         * Extract the card ID from the windowed key.
         * Extract the window’s start & end timestamps.
         *
         * Print out a message like:
         * Card 12345 has 3 transactions between 2024-02-01T12:01 and 2024-02-01T12:06
         *
         * 👉 This is where you could also send alerts (instead of printing) when count >= 3.
         */
        counts.toStream().foreach((windowedKey, count) -> {
            String cardId = windowedKey.key();
            long start = windowedKey.window().start();
            long end = windowedKey.window().end();

            System.out.printf("Card %s has %d transactions between %s and %s%n",
                    cardId, count, Instant.ofEpochMilli(start), Instant.ofEpochMilli(end));
        });

        /**
         * Build the processing topology from the builder.
         *
         * Create a KafkaStreams object.
         * Start the stream processing:
         * Subscribes to the topic(s).
         * Begins consuming messages.
         * Maintains state in RocksDB.
         * Continuously updates counts in sliding windows.
         */
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
