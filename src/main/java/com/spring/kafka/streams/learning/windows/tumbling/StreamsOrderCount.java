package com.spring.kafka.streams.learning.windows.tumbling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

/**
 * 👉 Use Case:
 * "Count how many orders per product_id arrive in each 1-minute tumbling window."
 *
 * 🔄 Example in your words:
 *
 * Suppose we set TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)) and process these events (key=productId):
 *
 * Event	productId	    Timestamp	      Window bucket
 * E1	        101	        10:00:05	    10:00:00–10:01:00
 * E2	        101	        10:00:25	    10:00:00–10:01:00
 * E3	        101	        10:01:15	    10:01:00–10:02:00
 *
 * First bucket [10:00:00–10:01:00) → count(101) = 2
 * Second bucket [10:01:00–10:02:00) → count(101) = 1
 *
 * Each bucket result is stored in RocksDB, keyed as something like:
 * (productId=101, window_start=10:00:00)
 * (productId=101, window_start=10:01:00)
 *
 * Key points:
 * ✅ Aggregation is applied inside each bucket.
 *
 * Example: You’re counting orders per product_id.
 * For every tumbling window bucket, Kafka Streams keeps a separate counter per key.
 * At the end of the window (or as events arrive), the counts are materialized (stored).
 *
 *
 * ✅ “Is there a limit to tumbling windows?”
 *
 * No, there’s no fixed limit like “only 100 tumbling windows.”
 * Windows are time-based, not count-based.
 * Kafka Streams will keep creating new windows as long as events come in.
 *
 * However:
 * Windows consume RocksDB storage.
 * To avoid unbounded growth, Kafka Streams automatically purges expired windows (those past windowEnd + grace).
 *
 * ✅ Late event
 * It's an event whose timestamp belongs to an already passed window.
 * Kafka Streams still accepts it if it arrives before windowEnd + grace.
 * Otherwise → dropped.
 *
 */
public class StreamsOrderCount {
    public static void main(String[] args) {

        /**
         * APPLICATION_ID_CONFIG: unique identifier for this Streams application (also used as Kafka consumer group id).
         * All internal topics and state-store names will be prefixed with this id.
         *
         * BOOTSTRAP_SERVERS_CONFIG: tells the Streams app where the Kafka brokers are.
         *
         * DEFAULT_KEY/VALUE_SERDE_CLASS_CONFIG: default serializers/deserializers (SerDes).
         * Here both key & value are strings (JSON text as String). SerDes are critical because Kafka Streams serializes state and sends changelog messages.
         */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-order-count");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /**
         * Build topology objects.
         *
         * StreamsBuilder is the DSL factory where you define sources, transformations and sinks.
         * ObjectMapper is just a JSON parser we’ll use to extract order_id from the JSON payload.
         */
        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        /**
         * Step 1 — Read input topic as a KStream.
         *
         * builder.stream("orders-topic") creates a KStream representing an infinite stream of records from that topic.
         * At this point the records have whatever key the producer sent (often null) and the value is the raw JSON string.
         */
        KStream<String, String> raw = builder.stream("orders-topic");

        /**
         * Step 2 — re-key by order_id (so we can aggregate by product).
         *
         * map transforms each record into a new KeyValue. Here we extract an order_id from the JSON and make it the record key.
         * Why re-key? Aggregations (like counts) are done per key. If the key is null or something else, grouping will be wrong.
         * The try/catch handles malformed JSON so the topology doesn't crash.
         */
        KStream<String, String> byProduct = raw.map((k, v) -> {
            try {
                JsonNode node = mapper.readTree(v);
                String productId = node.get("order_id").asText();
                return KeyValue.pair(productId, v);
            } catch (Exception e) {
                return KeyValue.pair("UNKNOWN", v);
            }
        });

        /**
         * Step 3: Group + Window + Aggregate (Count)
         *
         * This is the core — we are telling Streams:
         * groupByKey() — group events by the current record key (productId).
         * windowedBy(tumbling) — apply a windowing policy to the grouped stream. Here it’s 1 minute tumbling windows with no grace (no late arrivals allowed).
         * .count(...) — maintain a running count per key per window.
         *
         * Materialized.as("product-counts-store") gives the store a stable name so:
         * The aggregation result is persisted in a local state store named product-counts-store (backed by RocksDB),
         * Streams will also create an internal changelog topic for durability and replication.
         *
         * Result: counts is a KTable<Windowed<String>, Long> — a changelog of counts keyed by Windowed<String> (key + window boundaries).
         */
        TimeWindows tumbling = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        KTable<Windowed<String>, Long> counts = byProduct
                .groupByKey()
                .windowedBy(tumbling)
                .count(Materialized.as("product-counts-store"));

        /**
         * Step 4 — convert updates to a stream and act on them.
         *
         * count() produces incremental updates (every time the count changes for a window) — KTable holds the current aggregate; toStream() turns those updates into a KStream of change events.
         * foreach is a terminal operation used here to print/update external systems. In production you’d send to another Kafka topic or write to a DB/ES.
         */
        counts.toStream().foreach((windowedKey, count) -> {
            String key = windowedKey.key();
            long start = windowedKey.window().start();
            long end = windowedKey.window().end();
            System.out.printf("Streams Product=%s Window=[%d,%d) Count=%d%n", key, start, end, count);
        });

        /**
         * Step 5 — start the Streams application.
         *
         * KafkaStreams is the runtime container for the stream processing application.
         * You pass it the topology (from builder.build()) and the configuration (props).
         * start() begins processing data.
         * The shutdown hook ensures graceful shutdown on JVM exit.
         */
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
