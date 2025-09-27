package com.spring.kafka.streams.learning.windows.tumbling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OrderCountTumblingWindow {

    public static void main(String[] args) throws InterruptedException {
        /**
         * This Kafka Streams application reads order events in JSON format from the "orders-json" topic.
         * It extracts the "product_id" from each order and counts how many orders were placed for each product
         * in 1-minute tumbling windows. The results are printed to the console.
         *
         * Key points:
         * - Input topic: "orders-json" with JSON string values containing "product_id".
         * - Tumbling window: 1 minute (non-overlapping).
         * - Output: Counts of orders per product per window printed to console.
         *
         * Example input message:
         * {"order_id": 123, "product_id": "A1", "quantity": 2, "price": 19.99}
         *
         * Example output:
         * Product=A1 Window=[1633035600000,1633035660000) Count=5
         */

        // ---- 1) Kafka Streams app configuration ----
        Properties props = new Properties();
        // Unique app id for this streams application (used in consumer groups, state stores)
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-counts-app");
        // Kafka bootstrap brokers
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Default serdes for keys and values (we'll read raw JSON strings)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        // Local directory for RocksDB state stores (window state)
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/order-counts");

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000); // 5 seconds


        // ---- 2) Build topology ----
        final StreamsBuilder builder = new StreamsBuilder();
        final ObjectMapper mapper = new ObjectMapper();

        // Read the input topic as KStream<String, String> (key may be null; value is JSON string)
        KStream<String, String> raw = builder.stream("orders-topic", Consumed.with(Serdes.String(), Serdes.String()));

        /*
         * Step: extract order_id from the JSON value and use it as the record key.
         * We keep the original value as-is (not needed here) — we only need the key for counting.
         */
        KStream<String, String> byProduct = raw.map((ignoredKey, value) -> {
            try {
                JsonNode node = mapper.readTree(value);
                String orderId = node.has("order_id") ? node.get("order_id").asText() : "UNKNOWN";
                return KeyValue.pair(orderId, value);
            } catch (Exception e) {
                // Malformed JSON — send to "UNKNOWN" key so it doesn't break the topology
                return KeyValue.pair("UNKNOWN", value);
            }
        });

        /**
         * RAW = what came directly from Kafka.
         * BY_PRODUCT = after you re-keyed by order_id.
         * Confirms records are flowing through the pipeline.
         */
        raw.print(Printed.<String, String>toSysOut().withLabel("RAW"));
        byProduct.print(Printed.<String, String>toSysOut().withLabel("BY_PRODUCT"));

        // Group by key (productId) — now we can apply windowed aggregations per key.
        KGroupedStream<String, String> grouped = byProduct.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        // Create a 1-minute tumbling window and count elements in each window.
        TimeWindows tumbling1min = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        KTable<Windowed<String>, Long> counts = grouped
                .windowedBy(tumbling1min)
                .count(Materialized.as("product-counts-store"));

        // Convert the KTable<Windowed<String>, Long> to a stream and print to console.
        counts.toStream().foreach((windowedKey, count) -> {
            String key = windowedKey.key();
            long windowStart = windowedKey.window().start();
            long windowEnd = windowedKey.window().end();
            System.out.printf("Product=%s Window=[%d,%d) Count=%d%n", key, windowStart, windowEnd, count);
        });


        // ---- 3) Start the Streams application ----
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); // for demo: clear local state so repeated runs behave predictably
        streams.start();

        while (streams.state() != KafkaStreams.State.RUNNING) {
            Thread.sleep(10000); // small delay
        }

      /*  ReadOnlyWindowStore<String, Long> store =
                streams.store(StoreQueryParameters.fromNameAndType(
                        "product-counts-store", QueryableStoreTypes.windowStore()));

        Instant from = Instant.now().minus(Duration.ofMinutes(5));
        Instant to   = Instant.now();

        try (WindowStoreIterator<Long> values = store.fetch("107", from, to)) {
            while (values.hasNext()) {
                KeyValue<Long, Long> next = values.next();
                System.out.printf("Window start=%s Count=%d%n", next.key, next.value);
            }
        }*/

        /**
         * Periodically query the state store for counts of a specific product (e.g., "107") over the last 5 minutes.
         * This runs every 10 seconds.
         */
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            ReadOnlyWindowStore<String, Long> store = streams.store(
                    StoreQueryParameters.fromNameAndType("product-counts-store", QueryableStoreTypes.windowStore())
            );

            Instant from = Instant.now().minus(Duration.ofMinutes(5));
            Instant to = Instant.now();

            try (WindowStoreIterator<Long> values = store.fetch("107", from, to)) {
                while (values.hasNext()) {
                    KeyValue<Long, Long> next = values.next();
                    System.out.printf("Window start=%s Count=%d%n", next.key, next.value);
                }
            }
        }, 0, 10, TimeUnit.SECONDS); // runs every 10 seconds



        // Add shutdown hook to close cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



    }
}

