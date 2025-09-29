package com.spring.kafka.streams.learning.state_store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.kafka.streams.learning.models.Order;
import com.spring.kafka.streams.learning.serdes.JsonPOJOSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Properties;

/**
 * ProductCountApp
 *
 * - Reads JSON order messages (value is JSON string) from "orders-topic".
 * - Extracts productId and re-keys the stream by productId.
 * - Counts orders per product using a RocksDB-backed KTable state store named "product-count-store".
 * - Prints counts as they update.
 *
 * -------------------------------------------------------------------------------------
 *
 * 🔎 How this works internally
 *
 * A new Order event arrives with customer_id=4.
 * → Streams re-keys to "4".
 * → Goes into the group "4".
 * → RocksDB increments count (1 → 2 → 3...).
 * → New count is written to product-count-store and to the Kafka changelog topic.
 * → Printed to console.
 *
 * If the app crashes:
 * On restart, Kafka Streams replays the changelog topic to restore the RocksDB state.
 * This ensures you don’t lose counts.
 */
public class ProductCountApp {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        // 1) Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-counts-app"); // consumer-group + namespace for state
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where RocksDB state files are stored locally
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/product-counts");
        // Commit interval controls how often processed offsets/state are committed to Kafka (affects durability)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // Set default serdes (we'll override specific ones)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Custom JSON Serdes for POJOs
        JsonPOJOSerde<Order> orderSerde = new JsonPOJOSerde<>(Order.class);

        // 2) Read raw orders as String (value contains JSON) from topic "orders-topic"
        KStream<String, Order> rawOrders = builder.stream(
                "orders-topic",
                Consumed.with(Serdes.String(), orderSerde) // key:string (may be null), value:string (JSON)
        );

        /**
         * ***** This is a classic Kafka Streams pattern: re-key → group → count → materialize → query. *****
         *
         * 3) Re-key stream by CustomerId (parsing JSON)  and filter out null keys ....
         *
         * rawOrders: This is your stream of Order objects read from orders-topic.
         * Original key: whatever Kafka assigned (maybe null if the producer didn’t set one).
         * Value: deserialized Order POJO.
         *
         * selectKey:
         * You override the key with customer_id.
         * Example:
         * Order = {orderId=101, customer_id=4, ...}
         * → Key becomes "4".
         * → Value is still the whole Order.
         *
         * Defensive null check:
         * If order is null or customer_id is missing, return null.
         * filter((k, v) -> k != null && v != null):
         * Kafka Streams doesn’t like null keys in operations like joins/aggregations.
         * This drops any “bad” events before moving forward.
         *
         * ✅ Result: ordersByProduct is a KStream with:
         * Key = customer_id (String)
         * Value = Order object
         */
        KStream<String, Order> ordersByProduct = rawOrders
                .selectKey((key, order) -> {
                    // Defensive null check, in case order or customer_id is missing
                    if (order == null || order.getCustomer_id() == 0) {
                        return null;
                    }
                    return String.valueOf(order.getCustomer_id());
                })
                .filter((k, v) -> k != null && v != null); // remove null keys/values

        /**
         * 4) Count occurrences per productId -> create a KTable backed by a state store
         *
         * ordersByProduct is still a stream. To aggregate (like counting), you must group by a key.
         * groupByKey collects all records that share the same key (customer_id here).
         * Grouped.with(...) tells Kafka Streams:
         * Keys are Strings (use Serdes.String()).
         * Values are Order objects (use your custom orderSerde).
         *
         * ✅ Result: You now have a grouped stream (KGroupedStream<String, Order>).
         *
         * -------------------------------------------------------------------------------------------------
         *
         * count(...):
         * For each key (customer_id), count how many events (orders) have been seen.
         * Each incoming order increments the counter.
         *
         * Materialized.as("product-count-store"):
         * This stores the results in a state store backed by RocksDB (embedded key-value DB).
         * Store name = "product-count-store".
         * Kafka Streams will also maintain a changelog topic in Kafka to make it fault-tolerant.
         *
         * .withKeySerde(...) and .withValueSerde(...):
         * Ensure correct serialization/deserialization for state store + changelog topic.
         * Key = String, Value = Long (the count).
         *
         * ✅ Result: A KTable<String, Long> (counts) where:
         * Key = customer_id
         * Value = current count of orders for that customer
         *
         */
        KTable<String, Long> counts = ordersByProduct
                // group by the current key (productId)
                .groupByKey(Grouped.with(Serdes.String(), orderSerde))
                // Materialize the count to a state store named "product-count-store"
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("product-count-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                );

        /**
         * 5) For visibility, convert KTable to stream of updates and print changes.
         *
         * A KTable only stores the latest value per key (like a table in DB).
         * If you want to see updates as they happen, you convert it to a stream:
         * KTable.toStream() → KStream.
         * foreach(...): a terminal operation that applies a side-effect function (printing here).
         *
         * Example output:
         * customerId=3 currentCount=9
         * customerId=3 currentCount=10
         */
        counts.toStream().foreach((customerId, count) ->
                System.out.println("customerId=" + customerId + " currentCount=" + count)
        );

        // 6) Build & start streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Add shutdown hook to close streams cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down streams...");
            streams.close(Duration.ofSeconds(10));
        }));

        streams.start();

    }


}
