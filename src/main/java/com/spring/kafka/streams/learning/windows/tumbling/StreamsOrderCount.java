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
 */
public class StreamsOrderCount {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-order-count");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // Step 1: Read stream
        KStream<String, String> raw = builder.stream("orders-topic");

        // Step 2: Re-key by product_id
        KStream<String, String> byProduct = raw.map((k, v) -> {
            try {
                JsonNode node = mapper.readTree(v);
                String productId = node.get("order_id").asText();
                return KeyValue.pair(productId, v);
            } catch (Exception e) {
                return KeyValue.pair("UNKNOWN", v);
            }
        });

        // Step 3: Group + Window + Count
        TimeWindows tumbling = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        KTable<Windowed<String>, Long> counts = byProduct
                .groupByKey()
                .windowedBy(tumbling)
                .count(Materialized.as("product-counts-store"));

        // Step 4: Print results
        counts.toStream().foreach((windowedKey, count) -> {
            String key = windowedKey.key();
            long start = windowedKey.window().start();
            long end = windowedKey.window().end();
            System.out.printf("Streams Product=%s Window=[%d,%d) Count=%d%n", key, start, end, count);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
