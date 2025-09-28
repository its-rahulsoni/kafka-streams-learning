package com.spring.kafka.streams.learning.windows.tumbling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class FraudDetectionApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // Step 1: Read transactions as a stream
        KStream<String, String> raw = builder.stream("transactions-topic");

        // Step 2: Re-key by card_id
        KStream<String, String> byCard = raw.map((k, v) -> {
            try {
                JsonNode node = mapper.readTree(v);
                String cardId = node.get("card_id").asText();
                return KeyValue.pair(cardId, v);
            } catch (Exception e) {
                return KeyValue.pair("UNKNOWN", v);
            }
        });

        // Step 3: Apply 1-minute tumbling window and count transactions per card_id
        TimeWindows tumbling = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        KTable<Windowed<String>, Long> counts = byCard
                .groupByKey()
                .windowedBy(tumbling)
                .count(Materialized.as("card-txn-counts-store"));

        // Step 4: Detect fraud (if count > 3)
        counts.toStream().foreach((windowedKey, count) -> {
            String cardId = windowedKey.key();
            long start = windowedKey.window().start();
            long end = windowedKey.window().end();

            if (count > 3) {
                System.out.printf("🚨 FRAUD ALERT! Card=%s had %d txns in Window=[%d,%d)%n",
                        cardId, count, start, end);
            } else {
                System.out.printf("✅ Card=%s had %d txns in Window=[%d,%d)%n",
                        cardId, count, start, end);
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

