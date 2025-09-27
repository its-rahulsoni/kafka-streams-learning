package com.spring.kafka.streams.learning.basics;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        /**
         * 1. Define properties.
         *
         * APPLICATION_ID_CONFIG:
         * A unique ID for this Kafka Streams app.
         * Used to track offsets and state in Kafka’s internal topics.
         * If you run two apps with the same ID, they’ll share consumer groups.
         *
         * BOOTSTRAP_SERVERS_CONFIG:
         * The Kafka broker(s) your app connects to.
         * "localhost:9092" means Kafka is running locally.
         *
         * DEFAULT_KEY_SERDE_CLASS_CONFIG & DEFAULT_VALUE_SERDE_CLASS_CONFIG:
         * SerDe = Serializer/Deserializer.
         * Defines how keys and values are converted between bytes (in Kafka) and Java objects (in your app).
         * Here → both key and value are treated as String
         */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /**
         *  2. Build topology.
         *
         * StreamsBuilder:
         * Used to define your processing logic (the “topology”).
         *
         * builder.stream("my-learning.learning.orders"):
         * Subscribes to a Kafka topic named "my-learning.learning.orders".
         * Every new event from that topic is read into the stream as a record (KStream<Key, Value>).
         * Here → both key and value are String.
         */
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("my-learning.learning.orders", Consumed.with(Serdes.String(), Serdes.String()));

        /**
         * Simple transformation + print example.
         *
         * mapValues:
         * Transforms only the value of each record (key remains unchanged).
         * value -> value.toUpperCase() makes every string uppercase.
         *
         * Example:
         * Input: "order placed"
         * Output: "ORDER PLACED"
         */
        textLines
                .mapValues(value -> value.toUpperCase())
                .foreach((key, value) -> System.out.println("Key=" + key + ", Value=" + value));

        /**
         * 1️⃣ What the key is in the Kafka message ?
         *
         * In our kafka message:
         * "key": {
         *   "schema": {...},
         *   "payload": {
         *     "order_id": 1007
         *   }
         * }
         *
         * The key is just the order_id.
         * Debezium automatically sets the primary key(s) of the table as the Kafka message key.
         * In your orders table, the primary key is likely order_id, so that’s why it’s used as the key.
         *
         * --------------------------------------------------------------------------
         * 2️⃣ How Debezium decides it ?
         *
         * Debezium follows this logic:
         *
         * Case	                        Key in Kafka message
         * Table has single PK	        PK column value → message key
         * Table has composite PK	    JSON object with all PK columns → message key
         * Table has no PK	            Uses a synthetic key (Debezium can generate a key)
         */

        /**
         * Write back to Kafka.
         *
         * Sends the transformed stream into a new Kafka topic "output-topic".
         * Every uppercase record gets published back to Kafka, available for other consumers/apps.
         */
        // uppercased.to("output-topic");

        /**
         * 3. Start Streams application.
         *
         * builder.build() → builds the processing topology.
         * KafkaStreams → starts consuming from the input topic, processing records, and producing results.
         */
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        /**
         * Shutdown hook.
         *
         * Ensures graceful shutdown when you stop the app (e.g., Ctrl+C).
         * Closes resources and commits offsets so you don’t reprocess duplicate data on restart.
         */
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

