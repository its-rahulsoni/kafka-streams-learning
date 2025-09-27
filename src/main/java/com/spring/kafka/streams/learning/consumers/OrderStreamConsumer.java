package com.spring.kafka.streams.learning.consumers;

import com.spring.kafka.streams.learning.models.Order;
import com.spring.kafka.streams.learning.serdes.JsonPOJOSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * This consumer application reads Order events from the "orders-topic" Kafka topic using Kafka Streams.
 * It processes each order by printing its details to the console.
 * The application uses a custom JSON SerDe "JsonPOJOSerde.java" to deserialize Order objects.
 */
public class OrderStreamConsumer {

    public static void main(String[] args) {

        // 1️⃣ Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonPOJOSerde.class); // custom SerDe

        // 2️⃣ Build the topology
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Order> ordersStream = builder.stream(
                "orders-topic",
                Consumed.with(
                        Serdes.String(),                     // key serde
                        new JsonPOJOSerde<>(Order.class)     // value serde
                )
        );

        // 3️⃣ Processing each order
        ordersStream.foreach((key, order) -> {
            System.out.println("Received Order Key: " + key);
            System.out.println("Received Order Value: " + order);
        });

        // Optional: forward processed stream to another topic
        // ordersStream.to("processed-orders-topic", Produced.with(Serdes.String(), new JsonPOJOSerde<>(Order.class)));

        // 4️⃣ Start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to cleanly close the streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
