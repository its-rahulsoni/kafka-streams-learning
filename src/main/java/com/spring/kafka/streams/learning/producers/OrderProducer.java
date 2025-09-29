package com.spring.kafka.streams.learning.producers;

import com.spring.kafka.streams.learning.models.Order;
import com.spring.kafka.streams.learning.serdes.JsonPOJOSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * This Producer is meant to demonstrate producing Order events to a Kafka topic using a custom JSON SerDe "JsonPOJOSerde.java".
 * It creates a sample Order object and sends it to the "orders-topic".
 */
public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerde.class.getName());

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props,
                new StringSerializer(),
                new JsonPOJOSerde<>(Order.class).serializer()
        );

        Order order = new Order();
        order.setOrder_id(44);
        order.setCustomer_id(2);
        order.setOrder_date("2025-08-21");
        order.setTotal_amount("800.00");

        producer.send(new ProducerRecord<>("orders-topic", String.valueOf(order.getOrder_id()), order));
        producer.close();
    }
}
