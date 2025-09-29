package com.spring.kafka.streams.learning.producers;

import com.spring.kafka.streams.learning.models.Customer;
import com.spring.kafka.streams.learning.models.Order;
import com.spring.kafka.streams.learning.serdes.JsonPOJOSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonPOJOSerde.class.getName());

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(props,
                new StringSerializer(),
                new JsonPOJOSerde<>(Customer.class).serializer()
        );

        Customer customer = new Customer();
        customer.setCustomerId("3");
        customer.setName("Charlie");
        customer.setEmail("charlie@example.com");

        producer.send(new ProducerRecord<>("customers-topic", String.valueOf(customer.getCustomerId()), customer));
        producer.close();
    }

}
