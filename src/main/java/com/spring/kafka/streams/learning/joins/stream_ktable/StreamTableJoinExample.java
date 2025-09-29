package com.spring.kafka.streams.learning.joins.stream_ktable;

import com.spring.kafka.streams.learning.models.Customer;
import com.spring.kafka.streams.learning.models.Order;
import com.spring.kafka.streams.learning.models.EnrichedOrder;
import com.spring.kafka.streams.learning.serdes.JsonPOJOSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class StreamTableJoinExample {

    public static void main(String[] args) {

        /**
         * 1️⃣ Kafka Streams Configuration
         *
         * Each Kafka Streams app needs an application.id (like a consumer group id).
         * Used to track progress (offsets, state stores).
         * Default serialization/deserialization = String.
         * We override this later with JsonPOJOSerde.
         */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /**
         * 2️⃣ Build the Topology
         *
         * Entry point for defining Kafka Streams topology.
         * All transformations (stream, table, join, etc.) attach to this builder.
         */
        StreamsBuilder builder = new StreamsBuilder();

        // Custom JSON Serdes for POJOs
        JsonPOJOSerde<Order> orderSerde = new JsonPOJOSerde<>(Order.class);
        JsonPOJOSerde<Customer> customerSerde = new JsonPOJOSerde<>(Customer.class);
        JsonPOJOSerde<EnrichedOrder> enrichedOrderSerde = new JsonPOJOSerde<>(EnrichedOrder.class);

        /**
         * 3️⃣ Orders Stream
         *
         * - Reads a stream of orders from "orders-topic"
         * - Key = customer_id (important for joining with customer table)
         */
        KStream<String, Order> ordersStream = builder.stream(
                "orders-topic",
                Consumed.with(Serdes.String(), orderSerde)
        ).selectKey((key, order) -> String.valueOf(order.getCustomer_id()));

        /**
         * 1️⃣ What .selectKey(...) does ?
         *
         * .selectKey(...) changes the key of each record in your stream.
         * In Kafka Streams, every record is a key–value pair: (K, V).
         * When you read from the topic, the key may be null (if producer didn’t set it), or it may not match the field you want to join on.
         * .selectKey(...) lets you reassign the key based on the record’s contents.
         *
         * Here:
         * (key, order) -> order.getCustomer_id().toString()
         * key: the existing key from the Kafka record (could be null or something else).
         * order: the value (Order object).
         * You’re saying: “Forget the old key. Instead, use the customer_id from the order as the new key.”
         * And because Kafka keys must be serialized, you convert it to String.
         * So now the stream’s key = customer_id, stream’s value = Order.
         *
         * ---------------------------------------------------------------------------------
         *
         * 2️⃣ Why is this needed?
         *
         * You plan to join ordersStream with customersTable.
         * customersTable is keyed by customer_id (the Kafka key of records in customers-topic).
         * For a correct join, both streams/tables must have the same key.
         * By re-keying ordersStream on customer_id, you align both sources on the same join field.
         *
         * Example:
         * orders-topic has:
         * key = null, value = {"orderId":101, "customer_id":4, ...}
         *
         * customers-topic has:
         * key = 4, value = {"customerId":4, "name":"Alice"}
         *
         * If you don’t use .selectKey(...), the order stream key is null, so no join happens.
         * With .selectKey(...), the order stream key = 4, so it can match with the customer table. ✅
         *
         * ---------------------------------------------------------------------------------
         *
         * 3️⃣ Example transformation
         *
         * Input from Kafka (orders-topic):
         *
         * (null, {"orderId":101,"customer_id":4,"amount":250.00})
         * (null, {"orderId":102,"customer_id":2,"amount":60.00})
         *
         * After .selectKey((key, order) -> order.getCustomer_id().toString()):
         *
         * ("4", {"orderId":101,"customer_id":4,"amount":250.00})
         * ("2", {"orderId":102,"customer_id":2,"amount":60.00})
         *
         * Now it’s aligned for joining with customersTable (which has keys "4", "2", ...).
         */

        /**
         * 4️⃣ Customers Table
         *
         * - Reads a changelog KTable from "customers-topic"
         * - The topic must be a **compacted topic**.
         * - Key = customer_id (primary key in customers data)
         *
         * KTable always stores the *latest value per key* → acts like a lookup table.
         *
         * KTable = latest snapshot of data per key:
         * If topic has customer_id=1, {"name": "Alice"}, then later customer_id=1, {"name": "Alicia"},
         * → the table keeps only the latest version.
         * Backed by a compacted topic (Kafka ensures only latest values per key remain).
         *
         * So:
         * orders-topic = transactional data (stream).
         * customers-topic = master/reference data (table).
         *
         * -----------------------------------------------------------------
         *
         * ⚡ How is the KTable populated ?
         * customers-topic acts as a changelog topic.
         * Every message updates the state store backing the KTable.
         *
         * Example:
         * Message: key=1, value={"name": "Alice"} → Table stores {1 → Alice}.
         * Later message: key=1, value={"name": "Alicia"} → Table updates {1 → Alicia}.
         * Internally, Kafka Streams continuously consumes from the topic and updates the KTable.
         * That’s why customers-topic must be a compacted topic: Kafka ensures only the latest record per key is retained, which perfectly matches KTable semantics.
         */
        KTable<String, Customer> customersTable = builder.table(
                "customers-topic",
                Consumed.with(Serdes.String(), customerSerde)
        );

        /**
         * 5️⃣ Stream-Table Join
         *
         * - For every record in ordersStream:
         *   → Kafka Streams looks up the latest customer from customersTable
         *   → Combines them into EnrichedOrder
         *
         * - If customer info is missing, the join result is null (inner join).
         */
        KStream<String, EnrichedOrder> enrichedOrders = ordersStream.join(
                customersTable,
                (order, customer) -> new EnrichedOrder(order, customer), // join logic
                Joined.with(Serdes.String(), orderSerde, customerSerde)
        );

        /**
         * Output the enriched orders to console (for demo purposes):
         * 1	{"order":{"order_id":10,"customer_id":1,"order_date":"2025-09-29","total_amount":"100.00"},"customer":{"customerId":"1","name":"Alice","email":"alice@example.com"}}
         */
        enrichedOrders.foreach((key, value) ->
                System.out.println("EnrichedOrder prepared - Key: " + key + " Value: " + value)
        );


        /**
         * 6️⃣ Output the Enriched Orders
         *
         * - Writes the enriched stream back to Kafka
         * - Consumers can now directly consume enriched data
         */
        enrichedOrders.to("enriched-orders-topic", Produced.with(Serdes.String(), enrichedOrderSerde));

        // 7️⃣ Start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook for graceful exit
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
