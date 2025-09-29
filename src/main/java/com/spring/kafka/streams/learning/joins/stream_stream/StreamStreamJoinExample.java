package com.spring.kafka.streams.learning.joins.stream_stream;

import com.spring.kafka.streams.learning.models.Order;
import com.spring.kafka.streams.learning.models.OrderPayment;
import com.spring.kafka.streams.learning.models.Payment;
import com.spring.kafka.streams.learning.serdes.JsonPOJOSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

/**
 * Kafka Streams Joins – Key Concepts
 *
 * Types of Joins:
 * Stream-Stream Join: Joins two continuous streams based on a common key within a time window. Useful for correlating related events (e.g., orders + payments).
 * Stream-Table Join: Joins a stream with a table (KTable) representing the latest state. Useful for enriching events with static or slowly changing reference data (e.g., orders + customer info).
 *
 * Windowing:
 * Required for Stream-Stream Join because events can arrive at different times.
 * Types: Tumbling, Sliding, Hopping (define how the timeline is split for aggregation/join).
 * Grace period can be set to allow late-arriving events.
 *
 * State Management:
 * Kafka Streams automatically maintains local state stores (usually RocksDB) to keep track of intermediate join results.
 * Handles fault tolerance, out-of-order events, and exactly-once processing.
 *
 * Key Concepts for Implementation:
 * Join key: The attribute used to match records across streams or with tables.
 * Join function: Logic to combine two matched records into a single enriched record.
 * Materialization: Optionally store join results in a state store for querying or downstream processing.
 *
 * Common Use Cases:
 * Real-time enrichment (adding customer info, product details).
 * Event correlation (matching orders with payments, clicks with sessions).
 * Fraud detection or anomaly detection based on combined event patterns.
 *
 * Advantages over vanilla consumer + Java stream processing:
 * Handles unbounded streams efficiently.
 * Automatically manages state and time windows.
 * Supports exactly-once semantics and recovery from failures.
 */

/**
 * Question: What happens if i send order with id 100 but no payments with order is 100 even after 2 hours ?
 *
 * 1️⃣ Key points about stream-stream joins.
 *
 * Stream-stream joins in Kafka Streams are always key-based and windowed.
 *
 * In your code, you have:
 * JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
 *
 * This means only records with the same key that arrive within a 5-minute window of each other will be joined.
 * If a matching record does not arrive within the window, the join will never produce an output for that key.
 *
 * -------------------------------------------------------------------------------------------------
 *
 * 2️⃣ In your example
 *
 * Order arrives: orderId = 100.
 *
 * No payment with orderId = 100 arrives even after 2 hours.
 *
 * The join window is 5 minutes, so:
 *
 * The order record waits for a matching payment for up to 5 minutes.
 *
 * After 5 minutes, Kafka Streams drops the unmatched record from its internal state store (used to hold join candidates).
 *
 * Result: No joined record is produced for that order.
 *
 * Stream-stream joins are inner joins by default — they only produce output when both sides have a matching key within the window.
 *
 * -------------------------------------------------------------------------------------------------
 */
public class StreamStreamJoinExample {

    public static void main(String[] args) {

        // 1️⃣ Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-to-stream-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // 2️⃣ Build topology
        StreamsBuilder builder = new StreamsBuilder();

        // Assuming we have Serdes for Order, Payment, OrderPayment (for simplicity using String Serde)
        /**
         * Creating two streams from "orders" and "payments" topics:
         *
         * Each stream reads from a separate Kafka topic.
         * Kafka Streams maps each record to a key-value pair (key = orderId, value = Order/Payment object).
         */
        // Custom JSON Serde ....
        JsonPOJOSerde<Order> orderSerde = new JsonPOJOSerde<>(Order.class);
        JsonPOJOSerde<Payment> paymentSerde = new JsonPOJOSerde<>(Payment.class);

        KStream<String, Order> ordersStream = builder.stream("orders-topic", Consumed.with(Serdes.String(), orderSerde));
        KStream<String, Payment> paymentsStream = builder.stream("payments-topic", Consumed.with(Serdes.String(), paymentSerde));

        /**
         * 3️⃣ Stream-Stream join (windowed)
         *
         * Join mechanics.
         * Key-based: Only pairs with the same key (orderId) are considered.
         * Windowed: Only events that occur within the 5-minute window of each other are joined.
         * The lambda (order, payment) -> ... produces a new enriched object OrderPayment.
         */
        KStream<String, OrderPayment> joined = ordersStream.join(
                paymentsStream,
                (order, payment) -> new OrderPayment(order.getOrder_id(), order.getCustomer_id(), payment.getAmount()),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), orderSerde, paymentSerde)
        );

        /**
         * Key points about stream-stream joins:
         *
         * 1️⃣ How the key is decided ?
         * Kafka Streams always joins streams by key.
         *
         * In your code:
         * ordersStream.join(paymentsStream, ... , StreamJoined.with(Serdes.String(), ...))
         *
         * The String key here (Serdes.String()) is the record key in the Kafka topic.
         * That means Kafka Streams will only attempt to join records from both streams that have the same key.
         * If your topics do not already have a meaningful key (like order_id), you must set it using .selectKey(...) before joining:
         *
         * ordersStream = ordersStream.selectKey((k, order) -> order.getOrder_id());
         * paymentsStream = paymentsStream.selectKey((k, payment) -> payment.getOrderId());
         *
         * After this, Kafka Streams knows which orders match which payments by their order_id key.
         *
         * -------------------------------------------------------------------------------------------------
         *
         * 2️⃣ How the pairing happens ?
         *
         * Stream-stream joins are windowed. In your code:
         *
         * JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
         * This means:
         * For each incoming record in ordersStream, Kafka Streams looks for records in paymentsStream with the same key that arrived within the last 5 minutes.
         * Similarly, for each record in paymentsStream, it looks for matching ordersStream records in the same window.
         *
         * Important: The join is not a Cartesian product of all records. Only records with the same key are compared.
         */

        /**
         * 4️⃣ Output joined stream
         *
         * Here, we use foreach to print each joined record.
         * Alternatively, you could to("joined-topic") to write the enriched events back to Kafka.
         *
         * Output:
         * Joined event - Key: 120 Value: OrderPayment{orderId='120', customerId='1', amount=100.0}
         * Joined event - Key: 121 Value: OrderPayment{orderId='121', customerId='1', amount=100.0}
         * 319:01:17.440 [stream-to-stream-join-app-066c52cc-3780-4543-9444-af474c328996-StreamThread-1] INFO org.apache.kafka.streams.processor.internals.StreamThread -- stream-thread [stream-to-stream-join-app-066c52cc-3780-4543-9444-af474c328996-StreamThread-1] Processed 5 total records, ran 0 punctuators, and committed 2 total tasks since the last update
         * Joined event - Key: 122 Value: OrderPayment{orderId='122', customerId='1', amount=500.0}
         * Joined event - Key: 123 Value: OrderPayment{orderId='123', customerId='1', amount=500.0}
         */
       // joined.foreach((key, value) -> System.out.println("Joined event - Key: " + key + " Value: " + value));

        joined.foreach((key, value) -> {
            System.out.println("Joined event - Key: " + key + " Value: " + value);
            System.out.println("The records from both the kafka topics are combined into one based upon their keys."); // Set breakpoint on this line
        });


        /**
         * 5️⃣ Start Kafka Streams
         *
         * JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)) ensures that orders and payments match if they occur within 5 minutes.
         * Without a window, a join would only match records that arrive at exactly the same time, which is impractical in streaming.
         */
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
