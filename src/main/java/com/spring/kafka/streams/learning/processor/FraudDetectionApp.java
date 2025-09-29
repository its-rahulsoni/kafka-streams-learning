package com.spring.kafka.streams.learning.processor;

import com.spring.kafka.streams.learning.models.Order;
import com.spring.kafka.streams.learning.serdes.JsonPOJOSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * So in short:
 *
 * .process(...) = “hook my custom logic into this stream”.
 * process() method = called automatically for each record.
 * context.forward() = “send this processed record to the next processor or topic”.
 */
public class FraudDetectionApp {

    public static void main(String[] args) {

        // 1️⃣ Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 2️⃣ Define a persistent state store for tracking order counts per customer
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("fraud-store"),
                        Serdes.String(),
                        Serdes.Long()
                );

        builder.addStateStore(storeBuilder);

        // 3️⃣ Create the processor class inline
        /*class FraudProcessor implements Processor<String, Order, String, String> {
            private ProcessorContext<String, String> context;
            private KeyValueStore<String, Long> store;

            @Override
            public void init(ProcessorContext<String, String> context) {
                this.context = context;
                this.store = context.getStateStore("fraud-store");
            }

            @Override
            public void process(Record<String, Order> record) {
                if (record.value() == null || record.value().getTotal_amount() < 500) {
                    return; // skip low-value orders
                }

                Long count = store.get(record.key());
                if (count == null) count = 0L;
                store.put(record.key(), count + 1);

                if (count + 1 > 3) {
                    String alert = "Fraud alert! Customer " + record.key()
                            + " placed " + (count + 1) + " high-value orders.";
                    context.forward(record.withValue(alert));
                }
            }

            @Override
            public void close() {
                // Cleanup resources if needed
            }
        }*/

        // Custom JSON Serdes for POJOs
        JsonPOJOSerde<Order> orderSerde = new JsonPOJOSerde<>(Order.class);


        /**
         * 4️⃣ Integrate processor with topology.
         *
         * How .process(() -> new FraudProcessor(), "fraud-store") works
         *
         * .process(...) attaches your custom Processor to the stream.
         *
         * Kafka Streams will internally do the following for each record:
         * Pull a record from the stream (after re-keying by customer_id in your case).
         * Pass the record to the process() method of your FraudProcessor.
         * Manage the state store "fraud-store" which is accessible inside the processor via context.getStateStore(...).
         *
         * So yes, Kafka Streams automatically invokes the process() method for every incoming record in that stream.
         */
        builder.stream("orders-topic", Consumed.with(Serdes.String(), orderSerde))
                // Re-key by customer_id instead of order_id
                .selectKey((orderId, order) -> String.valueOf(order.getCustomer_id()))
                // Attach processor with state store
                .process(() -> new FraudProcessor(), "fraud-store")
                .to("fraud-alert-topic", Produced.with(Serdes.String(), Serdes.String()));

        // 5️⃣ Start Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 6️⃣ Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
