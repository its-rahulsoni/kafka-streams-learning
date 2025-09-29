package com.spring.kafka.streams.learning.processor;

import com.spring.kafka.streams.learning.models.Order;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class FraudProcessor implements Processor<String, Order, String, String> {

    /**
     * 1️⃣ ProcessorContext
     * Gives access to:
     * State stores (for persistent counting)
     * Timestamps of records
     * Forwarding to downstream processors or topics
     *
     * 2️⃣ State Store (KeyValueStore)
     * Persistently stores counts per customer
     * Backed by RocksDB (or in-memory)
     * Fault-tolerant: if app restarts, counts are restored from Kafka changelog
     */
    private ProcessorContext<String, String> context;
    private KeyValueStore<String, Long> store;

    @Override
    public void init(ProcessorContext<String, String> context) {
        // Context provides access to state stores, timestamps, and forwarding
        this.context = context;

        // Access the state store for maintaining counts per customer
        this.store = context.getStateStore("fraud-store");
    }

    @Override
    public void process(Record<String, Order> record) {
        /**
         * Step 1: Filtering:
         * Only high-value orders are considered; DSL can do .filter(...) but the Processor API allows richer state-dependent logic.
         */
        if (record.value() == null || Double.parseDouble(record.value().getTotal_amount()) < 500) {
            return; // Skip processing
        }

        /**
         * Step 2: Count orders per customer
         *
         * Counts per key (customer_id)
         * Uses state store for fault-tolerant storage
         */
        Long currentCount = store.get(record.key());
        if (currentCount == null) currentCount = 0L;
        store.put(record.key(), currentCount + 1);

        /**
         * Step 3: Trigger alert if threshold exceeded
         *
         * Forward the record with alert information
         * Can send to fraud-alert-topic
         */
        if (currentCount + 1 > 3) {
            String alertMessage = "Fraud alert! Customer " + record.key()
                    + " placed " + (currentCount + 1) + " high-value orders.";
            System.out.println("Alert: " + alertMessage);

            /**
             * Forward to downstream topic (or processor).
             *
             * What context.forward(record.withValue(alertMessage)) does
             *
             * context is the ProcessorContext, which represents the runtime environment of your processor inside the topology.
             * forward() sends the processed record downstream to:
             * Another processor in the topology or
             * An output topic (if .to() is defined after .process()).
             *
             * In your example:
             * context.forward(record.withValue(alertMessage));
             *
             * You are creating a new record (keeping the same key) but changing the value to alertMessage.
             * This record is sent to the next node in the topology — in your case, .to("fraud-alert-topic").
             */
            context.forward(record.withValue(alertMessage));
        }
    }

    @Override
    public void close() {
        // Clean up resources if needed
    }
}
