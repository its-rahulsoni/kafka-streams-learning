package com.spring.kafka.streams.learning.timestamp_extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.Properties;

/**
 * Demonstrates extracting event-time from a JSON field "order_date" using a custom TimestampExtractor,
 * and prints each record together with the timestamp that the extractor produced.
 *
 * Notes:
 * - Expects messages on "orders-topic" with value as JSON string containing an "order_date" field.
 * - The extractor attempts to parse ISO-8601 instants first, then ISO_LOCAL_DATE (yyyy-MM-dd).
 *
 * ----------------------------------------------------------
 *
 * 🔁 Sequence of Events for That Record
 *
 * Kafka Streams consumes record from orders-topic:
 * key=101, value={"order_id":101,"customer_id":4,...}
 *
 * Your custom transformer runs:
 * Calls context.timestamp() → event-time 2025-09-27T00:00:00Z
 * Logs "Record: key=101, extractedTimestamp=..., value=..."
 * Returns the same value.
 *
 * Kafka Streams forwards the unchanged key/value pair downstream.
 * Logs "Forwarded record: key=101, value=..."
 *
 * Next stage in your topology (could be a join, count, or to("topic")) receives this forwarded record.
 *
 */
public class OrderTimestampStreamApp {

    // Reuse a single ObjectMapper instance (thread-safety: this instance is safe for reuse for readValue calls)
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        // --------------------------
        // 1) Configure Kafka Streams
        // --------------------------
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-ts-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // For development: read from earliest if no committed offsets exist
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        // Default SerDes (we override per-stream)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Optional state dir (not used here but good to set)
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/order-ts-app");

        StreamsBuilder builder = new StreamsBuilder();

        // ----------------------------------------------------------
        // 2) Create the stream and attach the custom timestamp extractor
        // ----------------------------------------------------------
        KStream<String, String> orders = builder.stream(
                "orders-topic",
                // tell Kafka Streams to treat key/value as strings AND use our extractor
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new OrderTimestampExtractor())
        );

        // ----------------------------------------------------------
        // 3) Transform so we can access ProcessorContext.timestamp() (which reflects extracted timestamp)
        // ----------------------------------------------------------
        KStream<String, String> withTsPrint = orders.transformValues(
                new ValueTransformerWithKeySupplier<String, String, String>() {
                    @Override
                    public ValueTransformerWithKey<String, String, String> get() {
                        return new ValueTransformerWithKey<String, String, String>() {

                            private ProcessorContext context;

                            @Override
                            public void init(ProcessorContext context) {
                                this.context = context;
                            }

                            @Override
                            public String transform(String readOnlyKey, String value) {
                                long extractedTs = context.timestamp();
                                Instant tsInstant = Instant.ofEpochMilli(extractedTs);
                                System.out.printf("Record: key=%s, extractedTimestamp=%s, value=%s%n",
                                        readOnlyKey, tsInstant, value);
                                return value;
                            }

                            @Override
                            public void close() {
                                // cleanup if needed
                            }
                        };
                    }
                }
        );


        // For demonstration also print a simple forward
        withTsPrint.foreach((k, v) -> {
            // additional printing to emphasize stream continues
            System.out.printf("Forwarded record: key=%s, value=%s%n", k, v);
        });

        // --------------------------
        // 4) Start the streams app
        // --------------------------
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down streams...");
            streams.close();
        }));

        streams.start();
    }

    // ----------------------------------------------------------------
    // Custom TimestampExtractor implementation
    // ----------------------------------------------------------------
    public static class OrderTimestampExtractor implements TimestampExtractor {
        private static final ObjectMapper mapper = OBJECT_MAPPER;

        /**
         * Extract timestamp from the record's value's "order_date" field.
         * expected formats (attempted in order):
         *  1) ISO-8601 instant e.g. 2025-09-28T12:34:56Z
         *  2) ISO_LOCAL_DATE e.g. 2025-09-28 (treated as start of day UTC)
         *  3) numeric epoch millis (if the field is a number)
         * On failure, fall back to partitionTime (Kafka consumer timestamp).
         */
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            Object value = record.value();
            if (value == null) {
                return partitionTime;
            }

            try {
                // We expect value to be JSON string; if it's already a JsonNode or POJO, adjust accordingly
                JsonNode root;
                if (value instanceof String) {
                    root = mapper.readTree((String) value);
                } else {
                    // if producer sent a structured type or a JsonNode, attempt to convert
                    root = mapper.valueToTree(value);
                }

                JsonNode orderDateNode = root.get("order_date");
                if (orderDateNode == null || orderDateNode.isNull()) {
                    return partitionTime;
                }

                // If it's numeric, treat as epoch millis
                if (orderDateNode.isNumber()) {
                    return orderDateNode.asLong();
                }

                // If it's text, try parse as Instant first, then LocalDate fallback
                String text = orderDateNode.asText();

                // Try parsing as full instant (e.g., "2025-09-28T12:34:56Z" or with offset)
                try {
                    Instant inst = Instant.parse(text);
                    return inst.toEpochMilli();
                } catch (DateTimeParseException ignored) {
                    // try next format
                }

                // Try parsing as ISO_LOCAL_DATE (yyyy-MM-dd) -> convert to start-of-day UTC
                try {
                    LocalDate ld = LocalDate.parse(text);
                    Instant inst = ld.atStartOfDay(ZoneOffset.UTC).toInstant();
                    return inst.toEpochMilli();
                } catch (DateTimeParseException ignored) {
                    // fall through to last resort
                }

                // As last resort, attempt parse as long (stringified epoch)
                try {
                    return Long.parseLong(text);
                } catch (NumberFormatException ignored) {
                    // give up and fallback
                }

                // fallback
                return partitionTime;
            } catch (Exception ex) {
                // Any parsing/deserialization error -> fallback to partitionTime
                return partitionTime;
            }
        }
    }
}
