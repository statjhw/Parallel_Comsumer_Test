package kaka.comusmer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerByPartition {

    private static final Logger log = LoggerFactory.getLogger(ConsumerByPartition
            .class.getSimpleName());

    private static void processMessage(String message) {
        try {
            if (message.contains("symbol") && message.contains("price")) {
                double sum = 0;
                for (int i = 0; i < 100; i++) {
                    sum += Math.sqrt(i * message.hashCode());
                }
                Thread.sleep(1);
            }
        } catch (Exception e) {
            // Ignore processing errors
        }
    }

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer for performance measurement.");

        String groupId = "parallel-consumer-partition-group";
        String topic = "binance";

        // Configure Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // Initialize Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        try {
            // Subscribe to topic
            consumer.subscribe(Collections.singletonList(topic));

            int totalMessagesRead = 0;
            int emptyPolls = 0;
            final int maxEmptyPolls = 5;

            // Start timing
            long startTime = System.currentTimeMillis();

            log.info("Starting consumption...");

            // Poll for messages
            while (emptyPolls < maxEmptyPolls) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    emptyPolls++;
                    log.info("No records received, empty poll count: {}", emptyPolls);
                    continue;
                }

                emptyPolls = 0;
                int batchSize = records.count();
                
                // Process messages
                records.forEach(record -> {
                    processMessage(record.value());
                });
                
                totalMessagesRead += batchSize;
                
                // Log batch progress
                log.info("Batch: {} records, Total: {} records", batchSize, totalMessagesRead);
                
                // Log milestone progress
                if (totalMessagesRead / 1000 > (totalMessagesRead - batchSize) / 1000) {
                    log.info("*** Milestone: {} records processed ***", totalMessagesRead);
                }

            }

            // Calculate and display results
            long endTime = System.currentTimeMillis();
            double durationSeconds = (endTime - startTime) / 1000.0;
            log.info("--------------------------------------------------");
            log.info("Finished reading all messages.");
            log.info("Total messages read: {}", totalMessagesRead);
            log.info("Total time taken: {} seconds", String.format("%.2f", durationSeconds));
            log.info("--------------------------------------------------");

        } catch (Exception e) {
            log.error("Unexpected error in the consumer", e);
        } finally {
            consumer.close();
            log.info("The consumer is now gracefully shut down.");
        }
    }
}