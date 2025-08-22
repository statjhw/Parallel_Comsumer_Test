package kaka.comusmer;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerByUnOrdered {

    private static final Logger log = LoggerFactory.getLogger(ConsumerByUnOrdered.class.getSimpleName());

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
        log.info("I am a Parallel Kafka Consumer (Unordered) for performance measurement.");

        String groupId = "parallel-consumer-unordered-group";
        String topic = "binance";

        // Configure Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Initialize standard Kafka consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // Configure parallel consumer options
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(kafkaConsumer)
                .maxConcurrency(10)
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .build();

        // Initialize parallel consumer
        ParallelStreamProcessor<String, String> parallelConsumer = ParallelStreamProcessor.createEosStreamProcessor(options);

        try {
            // Subscribe to topic
            parallelConsumer.subscribe(Collections.singletonList(topic));

            // Initialize timing and counters
            AtomicInteger totalMessagesRead = new AtomicInteger(0);
            AtomicInteger emptyPolls = new AtomicInteger(0);
            final int maxEmptyPolls = 5;
            long startTime = System.currentTimeMillis();

            log.info("Starting consumption...");

            // Message processing logic
            parallelConsumer.poll(context -> {
                // Reset empty poll counter
                emptyPolls.set(0);
                
                // Process message
                processMessage(context.getSingleConsumerRecord().value());
                
                int currentCount = totalMessagesRead.incrementAndGet();
                
                // Log progress periodically
                if (currentCount % 1000 == 0) {
                    log.info("Received {} records.", currentCount);
                }
            });

            // Monitor thread for shutdown condition
            Thread monitorThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(1000);
                        
                        // Increment empty poll counter
                        int currentEmptyPolls = emptyPolls.incrementAndGet();
                        
                        if (currentEmptyPolls >= maxEmptyPolls) {
                            log.info("No records received, empty poll count: {}", currentEmptyPolls);
                            log.info("Reached max empty polls ({}), shutting down...", maxEmptyPolls);
                            parallelConsumer.close();
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            
            monitorThread.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                monitorThread.interrupt();
                parallelConsumer.close();
            }));

            // Wait for completion
            monitorThread.join();

            // Calculate and display results
            long endTime = System.currentTimeMillis();
            double durationSeconds = (endTime - startTime) / 1000.0;
            log.info("--------------------------------------------------");
            log.info("Finished reading all messages.");
            log.info("Total messages read: {}", totalMessagesRead.get());
            log.info("Total time taken: {} seconds", String.format("%.2f", durationSeconds));
            log.info("--------------------------------------------------");

        } catch (Exception e) {
            log.error("Unexpected error in the consumer", e);
        } finally {
            parallelConsumer.close();
            log.info("The consumer is now gracefully shut down.");
        }
    }
}