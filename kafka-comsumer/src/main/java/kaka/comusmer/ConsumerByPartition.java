package kaka.comusmer; // 패키지 이름은 본인 환경에 맞게 수정하세요.

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

    // 메시지 처리 시뮬레이션 (JSON 파싱 + 간단한 계산)
    private static void processMessage(String message) {
        try {
            // 1. JSON 파싱 시뮬레이션 (문자열 처리)
            if (message.contains("symbol") && message.contains("price")) {
                // 2. 간단한 계산 작업 (CPU 사용)
                double sum = 0;
                for (int i = 0; i < 100; i++) {
                    sum += Math.sqrt(i * message.hashCode());
                }
                
                // 3. 짧은 대기 (I/O 시뮬레이션)
                Thread.sleep(1);
            }
        } catch (Exception e) {
            // 에러 무시 (성능 측정에 집중)
        }
    }

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer for performance measurement.");

        String groupId = "parallel-consumer-partition-group";
        String topic = "binance";

        // Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        try {
            // Subscribe to a topic
            consumer.subscribe(Collections.singletonList(topic));

            int totalMessagesRead = 0;
            int emptyPolls = 0;
            final int maxEmptyPolls = 5; // 5번 연속으로 메시지가 없으면 종료

            // 시간 측정 시작
            long startTime = System.currentTimeMillis();

            log.info("Starting consumption...");

            // Pull for data
            while (emptyPolls < maxEmptyPolls) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    emptyPolls++;
                    log.info("No records received, empty poll count: {}", emptyPolls);
                    continue; // 메시지가 없으면 다시 poll
                }

                emptyPolls = 0; // 메시지를 받으면 카운터 초기화
                int batchSize = records.count();
                
                // 실제 메시지 처리 시뮬레이션
                records.forEach(record -> {
                    processMessage(record.value());
                });
                
                totalMessagesRead += batchSize;
                
                // 배치별 로그 출력
                log.info("Batch: {} records, Total: {} records", batchSize, totalMessagesRead);
                
                // 1000개 단위로도 로그 출력
                if (totalMessagesRead / 1000 > (totalMessagesRead - batchSize) / 1000) {
                    log.info("*** Milestone: {} records processed ***", totalMessagesRead);
                }

            }

            // 시간 측정 종료 및 결과 출력
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