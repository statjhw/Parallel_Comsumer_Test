package kaka.comusmer; // 패키지 이름은 본인 환경에 맞게 수정하세요.

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

public class ConsumerByKey {

    private static final Logger log = LoggerFactory.getLogger(ConsumerByKey.class.getSimpleName());

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
        log.info("I am a Parallel Kafka Consumer (Key-ordered) for performance measurement.");

        String groupId = "parallel-consumer-key-group";
        String topic = "binance";

        // Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Parallel Consumer가 오프셋을 관리합니다.

        // Create a standard Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // Parallel Consumer 옵션 설정
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(kafkaConsumer)
                .maxConcurrency(10)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .build();

        // Create the Parallel Consumer  
        ParallelStreamProcessor<String, String> parallelConsumer = ParallelStreamProcessor.createEosStreamProcessor(options);

        try {
            // Subscribe to a topic
            parallelConsumer.subscribe(Collections.singletonList(topic));

            // 시간 측정 및 카운터 초기화
            AtomicInteger totalMessagesRead = new AtomicInteger(0);
            AtomicInteger emptyPolls = new AtomicInteger(0);
            final int maxEmptyPolls = 5; // 5번 연속으로 메시지가 없으면 종료
            long startTime = System.currentTimeMillis();

            log.info("Starting consumption...");

            // 메시지 처리 로직
            parallelConsumer.poll(context -> {
                // 메시지를 받으면 empty poll 카운터 초기화
                emptyPolls.set(0);
                
                // 실제 메시지 처리
                processMessage(context.getSingleConsumerRecord().value());
                
                int currentCount = totalMessagesRead.incrementAndGet();
                
                // 주기적으로 진행 상황 로그
                if (currentCount % 1000 == 0) {
                    log.info("Received {} records.", currentCount);
                }
            });

            // 종료 조건 체크를 위한 별도 스레드
            Thread monitorThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(1000); // 1초마다 체크
                        
                        // 메시지가 없을 때 카운터 증가
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

            // Add a shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                monitorThread.interrupt();
                parallelConsumer.close();
            }));

            // 메인 스레드는 종료까지 대기
            monitorThread.join();

            // 시간 측정 종료 및 결과 출력
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