# Kafka Consumer 성능 비교 실험

## 개요
기본 Kafka Consumer와 Confluent Parallel Consumer의 처리 순서별 성능 비교 실험

## 테스트 환경
- **Kafka 버전**: 3.4.0
- **토픽**: binance
- **파티션 수**: 3개
- **전체 메시지 수**: 955,152개
- **메시지 처리**: JSON 파싱 시뮬레이션 + CPU 연산 + 1ms I/O 지연

## 컨슈머 유형

### 1. ConsumerByPartition (기본 Kafka Consumer)
- **처리 방식**: 순차 처리
- **동시성**: 단일 스레드
- **순서 보장**: 파티션 레벨 순서 유지

### 2. ConsumerByKey (Parallel Consumer - 키 순서)
- **처리 방식**: 키 기반 순서를 유지하는 병렬 처리
- **동시성**: 10개 스레드
- **순서 보장**: 키별 순서 유지

### 3. ConsumerByUnOrdered (Parallel Consumer - 순서 무관)
- **처리 방식**: 순서 보장 없는 병렬 처리
- **동시성**: 10개 스레드
- **순서 보장**: 순서 보장 없음

## 실험 결과

| 컨슈머 유형 | 처리 시간 
|------------|-----------|
| ConsumerByPartition | 1244초 |
| ConsumerByKey | 131초 |
| ConsumerByUnOrdered | 133초 |

## 주요 발견사항

### 성능 분석

실험 결과 Key > UnOrdered > Partition 순서로 처리 속도가 빨랐다. 같은 파티션의 갯수에서 메세지 처리 속도를 크게 향상된 볼 수 있다. 

UnOrdered 단위 보다 Key 단위 보장이 조금 더 빠르게 처리된 것을 볼 수 있었다. 먼저 Key 단위 처리 시에는 같은 Key는 같은 스레드에 할당된다는 조건이 붙는다. 같은 스레드안에서는 먼저 온 것이 먼저 처리되기 때문에 같은 Key 메세지의 순서가 보장이 된다. 

현재 실험의 Topic을 보면 Key의 갯수가 많고 각 Key 별 메세지 수도 균등하다. 여러 가상화폐의 티커가 key로 저장된다. 따라서 여러 스레드에 균등하게 할당되기 때문에 위 실험에는 Key와 UnOrdered의 성능 차이가 없었던 것으로 보인다.

## 결론
동일한 파티션을 환경에서 **Parallel Consumer**를 사용하면 데이터 동시처리 속도를 향상시키는 것을 확인했다.

파티션을 증가시키기 어려운 환경에서 **Parallel Consumer**는 좋은 선택지가 될 수 있을 것 같다.