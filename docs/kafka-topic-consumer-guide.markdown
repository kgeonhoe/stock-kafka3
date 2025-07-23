# Kafka 토픽 생성 및 Consumer Group 관리 가이드

## 📋 목차
1. [Kafka 토픽 생성](#kafka-토픽-생성)
2. [Consumer Group 관리](#consumer-group-관리)
3. [토픽 및 Consumer Group 모니터링](#토픽-및-consumer-group-모니터링)
4. [실용적인 예제](#실용적인-예제)
5. [문제 해결](#문제-해결)

---

## 🎯 Kafka 토픽 생성

### 1. 기본 토픽 생성
```bash
# Kafka 컨테이너 접속
docker exec -it kafka bash

# 토픽 생성 (기본 설정)
kafka-topics --bootstrap-server localhost:29092 --create --topic 토픽명

# 파티션과 복제 팩터 지정
kafka-topics --bootstrap-server localhost:29092 --create --topic 토픽명 \
  --partitions 3 \
  --replication-factor 1
```

### 2. 현재 프로젝트에서 사용하는 토픽 생성
```bash
# KIS 주식 데이터 토픽
kafka-topics --bootstrap-server localhost:29092 --create --topic kis-stock \
  --partitions 3 --replication-factor 1

# yfinance 주식 데이터 토픽
kafka-topics --bootstrap-server localhost:29092 --create --topic yfinance-stock \
  --partitions 3 --replication-factor 1

# 일반적인 주식 데이터 토픽
kafka-topics --bootstrap-server localhost:29092 --create --topic stock-data \
  --partitions 3 --replication-factor 1
```

### 3. 토픽 설정 옵션
```bash
# 상세 설정으로 토픽 생성
kafka-topics --bootstrap-server localhost:29092 --create --topic advanced-topic \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config segment.ms=86400000 \
  --config compression.type=snappy
```

**주요 설정 옵션:**
- `--partitions`: 파티션 수 (병렬 처리 수준 결정)
- `--replication-factor`: 복제 팩터 (데이터 안정성)
- `--config retention.ms`: 메시지 보존 시간 (밀리초)
- `--config segment.ms`: 세그먼트 롤링 시간
- `--config compression.type`: 압축 타입 (gzip, snappy, lz4, zstd)

---

## 👥 Consumer Group 관리

### 1. Consumer Group 생성

Consumer Group은 **Consumer가 토픽을 구독할 때 자동으로 생성**됩니다.

#### 일반적인 Kafka Consumer로 그룹 생성
```bash
# 단일 토픽 구독
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic kis-stock \
  --group my-consumer-group

# 여러 메시지 소비 후 종료
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic kis-stock \
  --group my-consumer-group \
  --max-messages 10
```

#### Python에서 Consumer Group 생성
```python
from kafka import KafkaConsumer

# kafka-python 사용
consumer = KafkaConsumer(
    'kis-stock', 'yfinance-stock',
    bootstrap_servers=['kafka:29092'],
    group_id='python-consumer-group',
    auto_offset_reset='latest'
)

for message in consumer:
    print(f"토픽: {message.topic}, 데이터: {message.value}")
```

#### Spark Structured Streaming에서 Consumer Group 설정
```python
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "kis-stock,yfinance-stock") \
    .option("kafka.group.id", "spark-consumer-group") \
    .option("startingOffsets", "latest") \
    .load()
```

### 2. 기존 Consumer Group에 토픽 추가

기존 Consumer Group에 새로운 토픽을 추가하려면, **해당 그룹으로 새 토픽을 구독**하면 됩니다:

```bash
# 기존 그룹에 새 토픽 추가
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic yfinance-stock \
  --group existing-consumer-group \
  --max-messages 1
```

### 3. Consumer Group 설정 변경

Consumer Group의 오프셋을 재설정할 수 있습니다:

```bash
# 오프셋을 처음으로 리셋
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group my-consumer-group \
  --reset-offsets \
  --to-earliest \
  --topic kis-stock \
  --execute

# 오프셋을 최신으로 리셋
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group my-consumer-group \
  --reset-offsets \
  --to-latest \
  --topic kis-stock \
  --execute

# 특정 시점으로 오프셋 리셋
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group my-consumer-group \
  --reset-offsets \
  --to-datetime 2025-07-21T12:00:00.000 \
  --topic kis-stock \
  --execute
```

---

## 📊 토픽 및 Consumer Group 모니터링

### 1. 토픽 정보 확인
```bash
# 모든 토픽 목록
kafka-topics --bootstrap-server localhost:29092 --list

# 특정 토픽 상세 정보
kafka-topics --bootstrap-server localhost:29092 --describe --topic kis-stock

# 토픽의 파티션별 정보
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 \
  --topic kis-stock
```

### 2. Consumer Group 모니터링
```bash
# 모든 Consumer Group 목록
kafka-consumer-groups --bootstrap-server localhost:29092 --list

# 특정 Consumer Group 상세 정보
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --describe --group my-consumer-group

# Consumer Group 상태 확인
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --describe --group my-consumer-group --state
```

### 3. 메시지 확인
```bash
# 토픽의 최신 메시지 확인
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic kis-stock \
  --from-beginning \
  --max-messages 5

# 특정 파티션의 메시지 확인
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic kis-stock \
  --partition 0 \
  --offset 100 \
  --max-messages 10
```

---

## 🚀 실용적인 예제

### 1. 현재 프로젝트에서 새 토픽 추가하기
```bash
# 1. 새 토픽 생성
docker exec -it kafka bash -c "
kafka-topics --bootstrap-server localhost:29092 --create --topic crypto-data \
  --partitions 3 --replication-factor 1
"

# 2. 토픽 생성 확인
docker exec -it kafka bash -c "
kafka-topics --bootstrap-server localhost:29092 --list
"

# 3. 테스트 메시지 전송
docker exec -it kafka bash -c "
echo '{\"symbol\":\"BTC\",\"price\":50000,\"timestamp\":\"$(date -Iseconds)\"}' | \
kafka-console-producer --bootstrap-server localhost:29092 --topic crypto-data
"
```

### 2. Consumer Group으로 여러 토픽 동시 구독
```bash
# test-multi-consumer-group 생성 및 여러 토픽 구독
docker exec -it kafka bash -c "
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic kis-stock,yfinance-stock,crypto-data \
  --group test-multi-consumer-group \
  --max-messages 5
"
```

### 3. Docker Compose 환경에서 Consumer 추가
```yaml
# docker-compose.yml에 새 Consumer 서비스 추가
  crypto-consumer:
    build:
      context: ..
      dockerfile: ./docker/Dockerfile.kafka
    container_name: crypto-consumer
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
      - PYTHONPATH=/app
    volumes:
      - ../kafka:/app/kafka
      - ../common:/app/common
      - ../config:/app/config
    command: >
      sh -c "
        sleep 10 &&
        python /app/kafka/crypto_consumer.py
      "
```

---

## 🛠️ 문제 해결

### 1. Consumer Group이 보이지 않는 경우
```bash
# Consumer가 실제로 실행 중인지 확인
docker compose logs kafka-consumer -f

# Consumer가 메시지를 소비했는지 확인
kafka-consumer-groups --bootstrap-server localhost:29092 --list

# 토픽에 메시지가 있는지 확인
kafka-console-consumer --bootstrap-server localhost:29092 \
  --topic kis-stock --max-messages 1
```

### 2. 토픽 생성 실패
```bash
# Kafka 브로커 상태 확인
docker compose logs kafka --tail=50

# 브로커 연결 테스트
kafka-broker-api-versions --bootstrap-server localhost:29092
```

### 3. Consumer Lag 확인
```bash
# Consumer Group의 지연(lag) 확인
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --describe --group my-consumer-group
```

### 4. Consumer Group 삭제
```bash
# Consumer Group 삭제 (모든 Consumer가 종료된 후)
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --delete --group my-consumer-group
```

---

## 📚 추가 리소스

### Kafka UI 접속 (http://localhost:8080)
- 토픽 목록 및 상세 정보
- Consumer Group 모니터링
- 메시지 브라우징
- 실시간 메트릭

### 유용한 명령어 모음
```bash
# 환경 변수 설정 (반복 작업 편의)
export KAFKA_BROKER="localhost:29092"

# 토픽 목록
alias kafka-topics-list="kafka-topics --bootstrap-server $KAFKA_BROKER --list"

# Consumer Group 목록
alias kafka-groups-list="kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --list"

# 특정 토픽의 메시지 수 확인
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list $KAFKA_BROKER \
  --topic kis-stock \
  --time -1
```

---

**📝 참고사항:**
- Consumer Group ID는 **유니크**해야 합니다
- 토픽 이름은 **알파벳, 숫자, 점(.), 하이픈(-), 언더스코어(_)** 만 사용 가능
- 파티션 수는 **증가만 가능**하며 감소할 수 없습니다
- Consumer Group은 **모든 Consumer가 종료되면 자동으로 정리**됩니다
