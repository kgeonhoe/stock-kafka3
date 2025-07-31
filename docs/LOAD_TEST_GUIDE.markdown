# 📊 Stock-Kafka3 부하테스트 및 모니터링 시스템

## 🎯 개요
Stock-Kafka3 프로젝트의 성능 검증과 장애 대응 능력을 테스트하기 위한 종합적인 부하테스트 시스템입니다.

## 📁 파일 구조 및 역할

### 🔧 **핵심 구성 파일**

| 파일명 | 위치 | 역할 | 활용 용도 |
|--------|------|------|-----------|
| **requirements-airflow.txt** | `/` | 부하테스트 라이브러리 의존성 | 필수 패키지 설치 |
| **run_load_test.py** | `/` | 통합 부하테스트 실행기 | 메인 테스트 실행 |
| **run_load_test.sh** | `/` | 간편 실행 스크립트 | 원클릭 테스트 시작 |
| **analyze_load_test.py** | `/` | 결과 분석 도구 | 테스트 후 성능 분석 |

### 📊 **부하테스트 모듈**

| 파일명 | 위치 | 역할 | 활용 용도 |
|--------|------|------|-----------|
| **stock_api_load_test.py** | `/load_tests/` | Locust 부하테스트 시나리오 | API 성능 테스트 |
| **performance_monitor.py** | `/monitoring/` | 성능 모니터링 시스템 | 실시간 메트릭 수집 |

### 🛡️ **장애 대응 강화**

| 파일명 | 위치 | 역할 | 활용 용도 |
|--------|------|------|-----------|
| **daily_watchlist_dag.py** | `/airflow/dags/` | 강화된 Airflow DAG | 재시도/Circuit Breaker 적용 |

## 🚀 부하테스트 실행 방법

### 1️⃣ **간편 실행 (추천)**
```bash
cd /home/grey1/stock-kafka3
./run_load_test.sh
```

### 2️⃣ **Python 직접 실행**
```bash
# 전체 테스트 (API + Kafka + 모니터링)
python3 run_load_test.py --test-type all --users 20 --duration 10m

# Kafka 전용 테스트
python3 run_load_test.py --test-type kafka --kafka-threads 15 --kafka-messages 1000

# API 전용 테스트
python3 run_load_test.py --test-type locust --users 50 --duration 5m
```

### 3️⃣ **Locust 웹 UI 실행**
```bash
locust -f load_tests/stock_api_load_test.py --host=http://localhost:8080
# 웹 브라우저에서 http://localhost:8089 접속
```

---

## 🕷️ Locust 부하테스트 상세 가이드

### 1. **Locust 기본 실행**

#### **💻 명령줄 실행**
```bash
# 기본 웹 UI 모드
cd /home/grey1/stock-kafka3
locust -f load_tests/stock_api_load_test.py --host=http://localhost:8080

# 웹 브라우저에서 http://localhost:8089 접속
# Number of users: 동시 사용자 수
# Spawn rate: 초당 사용자 증가율
# Host: 테스트 대상 서버 (http://localhost:8080)
```

#### **🚀 헤드리스 모드 (자동화)**
```bash
# 사용자 50명, 초당 10명씩 증가, 5분간 실행
locust -f load_tests/stock_api_load_test.py \
    --host=http://localhost:8080 \
    --users 50 \
    --spawn-rate 10 \
    --run-time 5m \
    --headless \
    --html=locust_report.html \
    --csv=locust_results

# 결과 파일:
# - locust_report.html     # HTML 리포트
# - locust_results_stats.csv    # 통계 데이터
# - locust_results_failures.csv # 실패 내역
```

### 2. **테스트 시나리오 구성**

#### **📊 현재 구현된 시나리오** (`stock_api_load_test.py`)

```python
class StockAPILoadTest(HttpUser):
    wait_time = between(1, 3)  # 요청 간 1-3초 대기
    
    @task(3)  # 가중치 3 (가장 자주 실행)
    def test_yfinance_data_collection(self):
        """📈 주식 데이터 수집 API 테스트"""
        # - yfinance API 호출 시뮬레이션
        # - 응답시간, 메모리 사용량 측정
        # - 90% 성공률로 시뮬레이션
    
    @task(2)  # 가중치 2
    def test_kafka_message_production(self):
        """📤 Kafka 메시지 전송 테스트"""
        # - 실제 Kafka 메시지 전송
        # - 전송 시간, 파티션 정보 측정
        # - 타임아웃 5초 설정
    
    @task(1)  # 가중치 1 (가장 적게 실행)
    def test_database_heavy_operation(self):
        """🗃️ 데이터베이스 집약적 작업 테스트"""
        # - 무거운 DB 쿼리 시뮬레이션
        # - 0.1-0.5초 처리시간
        # - 85% 성공률
```

### 3. **웹 UI 사용법**

#### **🌐 Locust 웹 대시보드 (http://localhost:8089)**

**메인 화면 설정**:
```
┌─────────────────────────────────────┐
│ Number of users (peak concurrency) │ 50   │  ⬅️ 최대 동시 사용자
├─────────────────────────────────────┼──────┤
│ Spawn rate (users started/second)  │ 10   │  ⬅️ 초당 사용자 증가율
├─────────────────────────────────────┼──────┤
│ Host (e.g. http://www.example.com)  │ http://localhost:8080 │
└─────────────────────────────────────┴──────┘
                [Start swarming] 🚀
```

**실시간 통계 화면**:
- **Type**: 요청 유형 (GET, KAFKA, DB)
- **Name**: 요청 이름 (/api/stock/AAPL/daily)
- **# requests**: 총 요청 수
- **# fails**: 실패 요청 수
- **Median (ms)**: 중간값 응답시간
- **90%ile (ms)**: 90% 응답시간
- **Average (ms)**: 평균 응답시간
- **Min/Max (ms)**: 최소/최대 응답시간
- **Average size (bytes)**: 평균 응답 크기
- **Current RPS**: 현재 초당 요청 수

### 4. **고급 설정 및 커스터마이징**

#### **🎯 신호 감지 시스템 특화 테스트**
```bash
# 신호 감지 시스템용 커스텀 Locust 파일 생성
cat << 'EOF' > signal_detection_load_test.py
from locust import HttpUser, task, between
import json
import random
import time

class SignalDetectionLoadTest(HttpUser):
    wait_time = between(0.5, 2)  # 더 빠른 간격
    
    def on_start(self):
        self.watchlist_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    @task(5)
    def send_realtime_stock_data(self):
        """실시간 주식 데이터 전송"""
        symbol = random.choice(self.watchlist_symbols)
        
        # 신호 유발 가능성이 높은 데이터 생성
        price_change = random.uniform(-3, 3)  # ±3% 변동
        
        data = {
            'symbol': symbol,
            'price': round(150 + (150 * price_change / 100), 2),
            'volume': random.randint(10000, 500000),
            'timestamp': time.time(),
            'source': 'locust_load_test'
        }
        
        # Kafka Producer 시뮬레이션 (실제로는 Kafka 전송)
        response_time = random.uniform(0.01, 0.1)  # 10-100ms
        
        self.environment.events.request.fire(
            request_type="KAFKA_SIGNAL",
            name=f"realtime_data_{symbol}",
            response_time=response_time * 1000,
            response_length=len(json.dumps(data)),
            exception=None
        )
    
    @task(2)
    def check_signal_detection(self):
        """신호 감지 상태 확인"""
        symbol = random.choice(self.watchlist_symbols)
        
        # Redis 신호 조회 시뮬레이션
        response_time = random.uniform(0.005, 0.05)  # 5-50ms
        
        self.environment.events.request.fire(
            request_type="REDIS_CHECK",
            name=f"signal_check_{symbol}",
            response_time=response_time * 1000,
            response_length=512,
            exception=None
        )
EOF

# 신호 감지 특화 테스트 실행
locust -f signal_detection_load_test.py \
    --host=http://localhost:8080 \
    --users 30 \
    --spawn-rate 5 \
    --run-time 10m \
    --headless \
    --html=signal_detection_report.html
```

#### **📊 다양한 부하 패턴 테스트**

**1. 스파이크 테스트 (급격한 부하 증가)**
```bash
# 1분간 급격히 100명까지 증가
locust -f load_tests/stock_api_load_test.py \
    --host=http://localhost:8080 \
    --users 100 \
    --spawn-rate 100 \
    --run-time 1m \
    --headless
```

**2. 소크 테스트 (장기간 안정성)**
```bash
# 30분간 지속적 부하
locust -f load_tests/stock_api_load_test.py \
    --host=http://localhost:8080 \
    --users 25 \
    --spawn-rate 5 \
    --run-time 30m \
    --headless
```

**3. 볼륨 테스트 (대용량 처리)**
```bash
# 200명 동시 사용자
locust -f load_tests/stock_api_load_test.py \
    --host=http://localhost:8080 \
    --users 200 \
    --spawn-rate 20 \
    --run-time 15m \
    --headless
```

### 5. **결과 분석 및 해석**

#### **📈 핵심 성능 지표**

**응답시간 분석**:
```
Median: 50ms      ← 50% 요청이 이 시간 내 완료
90%ile: 120ms     ← 90% 요청이 이 시간 내 완료  
95%ile: 200ms     ← 95% 요청이 이 시간 내 완료
99%ile: 500ms     ← 99% 요청이 이 시간 내 완료
Max: 2000ms       ← 최대 응답시간
```

**처리량 분석**:
```
Total RPS: 150    ← 초당 총 요청 수
Current RPS: 145  ← 현재 초당 요청 수
Total Requests: 45,000  ← 총 처리 요청
Failures: 450 (1%)      ← 실패율
```

#### **🔍 문제 식별 가이드**

**높은 응답시간 (>1000ms)**:
```bash
# 원인 분석
- CPU 병목: htop으로 CPU 사용률 확인
- 메모리 부족: free -h로 메모리 상태 확인  
- 네트워크 지연: ping, traceroute 확인
- 데이터베이스 락: DuckDB 쿼리 성능 분석
```

**높은 실패율 (>5%)**:
```bash
# 실패 원인 분석
- Kafka 연결 실패: docker compose logs kafka
- 타임아웃 오류: 타임아웃 값 조정
- 리소스 부족: Docker 리소스 할당량 증가
```

### 6. **Docker 환경에서 Locust 실행**

#### **🐳 Docker 컨테이너로 Locust 실행**
```bash
# Locust Docker 이미지 실행
docker run -d \
    --name locust-master \
    -p 8089:8089 \
    -v /home/grey1/stock-kafka3:/mnt/locust \
    locustio/locust \
    -f /mnt/locust/load_tests/stock_api_load_test.py \
    --master \
    --host=http://host.docker.internal:8080

# 워커 노드 추가 (선택사항)
docker run -d \
    --name locust-worker-1 \
    -v /home/grey1/stock-kafka3:/mnt/locust \
    locustio/locust \
    -f /mnt/locust/load_tests/stock_api_load_test.py \
    --worker \
    --master-host=locust-master
```

### 7. **모니터링 통합**

#### **📊 실시간 모니터링 동시 실행**
```bash
# 터미널 1: Locust 실행
locust -f load_tests/stock_api_load_test.py --host=http://localhost:8080

# 터미널 2: 시스템 리소스 모니터링
watch -n 1 'docker stats --no-stream'

# 터미널 3: Kafka 모니터링  
docker compose logs kafka-consumer -f

# 터미널 4: Streamlit 신호 감지 모니터링
# 브라우저: http://localhost:8501 → "05_실시간_Redis_모니터링"
```

#### **📈 통합 분석 대시보드**
```bash
# 부하테스트 중 동시 모니터링
# 1. Locust UI: http://localhost:8089 (부하테스트 상태)
# 2. Kafka UI: http://localhost:8080 (메시지 처리 상태)  
# 3. Streamlit: http://localhost:8501 (신호 감지 상태)
# 4. System: htop, iostat (시스템 리소스)
```

## 📊 테스트 시나리오

### 🌐 **1. API 부하테스트**
- **파일**: `load_tests/stock_api_load_test.py`
- **도구**: Locust
- **목적**: REST API 성능 검증
- **측정 항목**:
  - 응답시간 (평균/최대/95%ile)
  - 처리량 (requests/sec)
  - 오류율 (%)
  - 동시 연결 수

**시나리오 종류**:
```python
@task(3) test_yfinance_data_collection()    # yfinance API 호출
@task(2) test_kafka_message_production()    # Kafka 메시지 전송  
@task(1) test_database_heavy_operation()    # DB 집약적 작업
```

### 📤 **2. Kafka 스트레스 테스트**
- **파일**: `run_load_test.py` 내 `run_kafka_stress_test()`
- **목적**: 메시지 큐 처리 성능 검증
- **측정 항목**:
  - 메시지 처리량 (msg/sec)
  - 지연시간 (latency)
  - 메시지 손실률
  - 프로듀서/컨슈머 성능

**테스트 설정**:
```python
threads=10              # 동시 프로듀서 스레드
messages_per_thread=500 # 스레드당 메시지 수
```

### 🗃️ **3. 데이터베이스 부하테스트**
- **파일**: `monitoring/performance_monitor.py` 내 Circuit Breaker 테스트
- **목적**: DuckDB 동시성 및 성능 검증
- **측정 항목**:
  - 쿼리 실행시간
  - 동시 연결 수
  - 메모리 사용량
  - 데이터 처리량

## 🛡️ 장애 대응 메커니즘

### ⚡ **1. 재시도 로직 (Retry with Exponential Backoff)**
```python
# 파일: monitoring/performance_monitor.py
@retry_with_backoff(max_retries=3, base_delay=1, max_delay=60, backoff_factor=2.0)
def api_function():
    # 실패 시 1초 → 2초 → 4초 간격으로 재시도
```

**적용 위치**:
- `daily_watchlist_dag.py`: Airflow 태스크
- API 호출, DB 연결, Kafka 전송

### 🔌 **2. Circuit Breaker 패턴**
```python
# 파일: monitoring/performance_monitor.py
circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60)
# 5회 연속 실패 시 60초간 차단 후 자동 복구 시도
```

**상태 관리**:
- **CLOSED**: 정상 동작
- **OPEN**: 장애 감지, 요청 차단
- **HALF_OPEN**: 복구 시도

### 📊 **3. 실시간 모니터링**
```python
# 파일: monitoring/performance_monitor.py
monitor.start_monitoring(interval=5)  # 5초마다 메트릭 수집
```

**모니터링 항목**:
- CPU/메모리/디스크 사용률
- 응답시간 분포
- 오류율 추세
- 시스템 리소스 임계값 알림

## 📈 성능 메트릭 수집

### 🔢 **Prometheus 메트릭**
- **포트**: `http://localhost:8000`
- **파일**: `monitoring/performance_monitor.py`

**수집 메트릭**:
```python
REQUESTS_TOTAL        # 총 요청 수
REQUEST_DURATION      # 요청 처리시간
MEMORY_USAGE         # 메모리 사용량
CPU_USAGE           # CPU 사용률
KAFKA_MESSAGES      # Kafka 메시지 수
DATABASE_OPERATIONS # DB 작업 수
```

### 📝 **구조화된 로깅**
- **파일**: `performance.log`
- **형식**: JSON 구조화 로그

**로그 예시**:
```json
{
  "timestamp": "2025-07-27T22:34:48",
  "level": "INFO",
  "event": "API_REQUEST",
  "symbol": "AAPL",
  "response_time": 0.234,
  "status_code": 200
}
```

## 📊 결과 분석

### 📈 **자동 분석 도구**
```bash
# 결과 분석 실행
python3 analyze_load_test.py --generate-report
```

**생성 파일**:
- `load_test_report_YYYYMMDD_HHMMSS.html` - 시각적 결과
- `load_test_results_YYYYMMDD_HHMMSS.csv` - 원시 데이터
- `performance_analysis_YYYYMMDD_HHMMSS.png` - 성능 차트
- `load_test_report_YYYYMMDD_HHMMSS.md` - 종합 보고서

### 📊 **성능 지표 분석**
```python
# 파일: analyze_load_test.py
분석 항목:
- 응답시간 분포 (평균/최대/95%ile)
- 처리량 추세 (req/sec)
- 오류율 분석 (%)
- 시스템 리소스 사용률
- 병목 지점 식별
```

## 🎯 테스트 시나리오별 활용

### 💡 **개발 단계**
```bash
# 가벼운 테스트 (개발 중)
./run_load_test.sh
# 선택: 1 (사용자 5명, 3분)
```

### 🔥 **스테이징 테스트**
```bash
# 중간 부하 테스트
python3 run_load_test.py --users 20 --duration 10m
```

### 💥 **프로덕션 준비**
```bash
# 고부하 테스트
python3 run_load_test.py --users 50 --duration 20m --kafka-threads 20
```

### 🎯 **특정 컴포넌트 테스트**
```bash
# Kafka 전용
python3 run_load_test.py --test-type kafka --kafka-threads 15

# API 전용  
python3 run_load_test.py --test-type locust --users 30
```

## 🔧 환경 설정

### 📦 **의존성 설치**
```bash
# 필수 패키지 설치
pip install -r requirements-airflow.txt
```

**추가된 패키지**:
- `locust>=2.17.0` - 부하테스트 프레임워크
- `prometheus-client>=0.16.0` - 메트릭 수집
- `psutil>=5.9.0` - 시스템 리소스 모니터링  
- `memory-profiler>=0.61.0` - 메모리 프로파일링

### 🐳 **Docker 서비스**
```bash
# Kafka 서비스 시작
docker-compose up -d kafka

# Airflow 서비스 시작
docker-compose up -d airflow-webserver airflow-scheduler
```

## 📋 과제 제출용 체크리스트

### ✅ **부하 시나리오 설정**
- [x] API 부하테스트 시나리오
- [x] Kafka 스트레스 테스트
- [x] 데이터베이스 동시성 테스트
- [x] 시스템 리소스 모니터링

### ✅ **장애 대응 코드**
- [x] 재시도 로직 (Exponential Backoff)
- [x] Circuit Breaker 패턴
- [x] 타임아웃 처리
- [x] 예외 처리 및 복구

### ✅ **로깅 시스템**
- [x] 구조화된 성능 로그
- [x] Prometheus 메트릭 수집
- [x] 실시간 시스템 모니터링
- [x] 오류 추적 및 분석

### ✅ **실험 결과 정리**
- [x] 자동 결과 분석 도구
- [x] 시각화 차트 생성
- [x] 종합 보고서 생성
- [x] 성능 지표 요약

## 🚀 실행 예시

### 📝 **Step 1: 환경 준비**
```bash
cd /home/grey1/stock-kafka3
pip install -r requirements-airflow.txt
docker-compose up -d kafka
```

### 🎯 **Step 2: 부하테스트 실행**
```bash
# 간편 실행
./run_load_test.sh
# 선택: 2 (중간 테스트)
```

### 📊 **Step 3: 결과 분석**
```bash
# 분석 실행
python3 analyze_load_test.py --generate-report

# 결과 확인
ls -la *.html *.csv *.png *.md
```

## 📈 기대 성과

### 🎯 **성능 목표**
- **응답시간**: 평균 < 500ms, 95% < 1,000ms
- **처리량**: > 100 req/s
- **오류율**: < 1%
- **가용성**: > 99.9%

### 🛡️ **안정성 검증**
- 장애 상황에서 자동 복구
- 부하 증가 시 성능 저하 최소화
- 시스템 리소스 효율적 사용

---

## 📞 문의 및 지원

**프로젝트**: Stock-Kafka3  
**작성자**: 부하테스트 시스템  
**업데이트**: 2025-07-27  

이 문서는 Stock-Kafka3 프로젝트의 부하테스트 시스템을 위한 완전한 가이드입니다. 추가 질문이나 개선사항이 있으면 언제든지 말씀해 주세요! 🚀
