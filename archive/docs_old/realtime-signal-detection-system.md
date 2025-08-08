# 실시간 신호 감지 및 성과 추적 시스템

## 📋 목차
- [시스템 개요](#시스템-개요)
- [아키텍처 플로우](#아키텍처-플로우)
- [구현된 컴포넌트](#구현된-컴포넌트)
- [데이터 플로우](#데이터-플로우)
- [신호 감지 전략](#신호-감지-전략)
- [성과 추적 메커니즘](#성과-추적-메커니즘)
- [실행 가이드](#실행-가이드)
- [모니터링 대시보드](#모니터링-대시보드)

---

## 🚨 시스템 개요

**실시간 신호 감지 및 성과 추적 시스템**은 Redis에 저장된 관심종목의 히스토리컬 데이터와 Kafka를 통해 수신되는 실시간 데이터를 결합하여 기술적 지표를 계산하고, 매매 신호를 감지한 후 해당 신호의 성과를 실시간으로 추적하는 시스템입니다.

### 🎯 핵심 기능
- **하이브리드 데이터 처리**: Redis 히스토리컬 + Kafka 실시간 데이터 결합
- **실시간 기술적 지표 계산**: RSI, 볼린저밴드, MACD 등
- **자동 신호 감지**: 과매수/과매도, 밴드 터치 등 조건 감지
- **성과 실시간 추적**: 신호 발생 후 수익/손실 모니터링
- **시각화 대시보드**: Streamlit 기반 실시간 모니터링

---

## 🏗️ 아키텍처 플로우

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DuckDB        │───▶│     Redis       │───▶│ Signal Detector │
│ (관심종목 선별)   │    │ (히스토리컬 캐시) │    │  (기술적 분석)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐             │
│ Test Producer   │───▶│     Kafka       │─────────────┘
│  (시뮬레이션)    │    │ (실시간 스트림)  │
└─────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Streamlit     │◀───│     Redis       │◀───│ Performance     │
│   Dashboard     │    │ (신호 & 성과)    │    │   Tracker       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### 핵심 데이터 플로우
1. **📊 데이터 준비**: DuckDB → Redis (관심종목 히스토리컬 데이터)
2. **📡 실시간 수신**: Kafka → Signal Detector (현재 가격 데이터)
3. **🔍 신호 감지**: 히스토리컬 + 실시간 → 기술적 지표 계산
4. **🚨 신호 저장**: Redis에 신호 발생 기록
5. **📈 성과 추적**: 실시간 가격 업데이트로 손익 계산
6. **📱 시각화**: Streamlit에서 실시간 모니터링

---

## 🛠️ 구현된 컴포넌트

### 1. **Redis Client 확장** (`common/redis_client.py`)
```python
# 관심종목 데이터 관리
def set_watchlist_data(symbol, historical_data, metadata)
def get_watchlist_data(symbol)

# 신호 발생 기록
def set_signal_trigger(symbol, signal_data)
def get_active_signals(symbol=None)

# 성과 추적
def update_signal_performance(symbol, trigger_time, current_price)

# 실시간 분석 결과
def set_realtime_analysis(symbol, analysis_data)
def get_realtime_analysis(symbol)
```

### 2. **관심종목 데이터 로더** (`scripts/load_watchlist_to_redis.py`)
```python
class WatchlistDataLoader:
    def load_watchlist_to_redis(days_back=30):
        # DuckDB에서 최근 관심종목 조회
        # 각 종목의 30일 히스토리컬 데이터 추출
        # Redis에 OHLCV + 메타데이터 저장
```

**기능**:
- DuckDB `daily_watchlist` 테이블에서 최근 7일 관심종목 조회
- 각 종목의 30일 히스토리컬 OHLCV 데이터 추출
- 메타데이터 (시가총액, 섹터, 티어) 포함하여 Redis 저장

### 3. **실시간 신호 감지기** (`kafka/realtime_signal_detector.py`)
```python
class RealTimeSignalDetector:
    def analyze_and_detect_signals(symbol, current_data, watchlist_data):
        # 히스토리컬 + 현재 데이터 결합
        # 기술적 지표 계산 (RSI, 볼린저밴드, MACD)
        # 신호 조건 확인 및 감지
        # Redis에 신호 저장
```

**신호 감지 조건**:
- 🔴 **볼린저밴드 상단 터치**: BB Position ≥ 0.95
- 📈 **RSI 과매수**: RSI ≥ 70
- 📉 **RSI 과매도**: RSI ≤ 30  
- 🟢 **MACD 상승**: MACD > Signal Line & Histogram > 0

### 4. **테스트 데이터 Producer** (`kafka/signal_test_producer.py`)
```python
class SignalTestProducer:
    def generate_signal_trigger_data(symbol):
        # 볼린저밴드 상단/하단 터치 유도
        # RSI 과매수/과매도 조건 생성
        # 높은 거래량과 함께 신호 발생
```

**시뮬레이션 전략**:
- 관심종목별 현실적인 가격 변동 (±2%)
- 30% 확률로 신호 유발 데이터 생성
- 볼린저밴드 돌파, 급격한 모멘텀 변화 시뮬레이션

### 5. **Streamlit 신호 추적 대시보드** (`streamlit/pages/05_실시간_Redis_모니터링.py`)
```python
# 🚨 신호 추적 탭
- 활성 신호 실시간 모니터링
- 신호별 성과 계산 및 색상 코딩
- 수익/손실 통계 및 차트
- 신호 타입별 성과 분석
```

---

## 📊 데이터 플로우

### Phase 1: 데이터 준비
```bash
# 1. DuckDB에서 관심종목 및 히스토리컬 데이터 조회
SELECT symbol, name, sector FROM daily_watchlist WHERE scan_date >= ?

# 2. 각 종목의 30일 OHLCV 데이터 추출
SELECT date, open, high, low, close, volume FROM stock_data 
WHERE symbol = ? AND date >= ? ORDER BY date DESC

# 3. Redis에 구조화된 데이터 저장
Key: "watchlist_data:{symbol}"
Value: {
    symbol, historical_data[30일], metadata, updated_at
}
```

### Phase 2: 실시간 신호 감지
```bash
# 1. Kafka에서 실시간 데이터 수신
Topic: "realtime-stock"
Data: {symbol, price, open, high, low, close, volume, timestamp}

# 2. Redis에서 히스토리컬 데이터 조회 및 결합
watchlist_data = redis.get("watchlist_data:{symbol}")
combined_data = historical_data + [current_data]

# 3. 기술적 지표 계산
RSI = calculate_rsi(close_prices, period=14)
BB_upper, BB_middle, BB_lower = calculate_bollinger_bands(close_prices)
MACD, Signal, Histogram = calculate_macd(close_prices)

# 4. 신호 조건 확인
if bb_position >= 0.95:  # 볼린저 상단 터치
    signal = "bollinger_upper_touch"
elif rsi >= 70:          # RSI 과매수
    signal = "rsi_overbought"

# 5. 신호 발생 시 Redis 저장
Key: "signal_trigger:{symbol}:{timestamp}"
Value: {
    signal_type, trigger_price, trigger_time, 
    technical_values, status="active"
}
```

### Phase 3: 성과 추적
```bash
# 1. 활성 신호 조회
active_signals = redis.keys("signal_trigger:*")

# 2. 각 신호의 성과 계산
current_price = realtime_data['price']
price_change = current_price - trigger_price
price_change_pct = (price_change / trigger_price) * 100

# 3. 성과 업데이트
signal['current_price'] = current_price
signal['price_change_pct'] = price_change_pct
signal['last_updated'] = datetime.now()

# 4. 대시보드에서 실시간 표시
🟢 수익: +2% 이상    🔵 소폭 수익: 0~2%
🟡 소폭 손실: 0~-2%  🔴 손실: -2% 이하
```

---

## 🎯 신호 감지 전략

### 1. **볼린저밴드 전략**
```python
# 상단 터치 (과매수 신호)
bb_position = (current_price - bb_lower) / (bb_upper - bb_lower)
if bb_position >= 0.95:
    signal = "bollinger_upper_touch"
    strength = bb_position  # 0.95~1.0
```

### 2. **RSI 전략**
```python
# 과매수/과매도 감지
if rsi >= 70:
    signal = "rsi_overbought"
    strength = rsi / 100
elif rsi <= 30:
    signal = "rsi_oversold" 
    strength = (30 - rsi) / 30
```

### 3. **MACD 전략**
```python
# 골든 크로스 및 상승 모멘텀
if macd > macd_signal and macd_histogram > 0:
    signal = "macd_bullish"
    strength = abs(macd_histogram) / current_price * 1000
```

### 4. **추천 시스템**
```python
def get_recommendation(indicators):
    score = 0
    # RSI 점수 (-2 ~ +2)
    # 볼린저밴드 점수 (-1 ~ +1) 
    # MACD 점수 (-1 ~ +1)
    
    if score >= 2: return "strong_buy"
    elif score >= 1: return "buy"
    elif score <= -2: return "strong_sell"
    elif score <= -1: return "sell"
    else: return "hold"
```

---

## 📈 성과 추적 메커니즘

### 실시간 성과 계산
```python
def update_signal_performance(symbol, trigger_time, current_price):
    signal = redis.get(f"signal_trigger:{symbol}:{trigger_time}")
    
    # 성과 계산
    trigger_price = signal['trigger_price']
    price_change = current_price - trigger_price
    price_change_pct = (price_change / trigger_price) * 100
    
    # 상태 업데이트
    signal.update({
        'current_price': current_price,
        'price_change': price_change,
        'price_change_pct': price_change_pct,
        'last_updated': datetime.now()
    })
    
    return signal
```

### 성과 분류 및 시각화
- **🟢 강한 수익**: +2% 이상 (`background-color: #d4edda`)
- **🔵 약한 수익**: 0% ~ +2% (`background-color: #cce5ff`)
- **🟡 약한 손실**: 0% ~ -2% (`background-color: #fff3cd`)
- **🔴 강한 손실**: -2% 이하 (`background-color: #f8d7da`)

---

## 🚀 실행 가이드

### 1단계: 환경 준비
```bash
cd /home/grey1/stock-kafka3/docker
docker compose up -d
```

### 2단계: 관심종목 데이터 Redis 로딩
```bash
docker compose exec kafka-consumer python /app/scripts/load_watchlist_to_redis.py
```
**출력 예시**:
```
🚀 관심종목 데이터를 Redis에 로딩 시작 (과거 30일)
📋 총 45개 관심종목 발견
✅ AAPL (Apple Inc.): 30일 데이터 저장
✅ MSFT (Microsoft Corporation): 30일 데이터 저장
🎉 완료! 45/45개 관심종목 데이터 Redis 로딩 완료
```

### 3단계: 실시간 신호 감지기 실행
```bash
docker compose exec kafka-consumer python /app/kafka/realtime_signal_detector.py
```
**출력 예시**:
```
🚀 실시간 신호 감지기 시작
📊 토픽: realtime-stock
🔍 신호 조건: 볼린저 밴드 상단 터치, RSI 과매도/과매수
📥 AAPL 실시간 데이터 수신: $150.25
🚨 AAPL 신호 감지: [{'type': 'bollinger_upper_touch', 'strength': 0.962}]
💾 AAPL 신호 저장 완료: bollinger_upper_touch
```

### 4단계: 테스트 데이터 Producer 실행 (별도 터미널)
```bash
docker compose exec kafka-producer python /app/kafka/signal_test_producer.py --interval 5 --signal-prob 0.3
```
**출력 예시**:
```
🚀 45개 종목 실시간 스트리밍 시작
⏱️ 간격: 5초, 🎯 신호 발생 확률: 30%
🚨 AAPL 신호 테스트 데이터: $152.30 (+1.50%)
📊 MSFT: $305.40 (-0.25%)
🚨 GOOGL 신호 테스트 데이터: $142.80 (+2.80%)
```

### 5단계: Streamlit 대시보드 모니터링
- **URL**: http://localhost:8501
- **페이지**: "05_실시간_Redis_모니터링"
- **탭**: "🚨 신호 추적"

---

## 📱 모니터링 대시보드

### 신호 추적 대시보드 구성

#### 1. **성과 요약 메트릭**
```
수익 신호: 12개    손실 신호: 8개    평균 성과: +0.85%    최고 성과: +3.2%
```

#### 2. **활성 신호 테이블**
| Symbol | Signal | Trigger Price | Current Price | Change | Performance | Trigger Time |
|--------|--------|---------------|---------------|--------|-------------|--------------|
| AAPL | 🔴 볼린저 상단 터치 | $150.25 | $153.80 | +2.36% | 🟢 | 2025-07-30 14:30:15 |
| MSFT | 📉 RSI 과매도 | $305.40 | $308.20 | +0.92% | 🔵 | 2025-07-30 14:28:42 |

#### 3. **성과 차트**
- **신호별 성과 바 차트**: 종목별 수익/손실 시각화
- **신호 타입별 통계**: 각 신호 유형의 평균 성과

#### 4. **실시간 업데이트**
- **자동 새로고침**: 5초마다 자동 업데이트
- **실시간 성과 추적**: 0.1% 이상 변화 시 로그 출력
- **알림 시스템**: 🟢/🔴 아이콘으로 즉시 상태 확인

### 추가 모니터링 탭

#### **📊 실시간 데이터 탭**
- Redis 실시간 가격 데이터
- 기술적 지표 현황
- 종목별 추천 등급

#### **🔧 Redis 상태 탭**  
- 연결된 클라이언트 수
- 메모리 사용량
- 키 개수 및 TTL 정보

#### **🔑 키 관리 탭**
- 신호 키 검색 및 조회
- TTL 확인 및 데이터 내용 확인

#### **🚀 Kafka 부하테스트 모니터링 탭** (신규 추가)
- **URL**: http://localhost:8501 → "06_Kafka_부하테스트_모니터링"
- **실시간 RPS 모니터링**: 초당 요청 수 실시간 그래프
- **응답시간 분포**: 지연시간 히스토그램 차트
- **토픽별 처리 현황**: 각 토픽의 메시지 처리량, Consumer Lag
- **시스템 리소스**: CPU, 메모리 사용률 실시간 게이지
- **오류 분석**: 오류 유형별 분포 및 로그 스트림
- **테스트 설정**: 부하테스트 매개변수 조정 및 실행

---

## 🔧 트러블슈팅

### 일반적인 문제

#### 1. **관심종목 데이터 없음**
```bash
# 증상: "관심종목이 아님, 건너뛰기" 메시지
# 해결: 관심종목 데이터 재로딩
docker compose exec kafka-consumer python /app/scripts/load_watchlist_to_redis.py
```

#### 2. **신호가 감지되지 않음**
```bash
# 증상: 신호 감지기가 실행되지만 신호 없음
# 해결: 테스트 Producer로 신호 유발 데이터 생성
docker compose exec kafka-producer python /app/kafka/signal_test_producer.py --signal-prob 1.0
```

#### 3. **Redis 연결 오류**
```bash
# 증상: "Redis 연결 오류" 메시지
# 해결: Redis 서비스 상태 확인
docker compose ps redis
docker compose restart redis
```

#### 4. **Kafka Consumer 지연**
```bash
# 증상: 실시간 데이터 수신이 느림
# 해결: Consumer Group 상태 확인
docker compose logs kafka-consumer --tail=50
```

### 성능 최적화

#### Redis 메모리 관리
```bash
# TTL 설정으로 자동 정리
- watchlist_data: 7일
- signal_trigger: 30일  
- realtime_analysis: 5분
```

#### 신호 감지 최적화
```bash
# 배치 처리로 CPU 사용량 감소
# 중복 신호 방지 로직
# 비동기 성과 업데이트
```

---

## 🚀 Kafka 부하테스트 가이드

### 1. **간단한 실행** (추천)
```bash
# 디렉토리 이동
cd /home/grey1/stock-kafka3

# 간편 실행 스크립트 사용
chmod +x run_load_test.sh
./run_load_test.sh

# 옵션 선택:
# 1. 가벼운 테스트 (사용자 5명, 3분)
# 2. 중간 테스트 (사용자 20명, 10분)
# 3. 무거운 테스트 (사용자 50명, 20분)
# 4. Kafka 전용 테스트
# 5. 커스텀 설정
```

### 2. **상세 커스터마이징**
```bash
# Kafka 스트레스 테스트만 실행
python3 run_load_test.py --test-type kafka --kafka-threads 15 --kafka-messages 2000

# 통합 테스트 (API + Kafka)
python3 run_load_test.py --test-type all --users 20 --duration 10m --kafka-threads 10 --kafka-messages 500

# 신호 감지 시스템 특화 테스트
python3 run_load_test.py --test-type kafka --kafka-threads 5 --kafka-messages 1000 --topic realtime-stock
```

### 3. **부하테스트 시나리오**

#### **📊 기본 처리량 테스트**
```bash
# 목적: 신호 감지 시스템의 기본 처리 능력 측정
# 설정: 스레드 5개, 스레드당 500개 메시지, 5분간
python3 run_load_test.py --test-type kafka --kafka-threads 5 --kafka-messages 500 --duration 5m

# 예상 결과: 
# - 처리량: ~100-200 msg/sec
# - 지연시간: <100ms
# - 성공률: >95%
```

#### **🔥 고부하 스트레스 테스트**
```bash
# 목적: 시스템 한계점 및 병목 지점 발견
# 설정: 스레드 20개, 스레드당 1000개 메시지, 10분간
python3 run_load_test.py --test-type kafka --kafka-threads 20 --kafka-messages 1000 --duration 10m

# 예상 결과:
# - 처리량: ~500-1000 msg/sec
# - CPU 사용률: 60-80%
# - 메모리 사용률: 1-2GB
```

#### **⚡ 실시간 신호 감지 테스트**
```bash
# 목적: 신호 감지 정확도 및 지연시간 측정
# 신호 테스트 Producer 실행 (높은 신호 발생 확률)
docker compose exec kafka-producer python /app/kafka/signal_test_producer.py --interval 1 --signal-prob 0.8

# 동시에 신호 감지기 실행
docker compose exec kafka-consumer python /app/kafka/realtime_signal_detector.py

# 모니터링: Streamlit 대시보드에서 실시간 확인
# URL: http://localhost:8501 → "05_실시간_Redis_모니터링" → "🚨 신호 추적"
```

### 4. **모니터링 및 결과 분석**

#### **실시간 모니터링 대시보드**
```bash
# 1. Kafka UI에서 토픽 상태 확인
# URL: http://localhost:8080
# - 토픽별 메시지 처리량 (msg/sec)
# - Consumer Lag 모니터링
# - 파티션별 처리 현황
# - Producer/Consumer 연결 상태

# 2. Streamlit에서 신호 감지 상태 확인  
# URL: http://localhost:8501
# - 실시간 신호 감지 현황
# - 성과 추적 대시보드
# - Redis 키 상태 모니터링

# 3. Locust 부하테스트 웹 UI
# URL: http://localhost:8089
# - 실시간 RPS (Requests Per Second)
# - 응답시간 분포 차트
# - 사용자 수 증가 그래프
# - 실패율 및 오류 현황

# 4. 시스템 리소스 모니터링
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}"
```

#### **📊 통합 모니터링 대시보드 구성**
```bash
# 터미널 1: Kafka 부하테스트 실행
python3 run_load_test.py --test-type kafka --kafka-threads 10 --kafka-messages 1000

# 터미널 2: Locust 웹 UI (API 부하테스트)
locust -f load_tests/stock_api_load_test.py --host=http://localhost:8080
# 브라우저: http://localhost:8089

# 터미널 3: 실시간 로그 모니터링
docker compose logs kafka-consumer kafka-producer -f

# 브라우저 탭들:
# - 탭 1: http://localhost:8080 (Kafka UI)
# - 탭 2: http://localhost:8089 (Locust UI) 
# - 탭 3: http://localhost:8501 (Streamlit 신호 감지)
# - 탭 4: htop 또는 시스템 모니터
```

#### **🎯 실시간 부하테스트 모니터링 예시**

**Kafka UI 대시보드** (http://localhost:8080):
```
Topics Overview:
┌──────────────────┬─────────┬──────────┬─────────────┐
│ Topic Name       │ Messages│ Consumers│ Throughput  │
├──────────────────┼─────────┼──────────┼─────────────┤
│ realtime-stock   │ 125,430 │    3     │ 1,250 msg/s│
│ yfinance-stock   │  89,220 │    2     │   890 msg/s│
│ kis-stock        │  45,100 │    1     │   451 msg/s│
└──────────────────┴─────────┴──────────┴─────────────┘

Consumer Groups:
┌─────────────────┬─────────┬─────────────┬──────────┐
│ Group ID        │ Members │ Total Lag  │ Status   │
├─────────────────┼─────────┼─────────────┼──────────┤
│ signal-detector │    1    │      45     │ Stable   │
│ data-processor  │    2    │     128     │ Catching │
└─────────────────┴─────────┴─────────────┴──────────┘
```

**Locust UI 대시보드** (http://localhost:8089):
```
Statistics:
┌──────────────────┬──────────┬─────────┬────────────┬─────────────┐
│ Type             │ # requests│ # fails │ Median (ms)│ Current RPS │
├──────────────────┼──────────┼─────────┼────────────┼─────────────┤
│ KAFKA_SIGNAL     │   8,450  │   85    │     45     │    125.3    │
│ REDIS_CHECK      │   3,220  │   12    │     15     │     48.7    │
│ DB_QUERY         │   1,890  │   28    │    150     │     22.1    │
└──────────────────┴──────────┴─────────┴────────────┴─────────────┘

Charts:
📈 Response Times: 실시간 응답시간 그래프
📊 RPS Over Time: 초당 요청 수 추이
👥 Number of Users: 사용자 수 증가 곡선
❌ Failures per Second: 실패율 추이
```

#### **결과 분석 대시보드**
```bash
# 부하테스트 결과 분석 스크립트 실행
python3 analyze_load_test.py --generate-dashboard

# 생성되는 대시보드:
# 1. load_test_report_YYYYMMDD_HHMMSS.html    # 상세 HTML 리포트
# 2. kafka_performance_dashboard.html         # Kafka 성능 대시보드
# 3. system_resource_dashboard.html           # 시스템 리소스 대시보드
# 4. signal_detection_performance.html        # 신호 감지 성능 대시보드

# 결과 파일들:
# - load_test_results_*.csv    # CSV 데이터 
# - performance.log            # 성능 로그
# - kafka_metrics.json         # Kafka 메트릭 데이터
```

### 5. **성능 메트릭 해석**

#### **📈 핵심 지표**
- **처리량 (Throughput)**: 초당 처리 메시지 수
  - 우수: >500 msg/sec
  - 보통: 100-500 msg/sec  
  - 개선 필요: <100 msg/sec

- **지연시간 (Latency)**: 메시지 처리 응답 시간
  - 우수: <50ms
  - 보통: 50-100ms
  - 개선 필요: >100ms

- **성공률 (Success Rate)**: 오류 없이 처리된 비율
  - 목표: >99%
  - 허용: >95%
  - 문제: <95%

#### **🔍 병목 지점 식별**
```bash
# CPU 병목
- 증상: CPU 사용률 >90%, 처리량 정체
- 해결: 프로세스 최적화, 스케일 아웃

# 메모리 병목  
- 증상: 메모리 사용률 >80%, 스왑 발생
- 해결: 메모리 증설, 캐시 최적화

# 네트워크 병목
- 증상: 네트워크 대역폭 포화, 타임아웃 증가
- 해결: 네트워크 업그레이드, 압축 적용

# Kafka 병목
- 증상: Consumer Lag 증가, 파티션 불균형
- 해결: 파티션 수 증가, Consumer 병렬화
```

### 6. **부하테스트 Best Practices**

#### **🎯 테스트 전략**
```bash
# 1. 단계적 부하 증가
./run_load_test.sh → 선택 1 (가벼운 테스트)
./run_load_test.sh → 선택 2 (중간 테스트)  
./run_load_test.sh → 선택 3 (무거운 테스트)

# 2. 장기간 안정성 테스트
python3 run_load_test.py --test-type kafka --kafka-threads 10 --kafka-messages 10000 --duration 60m

# 3. 스파이크 테스트 (급격한 부하 증가)
python3 run_load_test.py --test-type kafka --kafka-threads 50 --kafka-messages 100 --duration 2m
```

#### **📊 테스트 환경 준비**
```bash
# 1. 충분한 시스템 리소스 확보
# - CPU: 최소 4코어 이상
# - 메모리: 최소 8GB 이상
# - 디스크: SSD 권장

# 2. 모니터링 도구 준비
# - Kafka UI: http://localhost:8080
# - Streamlit: http://localhost:8501
# - 시스템 모니터: htop, iostat

# 3. 기준선(Baseline) 측정
# - 부하테스트 전 정상 상태 성능 측정
# - 비교 기준으로 활용
```

---

## 📚 참고 자료

### 기술적 지표 참고
- **RSI**: Relative Strength Index (14일 기준)
- **볼린저밴드**: 20일 이동평균 ± 2 표준편차
- **MACD**: 12일-26일 지수이동평균 차이, 9일 시그널

### Redis 키 구조
```
watchlist_data:{symbol}     - 관심종목 히스토리컬 데이터
signal_trigger:{symbol}:{timestamp} - 신호 발생 기록
realtime_analysis:{symbol}  - 실시간 분석 결과
realtime_price:{symbol}     - 실시간 가격 캐시
```

### Kafka 토픽 구조
```
realtime-stock             - 실시간 주식 데이터
kis-stock                  - KIS API 데이터
yfinance-stock            - yfinance 데이터
```

---

*이 문서는 구현된 신호 감지 및 성과 추적 시스템의 완전한 가이드입니다. 추가 질문이나 개선사항이 있으면 이슈를 등록해 주세요.*
