# Streamlit Dashboard 사용 가이드

## 📋 목차
1. [대시보드 개요](#대시보드-개요)
2. [페이지별 상세 가이드](#페이지별-상세-가이드)
3. [사용 시나리오](#사용-시나리오)
4. [트러블슈팅](#트러블슈팅)

---

## 🚀 대시보드 개요

Stock Pipeline의 Streamlit 대시보드는 실시간 주식 데이터 모니터링, 기술적 분석, API 성능 테스트 등을 위한 통합 웹 인터페이스입니다.

### 접속 방법
```bash
# Docker Compose로 실행 (권장)
cd /home/grey1/stock-kafka3/docker
docker-compose up -d streamlit

# 브라우저에서 접속
http://localhost:8501
```

### 전체 페이지 구조
```
📊 Main Dashboard (monitoring_dashboard.py)
├── 05_실시간_Redis_모니터링.py
├── 06_Kafka_부하테스트_모니터링.py
└── 07_API_호출_테스트_대시보드.py
```

---

## 📄 페이지별 상세 가이드

### 1️⃣ 실시간 Redis 모니터링 (`05_실시간_Redis_모니터링.py`)

#### 📌 목적
Redis에 캐싱된 실시간 데이터와 기술적 신호를 모니터링합니다.

#### 🎯 주요 기능

##### 신호 추적 탭
```python
활성 신호 모니터링:
- 신호 타입: 볼린저, RSI, MACD 등
- Trigger Price: 신호 발생 시점 가격
- 현재 가격: 실시간 업데이트
- 수익률: (현재가 - Trigger Price) / Trigger Price
- 신호 강도: 조건 충족 정도
```

##### 실시간 데이터 탭
- **가격 데이터**: 실시간 주가, 변동률, 거래량
- **기술적 지표**: RSI, MACD, 볼린저 밴드 값
- **업데이트 주기**: 1초 단위 자동 새로고침

##### Redis 상태 탭
```
모니터링 지표:
- 메모리 사용량: used_memory_human
- 연결 클라이언트: connected_clients
- 초당 명령어: instantaneous_ops_per_sec
- 캐시 히트율: keyspace_hits / (hits + misses)
- TTL 상태: 만료 예정 키 수
```

#### 💡 Trigger Price 이해하기

**관심종목 Trigger Price** (PostgreSQL 기반)
- 정의: 관심종목으로 등록한 날의 종가
- 용도: 장기 수익률 계산 기준
- 특징: 고정값 (변경되지 않음)

**활성 신호 Trigger Price** (Redis 기반)
- 정의: 기술적 조건이 실시간으로 충족된 시점의 가격
- 용도: 단기 매매 타이밍 판단
- 특징: 동적값 (신호마다 다름)

---

### 2️⃣ Kafka 부하테스트 모니터링 (`06_Kafka_부하테스트_모니터링.py`)

#### 📌 목적
Kafka 메시지 큐의 성능 테스트 및 부하 상태를 모니터링합니다.

#### 🎯 주요 기능

##### 부하 테스트 실행
```python
테스트 설정:
- 메시지 전송률: msgs/sec 설정
- 테스트 기간: 1분~60분
- 메시지 크기: 작은/중간/큰 메시지
- 동시 프로듀서 수: 1-10개
```

##### 실시간 모니터링
- **처리량 지표**
  - 초당 메시지 생산량 (msgs/sec)
  - 초당 메시지 소비량 (msgs/sec)
  - 지연(Lag): 미처리 메시지 수

- **성능 지표**
  - 평균 응답 시간 (ms)
  - 처리량 변화 추이
  - 에러율 모니터링

##### 토픽별 상세 분석
- 토픽별 메시지 분포
- 파티션 상태 확인
- 컨슈머 그룹 상태
- 브로커 부하 분산

#### 💡 사용 방법
```python
1. 테스트 설정
   - 토픽 선택: stock-data, signals 등
   - 부하 수준: 낮음/보통/높음
   - 테스트 기간 설정

2. 부하 테스트 실행
   - "부하 테스트 시작" 버튼 클릭
   - 실시간 지표 모니터링
   - 성능 임계점 확인

3. 결과 분석
   - 최대 처리량 확인
   - 병목 지점 식별
   - 최적 설정 도출
```

---

### 3️⃣ API 호출 테스트 대시보드 (`07_API_호출_테스트_대시보드.py`)

#### 📌 목적
외부 API 및 내부 서비스의 호출 성능을 테스트하고 모니터링합니다.

#### 🎯 주요 기능

##### API 테스트 대상
```python
외부 API:
- yfinance API: Yahoo Finance 주식 데이터
- KIS API: 한국투자증권 OpenAPI
- NASDAQ API: 나스닥 심볼 정보

내부 서비스:
- Kafka UI: 관리 인터페이스
- Airflow API: DAG 상태 확인
- Redis: 캐시 상태 확인
```

##### 성능 테스트 기능
- **단일 호출 테스트**
  - API 응답 시간 측정
  - 성공/실패 상태 확인
  - 응답 데이터 검증

- **부하 테스트**
  - 동시 호출 수 설정 (1-100개)
  - 지속 시간 설정
  - 처리량 및 응답 시간 분석

##### 모니터링 지표
```
성능 지표:
- 평균/최소/최대 응답 시간
- 초당 처리 요청 수 (RPS)
- 성공률 (%)
- 에러 발생 빈도와 유형
- 타임아웃 발생률
```

#### 💡 사용 시나리오
```
시나리오 1: API 상태 점검
1. 모든 API 순차 테스트
2. 응답 시간 임계값 확인
3. 에러 발생 API 식별

시나리오 2: 부하 성능 테스트
1. 특정 API 선택
2. 동시 호출 수 증가
3. 성능 한계점 측정
4. 최적 호출 빈도 결정
```

---

## 🔧 트러블슈팅

### 문제 1: Redis 연결 실패
```bash
# 해결 방법
docker-compose restart redis
docker logs redis-stock
```

### 문제 2: Streamlit 페이지 로딩 실패
```python
# 패키지 재설치
pip install -r requirements-streamlit.txt
# 캐시 삭제
streamlit cache clear
```

### 문제 3: API 호출 실패
```python
# 환경변수 확인
1. .env 파일의 API 키 확인
2. KIS API 토큰 만료 확인
3. 네트워크 연결 상태 점검
```

### 문제 4: Kafka 연결 오류
```bash
# Kafka 서비스 상태 확인
docker-compose ps kafka
docker logs kafka

# 토픽 존재 확인
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## 📚 추가 자료

- [Redis 데이터 관리 가이드](redis-data-management.md)
- [Kafka 토픽 컨슈머 가이드](kafka-topic-consumer-guide.markdown)
- [Airflow DAG 구현 상세](airflow-dag-implementation-details.md)

---

## 🚀 빠른 시작 가이드

### 1. Docker Compose로 전체 시스템 실행
```bash
cd /home/grey1/stock-kafka3/docker
docker-compose up -d
```

### 2. Streamlit 대시보드 접속
```bash
# 브라우저에서 접속
http://localhost:8501

# 또는 직접 실행
cd /home/grey1/stock-kafka3
source venv/bin/activate
streamlit run streamlit/monitoring_dashboard.py --server.port 8501
```

### 3. 각 페이지 기능 확인
- **Redis 모니터링**: 실시간 신호 및 데이터 상태
- **Kafka 부하테스트**: 메시지 큐 성능 측정
- **API 테스트**: 외부 API 및 내부 서비스 성능 검증

---

*최종 업데이트: 2025-08-09*
