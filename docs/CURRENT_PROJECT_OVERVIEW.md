# 📈 Stock Kafka3 - PostgreSQL 기반 실시간 주식 데이터 파이프라인

## 🎯 프로젝트 개요

**Stock Kafka3**는 PostgreSQL을 중심으로 한 실시간 주식 데이터 수집, 처리, 분석 파이프라인입니다. 
Kafka 스트리밍, Redis 캐싱, Streamlit 모니터링을 통해 완전한 실시간 데이터 파이프라인을 제공합니다.

## 🏗️ 시스템 아키텍처

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│ KIS API     │    │ Kafka        │    │ Spark       │    │ PostgreSQL   │
│ yfinance    │───▶│ Streaming    │───▶│ Consumer    │───▶│ Database     │
│ Data Sources│    │              │    │             │    │              │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
                            │                                      │
                   ┌──────────────┐                      ┌──────────────┐
                   │ Redis        │                      │ Streamlit    │
                   │ Cache        │◀─────────────────────│ Dashboard    │
                   └──────────────┘                      └──────────────┘
```

## 🔧 핵심 컴포넌트

### 1. **데이터 수집 (Producer)**
- **KIS API**: 한국투자증권 해외주식 API (yfinance 폴백 지원)
- **yfinance**: 실시간 주식 데이터 수집
- **파일**: `kafka/multi_source_producer.py`

### 2. **스트리밍 처리 (Consumer)**
- **Spark Structured Streaming**: 실시간 데이터 처리
- **기술적 지표 계산**: RSI, MACD, Bollinger Bands
- **Redis 저장**: 실시간 데이터 및 기술적 지표 캐싱
- **파일**: `kafka/realtime_redis_consumer.py`

### 3. **데이터베이스**
- **PostgreSQL**: 메인 데이터베이스
  - `daily_watchlist`: 관심종목 관리
  - `stock_data`: 히스토리컬 주식 데이터
  - `technical_indicators`: 계산된 기술적 지표
- **Redis**: 실시간 캐싱
  - `realtime:{symbol}`: 실시간 주식 데이터
  - `indicators:{symbol}`: 기술적 지표
  - `watchlist:{symbol}`: 관심종목 정보

### 4. **모니터링 & 대시보드**
- **Streamlit**: 실시간 모니터링 대시보드
- **파일**: `streamlit/pages/05_실시간_Redis_모니터링.py`

## 📊 데이터 플로우

1. **관심종목 관리**: PostgreSQL `daily_watchlist` 테이블에서 추적할 종목 관리
2. **실시간 수집**: Producer가 watchlist 종목들의 실시간 데이터 수집
3. **Kafka 스트리밍**: 수집된 데이터를 Kafka 토픽으로 전송
4. **실시간 처리**: Consumer가 Kafka에서 데이터를 읽어 기술적 지표 계산
5. **캐싱**: 처리된 데이터를 Redis에 실시간 저장
6. **모니터링**: Streamlit 대시보드에서 실시간 모니터링

## 🔑 주요 변경사항 (v3 업그레이드)

### ✅ **DuckDB → PostgreSQL 전환**
- 더 나은 ACID 속성 및 동시성 지원
- 표준 SQL 호환성 향상
- 더 안정적인 프로덕션 환경

### ✅ **쿼리 최적화**
```sql
-- 이전: 날짜 기반 필터링
SELECT DISTINCT symbol 
FROM daily_watchlist 
WHERE date = (SELECT MAX(date) FROM daily_watchlist)

-- 현재: 단순화된 쿼리
SELECT DISTINCT symbol 
FROM daily_watchlist 
ORDER BY symbol
```

### ✅ **불필요한 코드 제거**
- `get_default_symbols()` 메소드 제거
- 시뮬레이션 모드에서도 실제 데이터 우선 사용
- 테스트 파일들 아카이브로 이동

## 🚀 실행 방법

### 1. PostgreSQL 관심종목 설정
```sql
-- 관심종목 추가
INSERT INTO daily_watchlist (symbol, date, condition_value, market_cap_tier) 
VALUES ('AAPL', CURRENT_DATE, 1, 'large');
```

### 2. Producer 실행
```bash
cd /home/grey1/stock-kafka3
docker-compose up producer -d
```

### 3. Consumer 실행
```bash
docker-compose up consumer -d
```

### 4. Streamlit 모니터링
```bash
docker-compose up streamlit -d
# 브라우저에서 http://localhost:8501 접속
```

## 📁 프로젝트 구조

```
stock-kafka3/
├── kafka/                     # Kafka Producer/Consumer
│   ├── multi_source_producer.py
│   └── realtime_redis_consumer.py
├── streamlit/                 # 모니터링 대시보드
│   └── pages/05_실시간_Redis_모니터링.py
├── common/                    # 공통 모듈
│   ├── database.py           # PostgreSQL 연결
│   ├── redis_manager.py      # Redis 관리
│   └── technical_indicator_calculator_postgres.py
├── config/                    # 설정 파일
├── docs/                      # 문서
├── archive/                   # 아카이브된 파일들
│   ├── tests/                # 테스트 파일들
│   ├── samples/              # 샘플 파일들
│   └── docs_old/             # 구식 문서들
└── docker-compose.yml        # Docker 컨테이너 설정
```

## 🔍 모니터링 대시보드 기능

- **실시간 데이터 현황**: Redis에 저장된 최신 주식 데이터 확인
- **기술적 지표 모니터링**: RSI, MACD 등 실시간 계산 결과
- **신호 감지**: 매매 신호 발생 시 실시간 알림
- **성과 추적**: 신호 발생 후 수익률 모니터링

## 🛠️ 개발자 가이드

### 새로운 종목 추가
```sql
INSERT INTO daily_watchlist (symbol, date, condition_value, market_cap_tier, id, created_at) 
VALUES ('MSFT', CURRENT_DATE, 1, 'large', nextval('daily_watchlist_id_seq'), CURRENT_TIMESTAMP);
```

### Redis 데이터 확인
```bash
docker exec -it redis redis-cli
> KEYS *
> HGETALL realtime:AAPL
> HGETALL indicators:AAPL
```

### 로그 모니터링
```bash
# Producer 로그
docker logs producer

# Consumer 로그  
docker logs consumer

# Streamlit 로그
docker logs streamlit
```

## 📈 성능 특징

- **실시간 처리**: 2초 간격 데이터 수집
- **폴백 지원**: KIS API 실패 시 yfinance 자동 전환
- **캐시 최적화**: Redis 30초 캐싱으로 API 부하 감소
- **배치 처리**: 30초마다 배치로 효율적 처리
- **자동 재시작**: 오류 발생 시 자동 복구

---
*Last Updated: 2025-01-07*
*Project: stock-kafka3 (PostgreSQL Branch)*
