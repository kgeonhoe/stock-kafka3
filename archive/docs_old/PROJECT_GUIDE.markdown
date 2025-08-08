# Stock Kafka Pipeline 프로젝트 가이드

## 📋 프로젝트 개요

**Stock Kafka Pipeline**은 실시간 주식 데이터를 수집, 처리, 분석하는 통합 데이터 파이프라인입니다. Kafka를 중심으로 한 이벤트 스트리밍 아키텍처를 통해 KIS API와 yfinance로부터 주식 데이터를 실시간으로 수집하고, Spark로 처리하여 다양한 저장소에 저장합니다.

### 🎯 주요 기능
- **실시간 데이터 수집**: KIS API, yfinance를 통한 주식 데이터 수집
- **스트리밍 처리**: Apache Kafka + Spark Structured Streaming
- **실시간 신호 감지**: Redis 히스토리컬 + Kafka 실시간 데이터 결합으로 기술적 지표 계산 및 매매 신호 자동 감지
- **성과 추적**: 신호 발생 후 실시간 수익/손실 모니터링
- **워크플로우 관리**: Apache Airflow를 통한 배치 작업 스케줄링
- **모니터링**: Streamlit 대시보드, Kafka UI를 통한 실시간 모니터링
- **데이터 저장**: DuckDB, Redis, PostgreSQL 다중 저장소 지원

### 🏗️ 시스템 아키텍처
```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────────┐
│ KIS API     │    │ Kafka        │    │ Spark       │    │ Storage      │
│ yfinance    │───▶│ (Producer/   │───▶│ Streaming   │───▶│ DuckDB/Redis │
│ Data Sources│    │ Consumer)    │    │ Processing  │    │ PostgreSQL   │
└─────────────┘    └──────────────┘    └─────────────┘    └──────────────┘
                            │                                      │
                   ┌──────────────┐                      ┌──────────────┐
                   │ Kafka UI     │                      │ Streamlit    │
                   │ Monitoring   │                      │ Dashboard    │
                   └──────────────┘                      └──────────────┘
                            │                                      │
                   ┌──────────────┐                      ┌──────────────┐
                   │ Airflow      │                      │ Redis Signal │
                   │ Workflow     │                      │ Detection    │
                   └──────────────┘                      └──────────────┘
```

### 🚨 실시간 신호 감지 플로우
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

---

## 📁 프로젝트 구조

```
stock-kafka3/
├── 📁 airflow/                    # Apache Airflow 워크플로우
├── 📁 common/                     # 공통 유틸리티 및 라이브러리
├── 📁 config/                     # 설정 파일
├── 📁 data/                       # 데이터 저장 디렉터리
├── 📁 docker/                     # Docker 설정 및 Dockerfile
├── 📁 docs/                       # 프로젝트 문서
├── 📁 kafka/                      # Kafka Producer/Consumer
├── 📁 scripts/                    # 유틸리티 스크립트
├── 📁 streamlit/                  # Streamlit 대시보드
├── 📄 requirements-*.txt          # Python 의존성 파일
└── 📄 test_*.py                   # 테스트 파일
```

---

## 📂 상세 폴더 구조 및 설명

### 🔧 `/airflow` - Apache Airflow 워크플로우
```
airflow/
├── dags/                          # DAG 정의 파일
│   ├── nasdaq_daily_pipeline.py   # NASDAQ 주식 데이터 파이프라인 DAG
│   └── daily_watchlist_dag.py     # 일일 관심종목 업데이트 DAG
└── plugins/                       # 커스텀 플러그인
    ├── collect_nasdaq_symbols_api.py      # NASDAQ 심볼 수집 플러그인
    ├── collect_stock_data_yfinance.py     # yfinance 주식 데이터 수집
    ├── technical_indicators.py            # 기술적 지표 계산
    └── technical_indicators_pandas.py     # pandas 기반 기술적 지표
```

**주요 파일 설명:**
- `nasdaq_daily_pipeline.py`: NASDAQ 심볼 수집 및 주식 데이터 수집의 전체 워크플로우
- `daily_watchlist_dag.py`: 일일 관심종목 리스트 업데이트 작업
- `collect_stock_data_yfinance.py`: 증분 업데이트 방식의 주식 데이터 수집 (18시간→1시간으로 단축)
- `technical_indicators.py`: RSI, MACD, 볼린저 밴드 등 기술적 지표 계산

### 🛠️ `/common` - 공통 유틸리티 라이브러리
```
common/
├── __init__.py                    # 패키지 초기화
├── database.py                    # DuckDB 연결 및 관리 (증분 업데이트 지원)
├── kis_api_client.py              # KIS API 클라이언트
├── redis_client.py                # Redis 연결 관리
├── redis_manager.py               # Redis 매니저 (고급 기능)
├── slack_notifier.py              # Slack 알림 유틸리티
├── technical_indicator_calculator.py  # 기술적 지표 계산 엔진
├── technical_scanner.py           # 기술적 분석 스캐너
└── db_sharding.py                 # 데이터베이스 샤딩 관리
```

**주요 파일 설명:**
- `database.py`: DuckDB 연결, 증분 업데이트, 배치 UPSERT 기능 (get_latest_date 포함)
- `kis_api_client.py`: 한국투자증권 API 인증, 주식 데이터 조회 기능
- `redis_manager.py`: Redis 캐싱, 실시간 데이터 저장, 세션 관리
- `slack_notifier.py`: 시스템 알림 및 에러 알림을 Slack으로 전송
- `technical_indicator_calculator.py`: RSI, MACD, 볼린저 밴드 등 기술적 지표 계산
- `technical_scanner.py`: 기술적 패턴 스캔 및 신호 감지
- `db_sharding.py`: 대용량 데이터 처리를 위한 데이터베이스 샤딩

### ⚙️ `/config` - 설정 파일
```
config/
├── __init__.py                    # 패키지 초기화
├── kafka_config.py                # Kafka 설정 (토픽명, 서버 등)
├── database_config.py             # 데이터베이스 연결 설정
├── api_config.py                  # API 엔드포인트 및 키 설정
└── technical_indicators.json      # 기술적 지표 설정 파일
```

**주요 파일 설명:**
- `kafka_config.py`: Kafka 브로커 주소, 토픽명, 파티션 설정
- `database_config.py`: DuckDB, PostgreSQL, Redis 연결 정보
- `api_config.py`: KIS API, yfinance API 설정 및 인증 정보
- `technical_indicators.json`: RSI, MACD 등 기술적 지표 매개변수 설정

### 💾 `/data` - 데이터 저장소
```
data/
├── duckdb/                        # DuckDB 데이터베이스 파일
│   ├── stock_data.db              # 주식 데이터 메인 DB
│   └── analytics.db               # 분석 결과 DB
├── logs/                          # 애플리케이션 로그
├── backups/                       # 데이터 백업 파일
├── checkpoints/                   # Spark 체크포인트
└── cache/                         # 임시 캐시 파일
```

### 🐳 `/docker` - Docker 설정
```
docker/
├── docker-compose.yml             # 전체 서비스 오케스트레이션
├── Dockerfile.airflow             # Airflow 컨테이너 이미지
├── Dockerfile.kafka               # Kafka Producer/Consumer 이미지
├── Dockerfile.spark               # Spark 워커 이미지
├── Dockerfile.streamlit           # Streamlit 대시보드 이미지
└── .env                           # 환경 변수 파일
```

**주요 파일 설명:**
- `docker-compose.yml`: Kafka, Spark, Airflow, PostgreSQL, Redis 등 모든 서비스 정의
- `Dockerfile.airflow`: Airflow 웹서버, 스케줄러, 워커 컨테이너 설정
- `Dockerfile.kafka`: Kafka 클라이언트 및 Python 애플리케이션 환경
- `Dockerfile.spark`: Spark 마스터/워커 노드와 PySpark 환경 설정

### 📚 `/docs` - 프로젝트 문서
```
docs/
├── README.md                      # 프로젝트 메인 문서
├── kafka-topic-consumer-guide.md  # Kafka 사용 가이드
├── API-integration-guide.md       # API 연동 가이드
├── deployment-guide.md            # 배포 가이드
├── troubleshooting.md             # 문제 해결 가이드
└── architecture.md                # 시스템 아키텍처 문서
```

### 🔄 `/kafka` - Kafka Producer/Consumer
```
kafka/
├── __init__.py                    # 패키지 초기화
├── multi_source_producer.py       # 멀티소스 데이터 프로듀서
├── test_multi_source_producer.py  # 테스트용 프로듀서 (시뮬레이션)
├── realtime_consumer.py           # 실시간 데이터 컨슈머
├── realtime_producer.py           # 실시간 데이터 프로듀서
├── realtime_redis_consumer.py     # Redis 기반 실시간 컨슈머
├── realtime_signal_detector.py    # 실시간 신호 감지기 (Redis + Kafka)
├── signal_test_producer.py        # 신호 테스트 프로듀서
├── kafka_consumer_group_register.py # Consumer Group 등록 관리
└── test_consumer_group.py         # Consumer Group 테스트
```

**주요 파일 설명:**
- `multi_source_producer.py`: KIS API + yfinance 데이터를 Kafka 토픽으로 전송
- `realtime_consumer.py`: kafka-python을 이용한 실시간 데이터 처리
- `realtime_redis_consumer.py`: Redis와 연동한 실시간 데이터 캐싱
- `realtime_signal_detector.py`: **신규** Redis 히스토리컬 + Kafka 실시간 데이터 결합으로 기술적 지표 계산 및 신호 감지
- `signal_test_producer.py`: **신규** 신호 발생 시뮬레이션을 위한 테스트 데이터 생성기
- `test_multi_source_producer.py`: 개발/테스트용 시뮬레이션 데이터 생성기
- `kafka_consumer_group_register.py`: Consumer Group 등록 및 관리

### 📊 `/streamlit` - 대시보드
```
streamlit/
├── streamlit_app.py               # 메인 Streamlit 애플리케이션
├── pages/                         # 멀티페이지 구성
│   ├── 01_실시간_모니터링.py        # 실시간 데이터 모니터링
│   ├── 02_데이터_분석.py           # 데이터 분석 대시보드
│   ├── 03_시스템_상태.py           # 시스템 헬스 체크
│   ├── 04_설정.py                 # 애플리케이션 설정
│   └── 05_실시간_Redis_모니터링.py  # Redis 신호 감지 및 성과 추적 대시보드
├── components/                    # 재사용 가능한 컴포넌트
│   ├── charts.py                  # 차트 컴포넌트
│   ├── metrics.py                 # 메트릭 표시 컴포넌트
│   └── tables.py                  # 테이블 컴포넌트
├── common/                        # Streamlit 공통 유틸리티
├── config/                        # Streamlit 설정
└── static/                        # 정적 파일 (CSS, JS, 이미지)
```

**주요 파일 설명:**
- `05_실시간_Redis_모니터링.py`: **신규** 실시간 신호 감지, 성과 추적, Redis 상태 모니터링 통합 대시보드

### 🔧 `/scripts` - 유틸리티 스크립트
```
scripts/
├── setup.sh                      # 초기 환경 설정 스크립트
├── start_pipeline.sh              # 파이프라인 시작 스크립트
├── stop_pipeline.sh               # 파이프라인 중지 스크립트
├── backup_data.sh                 # 데이터 백업 스크립트
├── check_health.sh                # 시스템 헬스 체크 스크립트
├── clean_logs.sh                  # 로그 정리 스크립트
└── load_watchlist_to_redis.py     # 관심종목 데이터 Redis 로딩 스크립트
```

**주요 파일 설명:**
- `load_watchlist_to_redis.py`: **신규** DuckDB에서 관심종목 히스토리컬 데이터를 Redis에 로딩하는 스크립트

---

## 📋 주요 설정 파일

### 📄 `requirements-*.txt` - Python 의존성
- `requirements-kafka.txt`: Kafka Producer/Consumer 의존성
- `requirements-spark.txt`: Spark 및 데이터 처리 의존성  
- `requirements-airflow.txt`: Airflow 워크플로우 의존성
- `requirements-streamlit.txt`: Streamlit 대시보드 의존성

### 🧪 테스트 파일
프로젝트 루트에 있는 주요 테스트 파일들:
- `test_yfinance_*.py`: yfinance API 관련 다양한 테스트
- `test_performance*.py`: 성능 테스트 및 벤치마크
- `test_kis_api_analysis.py`: KIS API 분석 테스트
- `test_enhanced_collector.py`: 향상된 데이터 수집기 테스트
- `analyze_*.py`: 데이터 분석 및 백필 관련 스크립트
- `backfill_historical_data.py`: 4년 히스토리컬 데이터 백필 스크립트

---

## 🚀 시작하기

### 1. 환경 설정
```bash
# 저장소 클론
git clone <repository-url>
cd stock-kafka3

# 환경 변수 설정
cp docker/.env.example docker/.env
# .env 파일에서 API 키 등 설정

# Docker 컨테이너 시작 (docker 디렉토리에서 실행)
cd docker
docker compose up -d
```

### 2. 서비스 접속
- **Kafka UI**: http://localhost:8080
- **Airflow**: http://localhost:8082 (admin/admin)
- **Streamlit**: http://localhost:8501
- **Spark UI**: http://localhost:4040 (Spark 작업 실행 중일 때)
- **PostgreSQL**: localhost:5432 (airflow/airflow)

### 3. 데이터 파이프라인 시작
```bash
# 디렉토리 이동 (중요!)
cd /home/grey1/stock-kafka3/docker

# 1. 관심종목 데이터 Redis 로딩
docker compose exec kafka-consumer python /app/scripts/load_watchlist_to_redis.py

# 2. 실시간 신호 감지기 시작
docker compose exec kafka-consumer python /app/kafka/realtime_signal_detector.py

# 3. 테스트 프로듀서 시작 (별도 터미널)
docker compose exec kafka-producer python /app/kafka/signal_test_producer.py --interval 5 --signal-prob 0.3

# 4. Airflow에서 DAG 실행 상태 확인
# http://localhost:8082에서 nasdaq_daily_pipeline DAG 확인

# 5. 서비스 로그 모니터링
docker compose logs kafka-producer -f    # 프로듀서 로그
docker compose logs kafka-consumer -f    # 컨슈머 및 신호 감지기 로그
docker compose logs airflow-scheduler -f # Airflow 스케줄러 로그
docker compose logs streamlit -f         # Streamlit 대시보드 로그
```

---

## 🔧 주요 기능

### 📊 실시간 데이터 수집
- **KIS API**: 한국 주식 실시간 데이터 (100일 제한)
- **yfinance**: 해외 주식 데이터 (5년 히스토리컬 + 증분 업데이트)
- **증분 업데이트**: 18시간 → 1시간으로 성능 대폭 개선
- **자동 재시도**: API 오류 시 자동 재시도 메커니즘
- **레이트 리미팅**: API 호출 제한 준수 (1-2초 지연, 배치 처리)

### � 실시간 신호 감지 및 성과 추적
- **하이브리드 데이터 처리**: Redis 히스토리컬 + Kafka 실시간 데이터 결합
- **기술적 지표 실시간 계산**: RSI, 볼린저밴드, MACD 등
- **자동 신호 감지**: 과매수/과매도, 밴드 터치, 모멘텀 변화 등
- **실시간 성과 추적**: 신호 발생 후 수익/손실 모니터링
- **시각화 대시보드**: 신호별 성과, 통계, 차트 실시간 업데이트

### �🔄 스트리밍 처리
- **Kafka**: 높은 처리량의 이벤트 스트리밍
- **실시간 Consumer**: kafka-python 기반 실시간 데이터 처리
- **Redis 캐싱**: 실시간 데이터 캐싱 및 세션 관리
- **배치 처리**: 10개씩 배치 처리로 메모리 최적화

### 💾 데이터 저장
- **DuckDB**: 분석용 컬럼형 데이터베이스 (증분 업데이트 지원)
- **Redis**: 실시간 캐싱 및 세션 저장
- **PostgreSQL**: Airflow 메타데이터 저장
- **배치 UPSERT**: 중복 데이터 자동 처리

### 📈 모니터링 및 알림
- **Streamlit**: 실시간 대시보드 (http://localhost:8501)
- **Kafka UI**: Kafka 클러스터 모니터링 (http://localhost:8080)
- **Airflow UI**: 워크플로우 모니터링 (http://localhost:8082)
- **Slack**: 시스템 알림 및 에러 알림
- **기술적 지표**: RSI, MACD, 볼린저 밴드 등 실시간 계산

---

## 🛠️ 개발 가이드

### 코드 구조
- **모듈화**: 기능별로 분리된 모듈 구조
- **설정 관리**: 환경별 설정 분리
- **에러 처리**: 포괄적인 예외 처리 및 로깅
- **테스트**: 단위 테스트 및 통합 테스트

### 추가 기능 개발
1. `/common`에 공통 유틸리티 추가
2. `/kafka`에 새로운 Producer/Consumer 추가
3. `/streamlit/pages`에 새로운 대시보드 페이지 추가
4. `/airflow/dags`에 새로운 워크플로우 추가

### 배포
- **Docker Compose**: 로컬 개발 환경
- **Kubernetes**: 프로덕션 환경 (별도 설정 필요)
- **모니터링**: Prometheus + Grafana (선택사항)

---

## 📞 지원 및 문제 해결

### 로그 확인
```bash
# 디렉토리 이동 (중요!)
cd /home/grey1/stock-kafka3/docker

# 전체 서비스 로그
docker compose logs -f

# 특정 서비스 로그
docker compose logs kafka-producer -f
docker compose logs kafka-consumer -f
docker compose logs streamlit -f
docker compose logs airflow-scheduler -f

# 서비스 상태 확인
docker compose ps

# 특정 서비스 재시작
docker compose restart streamlit
docker compose restart kafka-producer
```

### 일반적인 문제
1. **API 인증 오류**: `.env` 파일의 API 키 확인
2. **Kafka 연결 오류**: 컨테이너 간 네트워크 상태 확인
3. **메모리 부족**: Docker 리소스 할당량 증가

### 추가 문서
- [Kafka 토픽 및 Consumer Group 가이드](kafka-topic-consumer-guide.markdown)
- [실시간 신호 감지 및 성과 추적 시스템](realtime-signal-detection-system.md)
- [로드 테스트 가이드](../LOAD_TEST_GUIDE.md)
- [로드 테스트 요약](../LOAD_TEST_SUMMARY.md)
- [프로젝트 아키텍처](../PROJECT_ARCHITECTURE.md)
- [정리 보고서](../CLEANUP_REPORT.md)
- [문서화 가이드](../DOCUMENTATION_GUIDE.md)

---

**📝 참고사항:**
- 이 프로젝트는 실시간 주식 데이터 처리를 위한 교육/연구 목적으로 개발되었습니다
- 실제 거래에 사용하기 전에 충분한 테스트와 검증이 필요합니다
- API 사용량 제한을 준수하여 사용하시기 바랍니다
- **성능 개선**: 증분 업데이트로 18시간 → 1시간으로 대폭 단축
- **데이터 백필**: 4년 히스토리컬 데이터 백필 지원
- **DuckDB 최적화**: 배치 UPSERT와 증분 업데이트로 성능 최적화
