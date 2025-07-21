# Stock Kafka Pipeline 프로젝트 가이드

## 📋 프로젝트 개요

**Stock Kafka Pipeline**은 실시간 주식 데이터를 수집, 처리, 분석하는 통합 데이터 파이프라인입니다. Kafka를 중심으로 한 이벤트 스트리밍 아키텍처를 통해 KIS API와 yfinance로부터 주식 데이터를 실시간으로 수집하고, Spark로 처리하여 다양한 저장소에 저장합니다.

### 🎯 주요 기능
- **실시간 데이터 수집**: KIS API, yfinance를 통한 주식 데이터 수집
- **스트리밍 처리**: Apache Kafka + Spark Structured Streaming
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
                            │
                   ┌──────────────┐
                   │ Airflow      │
                   │ Workflow     │
                   └──────────────┘
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
│   ├── stock_data_pipeline.py     # 주식 데이터 파이프라인 DAG
│   ├── data_quality_check.py      # 데이터 품질 검사 DAG
│   └── backup_pipeline.py         # 백업 작업 DAG
├── plugins/                       # 커스텀 플러그인
│   ├── operators/                 # 커스텀 오퍼레이터
│   └── hooks/                     # 커스텀 훅
└── logs/                          # Airflow 로그 파일
```

**주요 파일 설명:**
- `stock_data_pipeline.py`: 주식 데이터 수집, 처리, 저장의 전체 워크플로우를 정의
- `data_quality_check.py`: 데이터 무결성 및 품질 검사 작업
- `backup_pipeline.py`: 정기적인 데이터 백업 및 아카이빙 작업

### 🛠️ `/common` - 공통 유틸리티 라이브러리
```
common/
├── __init__.py                    # 패키지 초기화
├── database.py                    # DuckDB 연결 및 관리
├── kis_api_client.py              # KIS API 클라이언트
├── yfinance_client.py             # yfinance 데이터 클라이언트
├── slack_notifier.py              # Slack 알림 유틸리티
├── utils.py                       # 공통 유틸리티 함수
├── redis_client.py                # Redis 연결 관리
└── data_validator.py              # 데이터 검증 유틸리티
```

**주요 파일 설명:**
- `database.py`: DuckDB 연결, 테이블 생성/관리, 쿼리 실행 기능
- `kis_api_client.py`: 한국투자증권 API 인증, 주식 데이터 조회 기능
- `yfinance_client.py`: Yahoo Finance API를 통한 해외 주식 데이터 수집
- `slack_notifier.py`: 시스템 알림 및 에러 알림을 Slack으로 전송
- `redis_client.py`: Redis 캐싱 및 실시간 데이터 저장 관리

### ⚙️ `/config` - 설정 파일
```
config/
├── __init__.py                    # 패키지 초기화
├── kafka_config.py                # Kafka 설정 (토픽명, 서버 등)
├── database_config.py             # 데이터베이스 연결 설정
├── api_config.py                  # API 엔드포인트 및 키 설정
├── logging_config.py              # 로깅 설정
└── settings.py                    # 전역 설정값
```

**주요 파일 설명:**
- `kafka_config.py`: Kafka 브로커 주소, 토픽명, 파티션 설정
- `database_config.py`: DuckDB, PostgreSQL, Redis 연결 정보
- `api_config.py`: KIS API, yfinance API 설정 및 인증 정보
- `settings.py`: 환경별 설정값, 디버그 모드, 타임아웃 등

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
├── realtime_consumer.py           # Spark 기반 실시간 컨슈머
├── simple_consumer.py             # 단순 Kafka 컨슈머
├── realtime_producer.py           # 실시간 데이터 프로듀서
└── batch_consumer.py              # 배치 처리용 컨슈머
```

**주요 파일 설명:**
- `multi_source_producer.py`: KIS API + yfinance 데이터를 Kafka 토픽으로 전송
- `realtime_consumer.py`: Spark Structured Streaming을 이용한 실시간 데이터 처리
- `test_multi_source_producer.py`: 개발/테스트용 시뮬레이션 데이터 생성기
- `simple_consumer.py`: kafka-python을 이용한 기본 컨슈머 (백업용)

### 📊 `/streamlit` - 대시보드
```
streamlit/
├── app.py                         # 메인 Streamlit 애플리케이션
├── pages/                         # 멀티페이지 구성
│   ├── 01_실시간_모니터링.py        # 실시간 데이터 모니터링
│   ├── 02_데이터_분석.py           # 데이터 분석 대시보드
│   ├── 03_시스템_상태.py           # 시스템 헬스 체크
│   └── 04_설정.py                 # 애플리케이션 설정
├── components/                    # 재사용 가능한 컴포넌트
│   ├── charts.py                  # 차트 컴포넌트
│   ├── metrics.py                 # 메트릭 표시 컴포넌트
│   └── tables.py                  # 테이블 컴포넌트
└── static/                        # 정적 파일 (CSS, JS, 이미지)
```

### 🔧 `/scripts` - 유틸리티 스크립트
```
scripts/
├── setup.sh                      # 초기 환경 설정 스크립트
├── start_pipeline.sh              # 파이프라인 시작 스크립트
├── stop_pipeline.sh               # 파이프라인 중지 스크립트
├── backup_data.sh                 # 데이터 백업 스크립트
├── check_health.sh                # 시스템 헬스 체크 스크립트
└── clean_logs.sh                  # 로그 정리 스크립트
```

---

## 📋 주요 설정 파일

### 📄 `requirements-*.txt` - Python 의존성
- `requirements-kafka.txt`: Kafka Producer/Consumer 의존성
- `requirements-spark.txt`: Spark 및 데이터 처리 의존성
- `requirements-airflow.txt`: Airflow 워크플로우 의존성
- `requirements-streamlit.txt`: Streamlit 대시보드 의존성

### 🧪 테스트 파일
- `test_yfinance_delay.py`: yfinance API 지연 테스트
- `test_yfinance_local.py`: 로컬 환경 yfinance 테스트
- `test_yfinance_simple.py`: 기본 yfinance 기능 테스트

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

# Docker 컨테이너 시작
cd docker
docker compose up -d
```

### 2. 서비스 접속
- **Kafka UI**: http://localhost:8080
- **Airflow**: http://localhost:8081 (admin/admin)
- **Spark UI**: http://localhost:8082
- **Streamlit**: http://localhost:8501

### 3. 데이터 파이프라인 시작
```bash
# 프로듀서 시작 (데이터 수집)
docker compose logs kafka-producer -f

# 컨슈머 시작 (데이터 처리)
docker compose logs kafka-consumer -f
```

---

## 🔧 주요 기능

### 📊 실시간 데이터 수집
- **KIS API**: 한국 주식 실시간 데이터
- **yfinance**: 해외 주식 데이터
- **자동 재시도**: API 오류 시 자동 재시도 메커니즘
- **레이트 리미팅**: API 호출 제한 준수

### 🔄 스트리밍 처리
- **Kafka**: 높은 처리량의 이벤트 스트리밍
- **Spark**: 분산 실시간 데이터 처리
- **체크포인팅**: 장애 복구를 위한 상태 저장

### 💾 데이터 저장
- **DuckDB**: 분석용 컬럼형 데이터베이스
- **Redis**: 실시간 캐싱 및 세션 저장
- **PostgreSQL**: Airflow 메타데이터 저장

### 📈 모니터링 및 알림
- **Streamlit**: 실시간 대시보드
- **Kafka UI**: Kafka 클러스터 모니터링
- **Slack**: 시스템 알림 및 에러 알림

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
# 전체 서비스 로그
docker compose logs -f

# 특정 서비스 로그
docker compose logs kafka-producer -f
docker compose logs kafka-consumer -f
```

### 일반적인 문제
1. **API 인증 오류**: `.env` 파일의 API 키 확인
2. **Kafka 연결 오류**: 컨테이너 간 네트워크 상태 확인
3. **메모리 부족**: Docker 리소스 할당량 증가

### 추가 문서
- [Kafka 토픽 및 Consumer Group 가이드](kafka-topic-consumer-guide.md)
- [API 연동 가이드](API-integration-guide.md)
- [배포 가이드](deployment-guide.md)
- [문제 해결 가이드](troubleshooting.md)

---

**📝 참고사항:**
- 이 프로젝트는 실시간 주식 데이터 처리를 위한 교육/연구 목적으로 개발되었습니다
- 실제 거래에 사용하기 전에 충분한 테스트와 검증이 필요합니다
- API 사용량 제한을 준수하여 사용하시기 바랍니다
