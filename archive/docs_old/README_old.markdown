# Stock-Kafka3 프로젝트 문서

이 디렉토리에는 Stock-Kafka3 프로젝트의 아키텍처, 설계 결정, 그리고 구현 세부사항에 대한 문서들이 포함되어 있습니다.

## 📚 문서 목록

### 🗃️ 아키텍처 & 설계

- **[database-concurrency-strategy.md](./database-concurrency-strategy.md)** - DuckDB 동시성 문제 해결을 위한 읽기 전용 복제본(Read Replica) 전략
- **[realtime-signal-detection-system.md](./realtime-signal-detection-system.md)** - Redis + Kafka 기반 실시간 신호 감지 및 성과 추적 시스템
- **[redis-data-management.md](./redis-data-management.md)** - Redis 데이터 관리 및 스마트 증분 업데이트 가이드
- **[airflow-data-pipeline-strategy.md](./airflow-data-pipeline-strategy.md)** - 🆕 Airflow 기반 주식 데이터 파이프라인 전체 전략
- **[airflow-dag-implementation-details.md](./airflow-dag-implementation-details.md)** - 🆕 주요 DAG 코드 분석 및 구현 세부사항

### 🛠️ 운영 가이드

- **[PROJECT_GUIDE.md](./PROJECT_GUIDE.md)** - 프로젝트 전체 설정 및 실행 가이드
- **[streamlit-package-fix-guide.md](./streamlit-package-fix-guide.md)** - Streamlit 패키지 문제 해결 가이드
- **Airflow DAG 관리**: `nasdaq_daily_pipeline`, `redis_watchlist_sync` 등 자동화된 데이터 파이프라인
- **시스템 모니터링**: Streamlit 대시보드, Kafka UI, Airflow UI를 통한 실시간 모니터링
- **Redis 데이터 관리**: 스마트 증분 업데이트를 통한 효율적 데이터 동기화
- **Docker 컨테이너 관리**: docker compose 기반 서비스 오케스트레이션

### 🔧 개발 가이드

- **환경 설정**: Docker 기반 개발 환경 (requirements-*.txt로 의존성 관리)
- **테스트 전략**: 단위 테스트 (`test_*.py`), 통합 테스트, 성능 테스트 파일들 포함
- **코드 구조**: 모듈화된 구조 (`/common`, `/kafka`, `/streamlit` 등)
- **API 통합**: KIS API, yfinance API 클라이언트 구현

## 📋 프로젝트 개요

Stock-Kafka3는 실시간 주식 데이터 수집 및 분석 파이프라인 프로젝트입니다.

### 주요 구성 요소

1. **실시간 데이터 수집** (Kafka Producer)
   - KIS API 및 yfinance를 통한 실시간 주가 데이터 수집
   - 다중 소스 fallback 전략으로 데이터 안정성 확보

2. **배치 데이터 처리** (Airflow)
   - 나스닥 전체 종목 수집
   - 기술적 지표 계산 (Spark 기반)
   - 관심종목 자동 스캔

3. **실시간 신호 감지** (Redis + Kafka)
   - 관심종목 히스토리컬 데이터 Redis 캐싱
   - Kafka 실시간 데이터와 결합하여 기술적 지표 계산
   - RSI, 볼린저밴드, MACD 기반 매매 신호 자동 감지
   - 신호 발생 후 실시간 성과 추적 및 모니터링

4. **데이터 저장소** (DuckDB)
   - 고성능 분석형 데이터베이스
   - 읽기 전용 복제본으로 동시성 문제 해결

5. **인프라** (Docker)
   - 전체 스택 컨테이너화
   - 개발/운영 환경 일관성 보장

### 주요 기술적 특징

- **동시성 안전성**: 읽기 전용 복제본 패턴으로 DB 잠금 충돌 해결
- **장애 복구**: 다중 fallback 전략으로 서비스 연속성 보장
- **확장성**: 마이크로서비스 아키텍처로 개별 컴포넌트 독립적 확장
- **데이터 무결성**: 원자적 연산과 트랜잭션으로 데이터 일관성 보장
- **실시간 신호 감지**: Redis 캐싱과 Kafka 스트리밍을 결합한 하이브리드 분석
- **성과 추적**: 신호 발생 후 실시간 수익/손실 모니터링 및 시각화

## 🤝 기여 가이드

### 문서 작성 원칙

1. **명확성**: 기술적 배경이 다른 사람도 이해할 수 있도록 작성
2. **실용성**: 실제 운영에 도움이 되는 정보 포함
3. **최신성**: 코드 변경 시 관련 문서도 함께 업데이트
4. **예시 포함**: 추상적 설명보다는 구체적 예시와 코드 제공

### 문서 구조

각 문서는 다음 구조를 따릅니다:

```markdown
# 제목

## 📋 목차
- [섹션 1](#섹션-1)
- [섹션 2](#섹션-2)

## 🚨 문제 상황 또는 배경

## 💡 해결책 또는 설계

## 🔧 구현 상세

## 📊 성능 및 효과

## 🔧 트러블슈팅

## 📚 참고 자료
```

---

*프로젝트에 대한 문의사항이 있으시면 이슈를 등록해 주세요.*
