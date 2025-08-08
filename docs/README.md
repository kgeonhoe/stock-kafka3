# 📈 Stock Kafka3 Documentation

현재 PostgreSQL 기반 실시간 주식 데이터 파이프라인 시스템의 문서 모음입니다.

## 📋 문서 목록

### 🎯 **메인 가이드**
- **[CURRENT_PROJECT_OVERVIEW.md](CURRENT_PROJECT_OVERVIEW.md)** - 현재 시스템 전체 개요 및 아키텍처

### 🔧 **기술별 가이드**
- **[kafka-topic-consumer-guide.markdown](kafka-topic-consumer-guide.markdown)** - Kafka 토픽 및 컨슈머 사용법
- **[redis-data-management.md](redis-data-management.md)** - Redis 데이터 관리 및 모니터링
- **[streamlit-package-fix-guide.md](streamlit-package-fix-guide.md)** - Streamlit 패키지 문제 해결

### 📊 **Airflow 관련**
- **[airflow-dag-implementation-details.md](airflow-dag-implementation-details.md)** - DAG 구현 세부사항
- **[airflow_nasdaq_improvement_report.md](airflow_nasdaq_improvement_report.md)** - Airflow 나스닥 수집 개선 보고서

## 🏗️ 현재 시스템 아키텍처

```
PostgreSQL (메인 DB)
    ↑
Producer → Kafka → Consumer → Redis (캐시)
    ↓                             ↓
yfinance/KIS API            Streamlit Dashboard
```

## 🔄 버전 히스토리

### v3.0 (Current - PostgreSQL Branch)
- ✅ DuckDB → PostgreSQL 전환
- ✅ 쿼리 최적화 및 단순화
- ✅ 불필요한 코드 제거
- ✅ 프로젝트 구조 정리

### v2.0 (Archived)
- DuckDB 기반 시스템
- 복잡한 날짜 기반 필터링
- 다양한 실험적 기능들

## 📁 아카이브된 문서들

구식 DuckDB 기반 시스템의 문서들은 `../archive/docs_old/` 폴더에 보관되어 있습니다:

- `PROJECT_GUIDE.markdown` - 구 시스템 프로젝트 가이드
- `airflow-data-pipeline-strategy.md` - DuckDB 기반 Airflow 전략
- `realtime-signal-detection-system.md` - 구 실시간 신호 감지 시스템
- `DYNAMIC_INDICATORS_GUIDE.md` - 구 동적 지표 가이드
- `database-concurrency-strategy.markdown` - 구 DB 동시성 전략
- `LOAD_TEST_GUIDE.markdown` - 부하테스트 가이드
- `WORKER_OPTIMIZATION_GUIDE.md` - 워커 최적화 가이드

## 🚀 빠른 시작

1. **시스템 개요 확인**: [CURRENT_PROJECT_OVERVIEW.md](CURRENT_PROJECT_OVERVIEW.md) 읽기
2. **Kafka 설정**: [kafka-topic-consumer-guide.markdown](kafka-topic-consumer-guide.markdown) 참조
3. **Redis 모니터링**: [redis-data-management.md](redis-data-management.md) 참조
4. **대시보드 접속**: Streamlit으로 실시간 모니터링

---
*Documentation for Stock Kafka3 - PostgreSQL Branch*
*Last Updated: 2025-01-07*
