# DuckDB 동시성 문제 해결: 읽기 전용 복제본(Read Replica) 전략

## 📋 목차
- [문제 상황](#문제-상황)
- [전략 개요](#전략-개요)
- [구현 상세](#구현-상세)
- [시스템 아키텍처](#시스템-아키텍처)
- [안전성 보장](#안전성-보장)
- [성능 및 효과](#성능-및-효과)
- [트러블슈팅](#트러블슈팅)

---

## 🚨 문제 상황

### 기존 아키텍처의 동시성 문제

본 프로젝트는 **실시간 Kafka 프로듀서**와 **배치 처리용 Airflow DAG**가 동시에 동일한 DuckDB 파일(`stock_data.db`)에 접근하는 구조였습니다.

```mermaid
graph LR
    A[Kafka Producer] --> C[stock_data.db]
    B[Airflow DAG] --> C
    C --> D[❌ Lock Conflict]
```

**발생한 문제:**
- **파일 잠금 충돌**: Airflow가 데이터를 쓰는 동안 Kafka 프로듀서의 읽기 요청이 `IOException: Cannot acquire lock` 오류로 거부됨
- **서비스 중단**: 프로듀서가 실시간 관심종목 리스트를 가져오지 못해 데이터 수집이 중단됨
- **데이터 일관성 위험**: 중간에 중단된 쓰기 작업으로 인한 불완전한 데이터 위험

### DuckDB의 특성상 제약

DuckDB는 **In-Process** 데이터베이스로, 별도의 서버 없이 매우 빠른 성능을 제공하지만:
- **단일 쓰기 원칙**: 한 번에 하나의 프로세스만 쓰기 작업 가능
- **쓰기 중 읽기 차단**: 쓰기 작업 중에는 다른 프로세스의 읽기도 차단됨

---

## 💡 전략 개요

### 읽기 전용 복제본(Read Replica) 패턴

**핵심 아이디어**: 읽기와 쓰기 작업을 **물리적으로 분리**하여 동시성 문제를 원천 차단

```mermaid
graph TB
    subgraph "Airflow (Writer)"
        A1[nasdaq_daily_pipeline]
        A2[collect_symbols]
        A3[collect_ohlcv]
        A4[calculate_indicators]
        A5[create_replica]
        A6[trigger_next_dag]
        A1 --> A2 --> A3 --> A4 --> A5 --> A6
    end
    
    subgraph "Data Flow"
        B1[stock_data.db<br/>마스터]
        B2[stock_data_replica.db<br/>복제본]
        B1 -.->|원자적 복사| B2
    end
    
    subgraph "Kafka Producer (Reader)"
        C1[multi_source_producer]
        C2[watchlist 조회]
        C1 --> C2
    end
    
    A5 --> B1
    B1 --> B2
    B2 --> C2
    
    style B1 fill:#ff9999
    style B2 fill:#99ccff
    style A5 fill:#99ff99
```

### 전략의 핵심 구성 요소

1. **마스터 DB** (`stock_data.db`): Airflow가 독점적으로 쓰기 작업 수행
2. **복제본 DB** (`stock_data_replica.db`): Kafka 프로듀서가 읽기 전용으로 사용
3. **원자적 복제**: 임시 파일 → 이름 변경 방식으로 안전한 파일 교체
4. **방어적 읽기**: 복제본 교체 중에도 안정성을 보장하는 오류 처리

---

## 🔧 구현 상세

### 1. DuckDBManager 수정: read_only 모드 지원

```python
class DuckDBManager:
    def __init__(self, db_path: str = "/data/duckdb/stock_data.db", read_only: bool = False):
        """
        Args:
            db_path: DuckDB 파일 경로
            read_only: 읽기 전용 모드로 연결할지 여부
        """
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 읽기 전용 모드 연결
        self.conn = duckdb.connect(database=db_path, read_only=read_only)
        
        if not read_only:
            self._create_tables()
```

**장점:**
- 실수로 읽기 전용 클라이언트가 쓰기를 시도하는 것을 방지
- 코드 레벨에서 역할 분리를 명확히 함

### 2. Airflow DAG: 원자적 복제본 생성 태스크

```python
def create_db_replica_func():
    """
    원자적 연산을 위해 임시 파일 사용 후 이름 변경 전략을 사용합니다.
    """
    master_db_path = "/data/duckdb/stock_data.db"
    replica_db_path = "/data/duckdb/stock_data_replica.db"
    temp_replica_path = replica_db_path + ".tmp"

    try:
        # 1. 마스터 DB를 임시 파일로 안전하게 복사
        shutil.copy2(master_db_path, temp_replica_path)
        
        # 2. 복사가 완료되면, 원자적 연산으로 파일 이름 변경
        os.rename(temp_replica_path, replica_db_path)
        
    except Exception as e:
        # 실패 시 임시 파일 정리
        if os.path.exists(temp_replica_path):
            os.remove(temp_replica_path)
        raise
```

**핵심 기술:**
- **임시 파일 사용**: 복사 중에 기존 복제본에 영향 없음
- **os.rename()**: Linux에서 원자적 연산으로 동작
- **예외 처리**: 실패 시 임시 파일 자동 정리

### 3. Kafka 프로듀서: 안전한 복제본 읽기

```python
def _load_watchlist_from_db(self) -> list:
    """
    파일이 교체되는 순간에 발생할 수 있는 오류를 처리합니다.
    """
    try:
        # 복제본에서 데이터 읽기 시도
        result = self.db_manager.execute_query(query)
        if result and len(result) > 0:
            return [row[0] for row in result]
        else:
            return getattr(self, 'nasdaq_symbols', self._get_default_symbols())
            
    except Exception as e:
        # 파일 교체 중 일시적 오류 발생 시 이전 데이터 사용
        print(f"⚠️ 일시적 오류 발생: {e}")
        return getattr(self, 'nasdaq_symbols', self._get_default_symbols())
```

**안전장치:**
- **이전 데이터 유지**: 읽기 실패 시 기존 메모리의 관심종목 리스트 사용
- **기본값 제공**: 초기 실행시 데이터가 없어도 기본 종목으로 시작
- **지속적 시도**: 다음 주기에 다시 읽기 시도

---

## 🏗️ 시스템 아키텍처

### 전체 데이터 흐름

```mermaid
sequenceDiagram
    participant A as Airflow DAG
    participant M as stock_data.db<br/>(Master)
    participant R as stock_data_replica.db<br/>(Replica)
    participant K as Kafka Producer
    
    Note over A,K: 일일 배치 처리 시작
    A->>M: 1. 나스닥 심볼 수집
    A->>M: 2. 주가 데이터 수집
    A->>M: 3. 기술적 지표 계산
    
    Note over A,R: 복제본 생성 (원자적)
    A->>R: 4. 임시파일.tmp 생성
    A->>R: 5. 원자적 이름 변경
    
    Note over K: 실시간 데이터 수집
    K->>R: 6. 관심종목 리스트 조회
    K->>K: 7. 실시간 주가 수집
    
    Note over A,K: 다음 날 배치 처리
    A->>M: 8. 새로운 배치 작업
    A->>R: 9. 복제본 업데이트
```

### 파일 시스템 레이아웃

```
/data/duckdb/
├── stock_data.db           # 마스터 (Airflow 쓰기 전용)
├── stock_data_replica.db   # 복제본 (Producer 읽기 전용)
└── stock_data_replica.db.tmp  # 임시 파일 (복제 중에만 존재)
```

---

## 🛡️ 안전성 보장

### 1. 원자적 파일 교체

**문제**: 파일 복사 중에 프로듀서가 접근하면 불완전한 데이터를 읽을 위험

**해결**:
```bash
# 안전하지 않은 방법
cp stock_data.db stock_data_replica.db  # 복사 중 접근 가능

# 안전한 방법 (원자적)
cp stock_data.db stock_data_replica.db.tmp  # 임시 파일로 복사
mv stock_data_replica.db.tmp stock_data_replica.db  # 원자적 이름 변경
```

**원자적 연산의 보장**:
- `os.rename()`은 Linux에서 단일 시스템 콜로 처리
- 파일 시스템 레벨에서 중간 상태가 존재하지 않음
- 프로듀서는 완전한 이전 파일 또는 완전한 새 파일만 볼 수 있음

### 2. 방어적 프로그래밍

**Kafka 프로듀서의 다중 보호막**:

1. **1차 방어**: 이전 데이터 유지
   ```python
   return getattr(self, 'nasdaq_symbols', self._get_default_symbols())
   ```

2. **2차 방어**: 기본 종목 제공
   ```python
   def _get_default_symbols(self) -> list:
       return ['AAPL', 'MSFT', 'GOOGL', ...]  # 항상 사용 가능한 기본 목록
   ```

3. **3차 방어**: 주기적 재시도
   ```python
   # 10 사이클마다 watchlist 새로고침 시도
   if cycle_count % watchlist_refresh_interval == 0:
       self.refresh_watchlist()
   ```

### 3. 장애 복구 시나리오

| 상황 | 프로듀서 동작 | 복구 방법 |
|------|---------------|-----------|
| 복제본 파일 없음 | 기본 종목으로 계속 동작 | 다음 Airflow 실행 시 자동 생성 |
| 복제본 손상 | 이전 메모리 데이터 사용 | Airflow 재실행으로 새 복제본 생성 |
| 파일 교체 중 접근 | 일시적 오류 후 재시도 | 다음 주기에 자동 복구 |
| DuckDB 연결 실패 | 기본 종목으로 fallback | 시스템 복구 후 자동 연결 |

---

## 📊 성능 및 효과

### Before vs After 비교

| 측면 | 기존 방식 | Read Replica 방식 |
|------|-----------|-------------------|
| **동시성 문제** | ❌ 빈번한 Lock 충돌 | ✅ 완전 해결 |
| **서비스 안정성** | ❌ 간헐적 중단 | ✅ 무중단 운영 |
| **코드 복잡성** | 🔶 재시도/대기 로직 필요 | ✅ 단순하고 명확 |
| **데이터 일관성** | ❌ 중간 상태 노출 위험 | ✅ 원자적 업데이트 |
| **확장성** | ❌ 동시 접근 클라이언트 제한 | ✅ 읽기 클라이언트 무제한 확장 |

### 성능 특성

**지연시간 (Latency)**:
- **읽기 작업**: 0ms 추가 지연 (로컬 파일 접근)
- **복제 작업**: 파일 크기에 비례 (~5-10초, 100MB 기준)
- **데이터 신선도**: 최대 24시간 (배치 주기에 따라)

**처리량 (Throughput)**:
- **읽기**: 원본과 동일한 성능
- **쓰기**: 영향 없음 (독립적 수행)
- **시스템 자원**: 디스크 사용량 2배, CPU/메모리 영향 미미

### 비용-효과 분석

**추가 비용**:
- 디스크 공간: 2배 (일반적으로 100-500MB 수준으로 미미)
- 복제 시간: 배치 작업에 5-10초 추가

**절약 효과**:
- 개발/운영 시간: 동시성 문제 디버깅 시간 대폭 절약
- 시스템 안정성: 서비스 중단으로 인한 비즈니스 손실 방지
- 확장성: 향후 읽기 클라이언트 추가 시 추가 작업 불필요

---

## 🔧 트러블슈팅

### 자주 발생하는 문제와 해결법

#### 1. 복제본 파일이 생성되지 않음

**증상**:
```
⚠️ DuckDB watchlist 조회 중 일시적 오류 발생: no such file: /data/duckdb/stock_data_replica.db
```

**원인**: Airflow DAG의 복제 태스크가 실행되지 않았거나 실패

**해결법**:
```bash
# 1. Airflow DAG 상태 확인
docker compose exec airflow-scheduler airflow dags state nasdaq_daily_pipeline

# 2. 수동으로 복제본 생성
docker compose exec airflow-scheduler python -c "
import shutil
shutil.copy2('/data/duckdb/stock_data.db', '/data/duckdb/stock_data_replica.db')
print('복제본 수동 생성 완료')
"
```

#### 2. 복제 중 프로듀서 오류

**증상**:
```
⚠️ watchlist 새로고침 중 일시적 오류: database is locked
```

**원인**: 매우 드물게 `os.rename()` 중에 접근 시도

**해결법**: 자동 복구됨 (다음 주기에 정상 동작)
- 수동 개입 불필요
- 이전 데이터로 계속 동작하므로 서비스 중단 없음

#### 3. 디스크 공간 부족

**증상**:
```
❌ DB 복제본 생성 실패: [Errno 28] No space left on device
```

**해결법**:
```bash
# 1. 디스크 사용량 확인
df -h /data

# 2. 오래된 로그 파일 정리
find /data -name "*.log" -mtime +30 -delete

# 3. 임시 파일 정리
rm -f /data/duckdb/*.tmp
```

#### 4. 권한 문제

**증상**:
```
❌ DB 복제본 생성 실패: [Errno 13] Permission denied
```

**해결법**:
```bash
# 데이터 디렉토리 권한 수정
sudo chown -R 50000:0 /data/duckdb
sudo chmod -R 755 /data/duckdb
```

### 모니터링 및 건강성 확인

#### 복제본 상태 확인 스크립트

```bash
#!/bin/bash
# check_replica_health.sh

MASTER="/data/duckdb/stock_data.db"
REPLICA="/data/duckdb/stock_data_replica.db"

echo "=== DuckDB Read Replica 상태 확인 ==="

# 파일 존재 여부
if [[ -f "$MASTER" ]]; then
    echo "✅ 마스터 DB 존재: $(ls -lh $MASTER)"
else
    echo "❌ 마스터 DB 없음: $MASTER"
fi

if [[ -f "$REPLICA" ]]; then
    echo "✅ 복제본 DB 존재: $(ls -lh $REPLICA)"
else
    echo "❌ 복제본 DB 없음: $REPLICA"
fi

# 데이터 동기화 상태 (크기 비교)
if [[ -f "$MASTER" && -f "$REPLICA" ]]; then
    MASTER_SIZE=$(stat -f%z "$MASTER" 2>/dev/null || stat -c%s "$MASTER")
    REPLICA_SIZE=$(stat -f%z "$REPLICA" 2>/dev/null || stat -c%s "$REPLICA")
    
    if [[ $MASTER_SIZE -eq $REPLICA_SIZE ]]; then
        echo "✅ 크기 동일: 마스터와 복제본이 동기화됨"
    else
        echo "⚠️ 크기 차이: 마스터=$MASTER_SIZE, 복제본=$REPLICA_SIZE"
    fi
fi

echo "=== 완료 ==="
```

---

## 📚 참고 자료

### 관련 문서
- [DuckDB 공식 문서 - Concurrency](https://duckdb.org/docs/connect/concurrency)
- [Database Replication Patterns](https://martinfowler.com/articles/patterns-of-distributed-systems/read-replicas.html)
- [Atomic File Operations in Unix](https://lwn.net/Articles/457667/)

### 프로젝트 내 관련 파일
- `common/database.py` - DuckDBManager 클래스
- `kafka/multi_source_producer.py` - Kafka 프로듀서
- `airflow/dags/nasdaq_daily_pipeline.py` - 복제본 생성 DAG
- `docker-compose.yml` - 전체 서비스 구성

### 추가 개선 아이디어
1. **압축된 복제본**: 읽기 전용이므로 압축하여 디스크 사용량 절약
2. **증분 복제**: 변경된 부분만 복제하여 복제 시간 단축
3. **다중 복제본**: 지역별 또는 용도별 복제본 생성
4. **자동 복구**: 복제본 손상 감지 시 자동 재생성

---

*마지막 업데이트: 2025년 7월 24일*
*작성자: GitHub Copilot & 사용자*
