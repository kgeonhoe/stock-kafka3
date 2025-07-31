# 🔧 주요 DAG 코드 분석 및 구현 세부사항

## 📊 1. nasdaq_daily_pipeline.py - 메인 데이터 수집 파이프라인

### 🎯 **DAG 기본 설정**

```python
# DAG 정의
dag = DAG(
    'nasdaq_daily_pipeline',
    default_args=default_args,
    description='🚀 나스닥 완전 파이프라인: 수집→분석→스캔→복제 (Kafka Ready)',
    schedule_interval='0 7 * * *',  # 한국시간 오전 7시 (미국 장마감 후)
    catchup=False,
    max_active_runs=1,
    tags=['nasdaq', 'stock', 'spark', 'technical-analysis', 'watchlist', 'kafka']
)
```

**핵심 설정**:
- **스케줄**: 매일 오전 7시 (미국 장마감 후)
- **동시 실행 제한**: 1개 (데이터 일관성 보장)
- **Catchup 비활성화**: 과거 실행 건너뛰기

### 🔄 **Task 1: 나스닥 심볼 수집**

```python
def collect_nasdaq_symbols_func(**kwargs):
    """나스닥 심볼 수집 함수"""
    import sys
    sys.path.append('/opt/airflow/plugins')
    sys.path.append('/opt/airflow/common')
    
    from collect_nasdaq_symbols_api import NasdaqSymbolCollector
    from database import DuckDBManager
    
    print("🚀 나스닥 심볼 수집 시작...")
    
    # 1. 심볼 수집기 초기화
    collector = NasdaqSymbolCollector()
    
    # 2. NASDAQ API에서 심볼 수집
    collected_count = collector.collect_and_save_symbols()
    
    # 3. 결과 검증
    if collected_count < 100:
        raise ValueError(f"수집된 심볼이 너무 적습니다: {collected_count}개")
    
    print(f"✅ 총 {collected_count}개 심볼 수집 완료")
    return collected_count
```

**핵심 기능**:
- NASDAQ API 호출 및 심볼 목록 수집
- 시가총액 기준 필터링
- DuckDB에 중복 제거하여 저장
- 최소 수집 개수 검증

### 📈 **Task 2: 주식 데이터 수집**

```python
def collect_stock_data_yfinance_func(**kwargs):
    """Yahoo Finance에서 주식 데이터 수집"""
    from collect_stock_data_yfinance import collect_stock_data_yfinance_task
    from database import DuckDBManager
    
    db = DuckDBManager(DB_PATH)
    
    try:
        # 1. 수집 대상 심볼 조회
        symbols_query = """
            SELECT DISTINCT symbol 
            FROM nasdaq_symbols 
            WHERE is_active = true 
            AND market_cap > 100000000  -- 1억 달러 이상
            ORDER BY market_cap DESC
        """
        
        symbols_df = db.execute_query(symbols_query)
        symbols = symbols_df['symbol'].tolist()
        
        print(f"🎯 수집 대상: {len(symbols)}개 심볼")
        
        # 2. 배치 처리로 데이터 수집
        batch_size = 100
        total_success = 0
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            print(f"📦 배치 {i//batch_size + 1} 처리 중... ({len(batch)}개)")
            
            # Yahoo Finance API 호출
            success_count = collect_stock_data_yfinance_task(
                symbols=batch,
                db_path=DB_PATH,
                days_back=5  # 최근 5일 데이터
            )
            
            total_success += success_count
            print(f"✅ 배치 완료: {success_count}/{len(batch)}개 성공")
        
        print(f"🎉 전체 수집 완료: {total_success}/{len(symbols)}개")
        return total_success
        
    except Exception as e:
        print(f"❌ 주식 데이터 수집 실패: {e}")
        raise
    finally:
        db.close()
```

**성능 최적화**:
- 배치 처리 (100개씩)
- 병렬 API 호출
- 연결 풀링 및 재사용
- 실패한 심볼 재시도

### ⚙️ **Task 3: 기술적 지표 계산 (Spark)**

```python
def calculate_technical_indicators_func(**kwargs):
    """Spark를 사용한 기술적 지표 계산"""
    from technical_indicators import calculate_technical_indicators_task
    
    print("📊 Spark 기술적 지표 계산 시작...")
    
    # Spark 설정
    spark_config = {
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    }
    
    try:
        # 기술적 지표 계산 실행
        processed_symbols = calculate_technical_indicators_task(
            db_path=DB_PATH,
            spark_config=spark_config,
            indicators=['rsi_14', 'macd', 'bollinger_bands', 'sma_20', 'sma_50']
        )
        
        print(f"✅ {processed_symbols}개 심볼 기술적 지표 계산 완료")
        return processed_symbols
        
    except Exception as e:
        print(f"❌ 기술적 지표 계산 실패: {e}")
        raise
```

**계산 지표**:
- **RSI (14일)**: 과매수/과매도 신호
- **MACD**: 추세 변화 감지
- **볼린저 밴드**: 변동성 기반 신호
- **이동평균선**: 추세 확인

### 🎯 **Task 4: 관심종목 스캔**

```python
def watchlist_scan_func(**kwargs):
    """기술적 분석 기반 관심종목 스캔"""
    from database import DuckDBManager
    from datetime import date
    
    db = DuckDBManager(DB_PATH)
    
    try:
        # 스캔 조건별 쿼리 실행
        scan_results = []
        
        # 1. RSI 과매도 종목 (RSI < 30)
        rsi_oversold_query = """
            SELECT 
                s.symbol, s.name, s.sector, s.market_cap,
                sd.close, sd.volume,
                ti.rsi_14, ti.bb_position,
                'rsi_oversold' as scan_reason,
                1 as priority
            FROM nasdaq_symbols s
            JOIN stock_data sd ON s.symbol = sd.symbol
            JOIN technical_indicators ti ON s.symbol = ti.symbol AND sd.date = ti.date
            WHERE sd.date = ?
            AND ti.rsi_14 < 30
            AND s.market_cap > 500000000  -- 5억 달러 이상
            AND sd.volume > s.avg_volume_30 * 1.5  -- 평소보다 50% 이상 거래량
            ORDER BY s.market_cap DESC
            LIMIT 20
        """
        
        # 2. 볼린저 밴드 하단 터치
        bollinger_touch_query = """
            SELECT 
                s.symbol, s.name, s.sector, s.market_cap,
                sd.close, sd.volume,
                ti.bb_lower, ti.bb_upper, ti.bb_position,
                'bollinger_lower_touch' as scan_reason,
                2 as priority
            FROM nasdaq_symbols s
            JOIN stock_data sd ON s.symbol = sd.symbol
            JOIN technical_indicators ti ON s.symbol = ti.symbol AND sd.date = ti.date
            WHERE sd.date = ?
            AND sd.close <= ti.bb_lower * 1.02  -- 하단 밴드 2% 내
            AND s.market_cap > 1000000000  -- 10억 달러 이상
            ORDER BY (ti.bb_lower - sd.close) DESC
            LIMIT 15
        """
        
        # 3. 대량 거래 돌파
        high_volume_query = """
            SELECT 
                s.symbol, s.name, s.sector, s.market_cap,
                sd.close, sd.volume,
                (sd.volume / s.avg_volume_30) as volume_ratio,
                'high_volume_breakout' as scan_reason,
                3 as priority
            FROM nasdaq_symbols s
            JOIN stock_data sd ON s.symbol = sd.symbol
            WHERE sd.date = ?
            AND sd.volume > s.avg_volume_30 * 3  -- 평소의 3배 이상
            AND sd.close > sd.open * 1.02  -- 2% 이상 상승
            ORDER BY (sd.volume / s.avg_volume_30) DESC
            LIMIT 10
        """
        
        today = date.today()
        
        # 각 조건별 스캔 실행
        for query_name, query in [
            ('rsi_oversold', rsi_oversold_query),
            ('bollinger_touch', bollinger_touch_query),
            ('high_volume', high_volume_query)
        ]:
            print(f"🔍 {query_name} 조건 스캔 중...")
            
            results = db.execute_query(query, (today,))
            scan_results.extend(results.to_dict('records'))
            
            print(f"✅ {query_name}: {len(results)}개 종목 발견")
        
        # 중복 제거 및 우선순위 정렬
        unique_results = {}
        for result in scan_results:
            symbol = result['symbol']
            if symbol not in unique_results or result['priority'] < unique_results[symbol]['priority']:
                unique_results[symbol] = result
        
        final_watchlist = list(unique_results.values())
        
        # daily_watchlist 테이블에 저장
        if final_watchlist:
            save_daily_watchlist(db, final_watchlist, today)
            print(f"💾 {len(final_watchlist)}개 관심종목 저장 완료")
        else:
            print("⚠️ 조건에 맞는 관심종목이 없습니다")
        
        return len(final_watchlist)
        
    finally:
        db.close()

def save_daily_watchlist(db, watchlist_data, scan_date):
    """일일 관심종목을 DB에 저장"""
    
    # 기존 데이터 삭제 (같은 날짜)
    delete_query = "DELETE FROM daily_watchlist WHERE scan_date = ?"
    db.execute_query(delete_query, (scan_date,))
    
    # 새 데이터 삽입
    insert_query = """
        INSERT INTO daily_watchlist 
        (symbol, name, sector, market_cap, scan_date, scan_reason, priority,
         close_price, volume, rsi_14, bb_position, volume_ratio, market_cap_tier)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for item in watchlist_data:
        # 시가총액 구간 분류
        market_cap = item.get('market_cap', 0)
        if market_cap >= 50_000_000_000:  # 500억 이상
            tier = 1  # 대형주
        elif market_cap >= 10_000_000_000:  # 100억 이상
            tier = 2  # 중형주
        else:
            tier = 3  # 소형주
        
        db.execute_query(insert_query, (
            item['symbol'], item['name'], item['sector'], item['market_cap'],
            scan_date, item['scan_reason'], item['priority'],
            item['close'], item['volume'],
            item.get('rsi_14'), item.get('bb_position'), item.get('volume_ratio'),
            tier
        ))
```

**스캔 전략**:
- **다중 조건 스캔**: RSI, 볼린저 밴드, 거래량
- **우선순위 기반 선별**: 중복 시 더 강한 신호 우선
- **시가총액 필터링**: 안정성 확보
- **거래량 검증**: 유동성 확보

### 🔄 **Task 5: 데이터 복제 (Read Replica)**

```python
def database_replication_func(**kwargs):
    """DuckDB 읽기 전용 복제본 생성"""
    import shutil
    from pathlib import Path
    
    source_db = Path(DB_PATH)
    replica_db = Path("/data/duckdb/stock_data_replica.db")
    
    try:
        print("🔄 데이터베이스 복제 시작...")
        
        # 1. 소스 DB 백업
        backup_db = Path("/data/duckdb/backup/stock_data_backup.db")
        backup_db.parent.mkdir(exist_ok=True)
        shutil.copy2(source_db, backup_db)
        
        # 2. 읽기 전용 복제본 생성
        shutil.copy2(source_db, replica_db)
        
        # 3. 권한 설정 (읽기 전용)
        replica_db.chmod(0o444)  # r--r--r--
        
        # 4. 복제 검증
        from database import DuckDBManager
        replica_manager = DuckDBManager(str(replica_db))
        
        # 테이블 수 확인
        table_count_query = """
            SELECT COUNT(*) as table_count
            FROM information_schema.tables 
            WHERE table_schema = 'main'
        """
        result = replica_manager.execute_query(table_count_query)
        table_count = result.iloc[0]['table_count']
        
        replica_manager.close()
        
        if table_count < 5:  # 최소 테이블 수 검증
            raise ValueError(f"복제 DB 테이블 수 부족: {table_count}개")
        
        print(f"✅ 복제 완료: {table_count}개 테이블")
        return "success"
        
    except Exception as e:
        print(f"❌ 데이터베이스 복제 실패: {e}")
        # 실패 시 이전 복제본 유지
        if backup_db.exists() and not replica_db.exists():
            shutil.copy2(backup_db, replica_db)
        raise
```

**복제 전략**:
- **파일 기반 복제**: 빠른 복사
- **읽기 전용 보장**: 권한 제한
- **백업 보관**: 실패 시 복구
- **무결성 검증**: 테이블 수 확인

## 🔄 2. redis_watchlist_sync.py - Redis 스마트 동기화

### 🧠 **스마트 업데이트 로직**

```python
def redis_smart_sync_task(**kwargs):
    """스마트 증분 업데이트 실행"""
    try:
        from load_watchlist_to_redis import WatchlistDataLoader
        
        # 실행 모드 결정
        execution_date = kwargs['execution_date']
        is_weekend = execution_date.weekday() in [5, 6]  # 토, 일
        force_full = kwargs.get('dag_run').conf.get('force_full', False) if kwargs.get('dag_run') and kwargs.get('dag_run').conf else False
        
        loader = WatchlistDataLoader()
        
        if force_full or is_weekend:
            print("🔄 주말 전체 재동기화 모드")
            success = loader.load_watchlist_to_redis(days_back=30)
            loader.set_last_update_info(1, 1, "weekly_full_sync")
        else:
            print("⚡ 평일 스마트 증분 업데이트")
            success = loader.smart_incremental_update()
        
        return "success" if success else raise Exception("Redis 동기화 실패")
```

**업데이트 전략**:
- **평일**: 스마트 증분 업데이트 (2-5분)
- **주말**: 전체 재동기화 (10-15분)
- **수동**: force_full 파라미터로 강제 실행

### 🔍 **Redis 상태 검증**

```python
def redis_health_check_task(**kwargs):
    """Redis 상태 및 데이터 검증"""
    from redis_client import RedisClient
    
    redis_client = RedisClient()
    
    # 1. 서버 상태 확인
    info = redis_client.redis_client.info()
    memory_usage = info.get('used_memory_human', 'Unknown')
    
    # 2. 데이터 개수 확인
    watchlist_keys = redis_client.redis_client.keys("watchlist_data:*")
    signal_keys = redis_client.redis_client.keys("signal_trigger:*")
    
    # 3. 최소 요구사항 검증
    if len(watchlist_keys) < 10:
        raise Exception(f"관심종목 데이터 부족: {len(watchlist_keys)}개")
    
    # 4. 샘플 데이터 무결성 확인
    if watchlist_keys:
        sample_data = redis_client.redis_client.get(watchlist_keys[0])
        if not sample_data:
            raise Exception("샘플 데이터 조회 실패")
    
    print(f"✅ Redis 상태 정상: {len(watchlist_keys)}개 관심종목")
    return "healthy"
```

### 🧹 **자동 데이터 정리**

```python
def cleanup_old_data_task(**kwargs):
    """오래된 Redis 데이터 정리"""
    from redis_client import RedisClient
    
    redis_client = RedisClient()
    cutoff_date = datetime.now() - timedelta(days=7)
    
    # 1. 오래된 신호 정리
    old_signal_keys = redis_client.redis_client.keys("signal_trigger:*")
    cleaned_signals = 0
    
    for key in old_signal_keys:
        try:
            key_str = key.decode('utf-8')
            parts = key_str.split(':')
            if len(parts) >= 3:
                timestamp_str = parts[2]
                signal_date = datetime.fromisoformat(timestamp_str[:19])
                
                if signal_date < cutoff_date:
                    redis_client.redis_client.delete(key)
                    cleaned_signals += 1
        except:
            continue
    
    # 2. 임시 데이터 정리
    temp_keys = redis_client.redis_client.keys("temp_*")
    for key in temp_keys:
        redis_client.redis_client.delete(key)
    
    print(f"🧹 정리 완료: 신호 {cleaned_signals}개, 임시 {len(temp_keys)}개")
    return "cleaned"
```

## 📊 3. Task 의존성 및 실행 흐름

### 🔄 **nasdaq_daily_pipeline 실행 흐름**

```python
# Task 의존성 정의
collect_symbols >> collect_stock_data >> calculate_indicators >> watchlist_scan >> database_replication >> success_notification

# 병렬 실행 가능한 Task들
[cleanup_old_files, generate_reports] >> final_notification
```

### ⚡ **redis_watchlist_sync 실행 흐름**

```python
# 순차 실행
wait_for_nasdaq >> redis_sync >> redis_health_check >> [signal_prepare, cleanup_data] >> success_notification

# 수동 실행 Task (독립적)
manual_full_sync  # 언제든 수동 실행 가능
```

## 🎯 4. 핵심 성능 지표 및 모니터링

### 📈 **처리 성능 메트릭**

```python
# DAG 성능 추적
def track_performance(**kwargs):
    """DAG 성능 메트릭 수집"""
    
    task_instance = kwargs['task_instance']
    execution_date = kwargs['execution_date']
    
    # 실행 시간 측정
    start_time = task_instance.start_date
    end_time = task_instance.end_date
    duration = (end_time - start_time).total_seconds()
    
    # 메트릭 저장
    metrics = {
        'dag_id': task_instance.dag_id,
        'task_id': task_instance.task_id,
        'execution_date': execution_date,
        'duration_seconds': duration,
        'status': task_instance.state,
        'try_number': task_instance.try_number
    }
    
    # Redis에 성능 데이터 저장
    redis_client.redis_client.lpush('dag_performance_metrics', json.dumps(metrics))
    
    return metrics
```

### 🔔 **알림 및 모니터링**

```python
# 실패 시 알림
def send_failure_notification(**kwargs):
    """DAG 실패 시 알림 전송"""
    
    task_instance = kwargs['task_instance']
    exception = kwargs.get('exception', 'Unknown error')
    
    notification = {
        'type': 'dag_failure',
        'dag_id': task_instance.dag_id,
        'task_id': task_instance.task_id,
        'execution_date': str(kwargs['execution_date']),
        'error_message': str(exception),
        'timestamp': datetime.now().isoformat()
    }
    
    # Slack/Discord 웹훅 (설정 시)
    if SLACK_WEBHOOK_URL:
        send_slack_notification(notification)
    
    # Redis에 알림 로그 저장
    redis_client.redis_client.lpush('system_alerts', json.dumps(notification))

# DAG 레벨 실패 핸들러 설정
dag.on_failure_callback = send_failure_notification
```

이 Airflow 기반 파이프라인은 **완전 자동화된 주식 데이터 처리 시스템**으로, 수집부터 실시간 신호 감지까지 모든 과정을 효율적으로 관리합니다! 🚀
