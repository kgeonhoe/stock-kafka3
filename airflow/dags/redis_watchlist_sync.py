#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis 관심종목 데이터 동기화 DAG
- 스마트 증분 업데이트 지원
- 실시간 신호 감지 시스템을 위한 데이터 준비
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import sys
import os

# 공통 모듈 경로 추가
sys.path.insert(0, '/opt/airflow/common')
sys.path.insert(0, '/opt/airflow/plugins')

# 기본 인수 설정
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

# DAG 정의
dag = DAG(
    'redis_watchlist_sync',
    default_args=default_args,
    description='🔄 Redis 관심종목 스마트 동기화 (최적화된 대량 수집 완료 후)',
    schedule_interval=None,  # 수동 트리거 (의존성 기반)
    catchup=False,
    max_active_runs=1,
    tags=['redis', 'watchlist', 'incremental', 'smart-update', 'signal-detection', 'triggered', 'optimized']
)

def read_pipeline_completion_info(**kwargs):
    """완료된 파이프라인 정보 읽기 (대량 수집 또는 일반 파이프라인)"""
    import json
    
    try:
        # 1. 대량 수집 완료 플래그 확인 (우선순위)
        bulk_flag_file = "/tmp/nasdaq_bulk_collection_complete.flag"
        
        if os.path.exists(bulk_flag_file):
            with open(bulk_flag_file, 'r') as f:
                completion_info = json.loads(f.read())
            
            print(f"📊 대량 수집 파이프라인 완료 정보:")
            print(f"   완료 시간: {completion_info['completion_time']}")
            print(f"   DAG Run ID: {completion_info['dag_run_id']}")
            print(f"   수집 타입: {completion_info['collection_type']}")
            
            # 수집 통계
            final_stats = completion_info.get('final_stats', {})
            main_table = final_stats.get('main_table', {})
            print(f"   총 레코드: {main_table.get('record_count', 0):,}")
            print(f"   종목 수: {main_table.get('symbol_count', 0):,}")
            
            date_range = main_table.get('date_range', {})
            print(f"   데이터 범위: {date_range.get('start', 'Unknown')} ~ {date_range.get('end', 'Unknown')}")
            
            # Redis 동기화용 정보 준비
            pipeline_results = {
                'collected_symbols': main_table.get('symbol_count', 0),
                'collected_ohlcv': main_table.get('record_count', 0),
                'calculated_indicators': 0,  # 기술적 지표는 별도 계산
                'scanned_watchlist': 0,     # 관심종목은 Redis에서 별도 관리
                'data_source': 'bulk_collection',
                'data_quality': completion_info.get('validation_results', {})
            }
            
            completion_info['pipeline_results'] = pipeline_results
            
            # XCom에 정보 저장
            kwargs['ti'].xcom_push(key='pipeline_completion_info', value=completion_info)
            
            return completion_info
        
        # 2. 기존 일반 파이프라인 플래그 확인 (백업)
        flag_file = "/tmp/nasdaq_pipeline_complete.flag"
        
        if os.path.exists(flag_file):
            with open(flag_file, 'r') as f:
                completion_info = json.loads(f.read())
            
            print(f"📊 일반 파이프라인 완료 정보:")
            print(f"   완료 시간: {completion_info['completion_time']}")
            print(f"   DAG Run ID: {completion_info['dag_run_id']}")
            
            results = completion_info.get('pipeline_results', {})
            print(f"   처리 심볼 수: {results.get('collected_symbols', 0)}")
            print(f"   수집 OHLCV: {results.get('collected_ohlcv', 0)}")
            print(f"   계산 지표: {results.get('calculated_indicators', 0)}")
            print(f"   관심종목 수: {results.get('scanned_watchlist', 0)}")
            
            # XCom에 정보 저장
            kwargs['ti'].xcom_push(key='pipeline_completion_info', value=completion_info)
            
            return completion_info
        
        # 3. 둘 다 없으면 오류
        raise FileNotFoundError("파이프라인 완료 플래그 파일을 찾을 수 없습니다")
        
    except Exception as e:
        print(f"❌ 파이프라인 완료 정보 읽기 실패: {e}")
        raise

def redis_smart_sync_task(**kwargs):
    """스마트 증분 업데이트 실행"""
    try:
        from load_watchlist_to_redis import WatchlistDataLoader
        
        print("🧠 Redis 스마트 동기화 시작...")
        
        # 실행 모드 결정 (execution_date 기반)
        execution_date = kwargs['execution_date']
        current_date = datetime.now()
        
        # 주말이면 전체 재동기화
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
        
        if success:
            print("✅ Redis 동기화 성공!")
            return "success"
        else:
            print("❌ Redis 동기화 실패!")
            raise Exception("Redis 동기화 실패")
            
    except Exception as e:
        print(f"💥 Redis 동기화 중 오류: {e}")
        raise

def redis_health_check_task(**kwargs):
    """Redis 상태 및 데이터 검증"""
    try:
        from redis_client import RedisClient
        
        redis_client = RedisClient()
        
        # 1. Redis 연결 확인
        info = redis_client.redis_client.info()
        print(f"📊 Redis 서버 상태: {info.get('redis_version', 'Unknown')}")
        print(f"💾 메모리 사용량: {info.get('used_memory_human', 'Unknown')}")
        
        # 2. 관심종목 데이터 검증
        watchlist_keys = redis_client.redis_client.keys("watchlist_data:*")
        signal_keys = redis_client.redis_client.keys("signal_trigger:*")
        
        print(f"🎯 관심종목 데이터: {len(watchlist_keys)}개")
        print(f"📈 활성 신호: {len(signal_keys)}개")
        
        # 3. 최소 데이터 요구사항 검증
        if len(watchlist_keys) < 10:
            raise Exception(f"관심종목 데이터 부족: {len(watchlist_keys)}개 (최소 10개 필요)")
        
        # 4. 샘플 데이터 무결성 검증
        if watchlist_keys:
            sample_key = watchlist_keys[0]
            sample_data = redis_client.redis_client.get(sample_key)
            if not sample_data:
                raise Exception("샘플 데이터 조회 실패")
        
        print("✅ Redis 상태 검증 완료!")
        return "healthy"
        
    except Exception as e:
        print(f"❌ Redis 상태 검증 실패: {e}")
        raise

def signal_detection_prepare_task(**kwargs):
    """신호 감지 시스템 준비"""
    try:
        from redis_client import RedisClient
        
        redis_client = RedisClient()
        
        # 신호 감지를 위한 메타데이터 준비
        watchlist_keys = redis_client.redis_client.keys("watchlist_data:*")
        
        signal_ready_symbols = []
        for key in watchlist_keys:
            symbol = key.decode('utf-8').split(':')[1]
            
            # 각 종목의 데이터 충분성 확인
            data = redis_client.get_watchlist_data(symbol)
            if data and data.get('historical_data'):
                historical_data = data['historical_data']
                if len(historical_data) >= 20:  # 기술적 지표 계산을 위한 최소 데이터
                    signal_ready_symbols.append(symbol)
        
        print(f"🚨 신호 감지 준비 완료: {len(signal_ready_symbols)}개 종목")
        
        # 신호 감지 준비 상태 저장
        ready_status = {
            'timestamp': datetime.now().isoformat(),
            'ready_symbols_count': len(signal_ready_symbols),
            'total_symbols': len(watchlist_keys),
            'readiness_rate': len(signal_ready_symbols) / len(watchlist_keys) if watchlist_keys else 0
        }
        
        redis_client.redis_client.setex(
            "signal_detection_ready",
            3600,  # 1시간 TTL
            str(ready_status)
        )
        
        return "prepared"
        
    except Exception as e:
        print(f"❌ 신호 감지 준비 실패: {e}")
        raise

def cleanup_old_data_task(**kwargs):
    """오래된 Redis 데이터 정리"""
    try:
        from redis_client import RedisClient
        
        redis_client = RedisClient()
        
        # 1. 오래된 신호 데이터 정리 (7일 이상)
        old_signal_keys = redis_client.redis_client.keys("signal_trigger:*")
        cleaned_signals = 0
        
        cutoff_date = datetime.now() - timedelta(days=7)
        
        for key in old_signal_keys:
            try:
                # 키에서 타임스탬프 추출하여 확인
                key_str = key.decode('utf-8')
                # signal_trigger:SYMBOL:TIMESTAMP 형식
                parts = key_str.split(':')
                if len(parts) >= 3:
                    timestamp_str = parts[2]
                    signal_date = datetime.fromisoformat(timestamp_str[:19])
                    
                    if signal_date < cutoff_date:
                        redis_client.redis_client.delete(key)
                        cleaned_signals += 1
            except:
                continue
        
        # 2. 임시 분석 데이터 정리
        temp_keys = redis_client.redis_client.keys("temp_*")
        for key in temp_keys:
            redis_client.redis_client.delete(key)
        
        print(f"🧹 데이터 정리 완료:")
        print(f"   📈 정리된 오래된 신호: {cleaned_signals}개")
        print(f"   🗑️ 정리된 임시 데이터: {len(temp_keys)}개")
        
        return "cleaned"
        
    except Exception as e:
        print(f"❌ 데이터 정리 실패: {e}")
        raise

# 1. 대량 수집 또는 일반 파이프라인 완료 대기
wait_for_pipeline = FileSensor(
    task_id='wait_for_pipeline_completion',
    filepath='/tmp/nasdaq_bulk_collection_complete.flag',  # 우선 대량 수집 완료 확인
    fs_conn_id='fs_default',
    poke_interval=300,  # 5분마다 확인
    timeout=7200,  # 2시간 타임아웃 (대량 수집은 오래 걸림)
    soft_fail=True,   # 실패 시 일반 파이프라인 플래그로 대체
    dag=dag
)

# 1-1. 백업: 일반 파이프라인 완료 대기 (대량 수집 실패 시)
wait_for_nasdaq_backup = FileSensor(
    task_id='wait_for_nasdaq_pipeline_backup',
    filepath='/tmp/nasdaq_pipeline_complete.flag',
    fs_conn_id='fs_default',
    poke_interval=300,
    timeout=3600,
    soft_fail=False,
    trigger_rule='one_failed',  # 대량 수집 대기가 실패했을 때만 실행
    dag=dag
)

# 2. 파이프라인 완료 정보 읽기
read_completion_info = PythonOperator(
    task_id='read_pipeline_completion_info',
    python_callable=read_pipeline_completion_info,
    provide_context=True,
    dag=dag
)

# 3. Redis 스마트 동기화
redis_sync = PythonOperator(
    task_id='redis_smart_sync',
    python_callable=redis_smart_sync_task,
    provide_context=True,
    dag=dag
)

# 3. Redis 상태 검증
redis_health_check = PythonOperator(
    task_id='redis_health_check',
    python_callable=redis_health_check_task,
    provide_context=True,
    dag=dag
)

# 4. 신호 감지 시스템 준비
signal_prepare = PythonOperator(
    task_id='prepare_signal_detection',
    python_callable=signal_detection_prepare_task,
    provide_context=True,
    dag=dag
)

# 5. 오래된 데이터 정리
cleanup_data = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data_task,
    provide_context=True,
    dag=dag
)

# 6. 성공 알림 (선택적)
success_notification = BashOperator(
    task_id='success_notification',
    bash_command='''
    echo "✅ Redis 관심종목 동기화 완료!"
    echo "🕐 완료 시간: $(date)"
    echo "📊 로그는 Airflow UI에서 확인하세요."
    ''',
    dag=dag
)

# Task 의존성 설정 - 순차적 실행
[wait_for_pipeline, wait_for_nasdaq_backup] >> read_completion_info >> redis_sync >> redis_health_check >> [signal_prepare, cleanup_data] >> success_notification

# 수동 실행을 위한 독립적인 task
manual_full_sync = PythonOperator(
    task_id='manual_full_sync',
    python_callable=lambda **kwargs: redis_smart_sync_task(force_full=True, **kwargs),
    provide_context=True,
    dag=dag
)

if __name__ == "__main__":
    dag.cli()
