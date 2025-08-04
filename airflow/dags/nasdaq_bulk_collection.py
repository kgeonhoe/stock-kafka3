#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
최적화된 나스닥 대량 데이터 수집 DAG
- FinanceDataReader 기반 5년치 데이터 수집
- HDD 최적화된 배치 처리
- Parquet 기반 고속 로드
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'max_active_runs': 1,
    'execution_timeout': timedelta(hours=2),  # 2시간 타임아웃
    'task_concurrency': 1  # 태스크 동시 실행 제한
}

# DAG 정의
dag = DAG(
    'nasdaq_bulk_data_collection',
    default_args=default_args,
    description='🚀 나스닥 5년치 대량 데이터 수집 (FinanceDataReader + 최적화)',
    schedule_interval='0 2 * * 0',  # 매주 일요일 02:00 (주말 대량 처리)
    catchup=False,
    max_active_runs=1,
    tags=['nasdaq', 'bulk-collection', 'financedatareader', 'parquet', 'optimized']
)

def check_system_resources(**kwargs):
    """시스템 리소스 확인 및 대량 수집 시작 플래그 생성"""
    import psutil
    import shutil
    from utils.dag_coordination import create_bulk_collection_running_flag
    
    # 대량 수집 시작 플래그 생성
    create_bulk_collection_running_flag(**kwargs)
    
    # 메모리 확인
    memory = psutil.virtual_memory()
    available_memory_gb = memory.available / (1024 ** 3)
    
    # 디스크 공간 확인
    disk_usage = shutil.disk_usage('/data')
    available_disk_gb = disk_usage.free / (1024 ** 3)
    
    print(f"💾 시스템 리소스 상태:")
    print(f"   사용 가능 메모리: {available_memory_gb:.1f} GB")
    print(f"   사용 가능 디스크: {available_disk_gb:.1f} GB")
    print(f"   메모리 사용률: {memory.percent:.1f}%")
    
    # 최소 요구사항 확인
    if available_memory_gb < 1.0:
        raise Exception(f"메모리 부족: {available_memory_gb:.1f}GB (최소 1GB 필요)")
    
    if available_disk_gb < 50.0:
        raise Exception(f"디스크 공간 부족: {available_disk_gb:.1f}GB (최소 50GB 필요)")
    
    return {
        'available_memory_gb': available_memory_gb,
        'available_disk_gb': available_disk_gb,
        'memory_usage_percent': memory.percent
    }

def initialize_optimized_storage(**kwargs):
    """최적화된 저장소 초기화"""
    from batch_scheduler import DuckDBBatchScheduler, CheckpointConfig
    from parquet_loader import ParquetDataLoader
    
    # 배치 스케줄러 설정 (HDD 최적화)
    config = CheckpointConfig(
        interval_seconds=300,        # 5분마다 체크
        wal_size_threshold_mb=200,   # 200MB WAL 임계값
        memory_threshold_percent=75, # 75% 메모리 임계값
        force_checkpoint_interval=1800  # 30분마다 강제 체크포인트
    )
    
    # 스케줄러 시작
    scheduler = DuckDBBatchScheduler("/data/duckdb/stock_data.db", config)
    scheduler.start()
    
    # Parquet 로더 초기화
    parquet_loader = ParquetDataLoader("/data/parquet_storage")
    
    # Append-only 구조 생성
    success = parquet_loader.create_append_only_structure(
        "/data/duckdb/stock_data.db", 
        "stock_data"
    )
    
    if not success:
        raise Exception("Append-only 구조 생성 실패")
    
    print("✅ 최적화된 저장소 초기화 완료")
    
    # XCom에 설정 정보 저장
    kwargs['ti'].xcom_push(key='scheduler_config', value=config.__dict__)
    kwargs['ti'].xcom_push(key='storage_initialized', value=True)
    
    return success

def collect_nasdaq_symbols_bulk(**kwargs):
    """나스닥 종목 대량 수집"""
    from bulk_data_collector import BulkDataCollector
    
    try:
        # 대량 수집기 초기화
        collector = BulkDataCollector(
            db_path="/data/duckdb/stock_data.db",
            batch_size=2000,  # HDD 환경에 맞춰 조정
            max_workers=3     # 메모리 제한 고려
        )
        
        print("📊 나스닥 종목 리스트 수집 중...")
        
        # 전체 나스닥 종목 수집
        symbols = collector.get_nasdaq_symbols()
        
        if not symbols:
            raise Exception("나스닥 종목 리스트 수집 실패")
        
        print(f"✅ 나스닥 종목 수집 완료: {len(symbols)}개")
        
        # XCom에 저장 (다음 task에서 사용)
        kwargs['ti'].xcom_push(key='nasdaq_symbols', value=symbols)
        kwargs['ti'].xcom_push(key='symbol_count', value=len(symbols))
        
        return len(symbols)
        
    except Exception as e:
        print(f"❌ 나스닥 종목 수집 실패: {e}")
        raise
    finally:
        if 'collector' in locals():
            collector.close()

def collect_5year_data_batch_1(**kwargs):
    """5년치 데이터 수집 - 배치 1 (상위 1/3 종목)"""
    return _collect_data_batch(kwargs, batch_num=1, total_batches=3)

def collect_5year_data_batch_2(**kwargs):
    """5년치 데이터 수집 - 배치 2 (중간 1/3 종목)"""
    return _collect_data_batch(kwargs, batch_num=2, total_batches=3)

def collect_5year_data_batch_3(**kwargs):
    """5년치 데이터 수집 - 배치 3 (하위 1/3 종목)"""
    return _collect_data_batch(kwargs, batch_num=3, total_batches=3)

def _collect_data_batch(kwargs, batch_num: int, total_batches: int):
    """데이터 배치 수집 공통 함수"""
    from bulk_data_collector import BulkDataCollector
    from parquet_loader import ParquetDataLoader
    import math
    
    try:
        # 이전 task에서 종목 리스트 가져오기
        symbols = kwargs['ti'].xcom_pull(key='nasdaq_symbols', task_ids='collect_nasdaq_symbols')
        
        if not symbols:
            raise Exception("나스닥 종목 리스트를 찾을 수 없음")
        
        # 배치 분할
        batch_size = math.ceil(len(symbols) / total_batches)
        start_idx = (batch_num - 1) * batch_size
        end_idx = min(start_idx + batch_size, len(symbols))
        batch_symbols = symbols[start_idx:end_idx]
        
        print(f"🔄 배치 {batch_num} 데이터 수집 시작:")
        print(f"   대상 종목: {len(batch_symbols)}개 ({start_idx+1}-{end_idx})")
        print(f"   수집 기간: 2020-01-01 ~ 현재 (약 5년)")
        
        # 대량 수집기 초기화 (더 보수적인 설정)
        collector = BulkDataCollector(
            db_path="/data/duckdb/stock_data.db",
            batch_size=500,   # 배치 크기를 더 작게 (메모리 절약)
            max_workers=1     # 단일 스레드로 안정성 확보
        )
        
        # Parquet 로더 초기화
        parquet_loader = ParquetDataLoader("/data/parquet_storage")
        
        # 데이터 수집 (5년치)
        success = collector.collect_bulk_data(
            symbols=batch_symbols,
            start_date='2020-01-01',  # 5년 전
            batch_symbols=20  # 한 번에 20개씩 처리
        )
        
        if not success:
            raise Exception(f"배치 {batch_num} 데이터 수집 실패")
        
        # 수집 통계
        stats = collector.get_collection_stats()
        print(f"✅ 배치 {batch_num} 수집 완료:")
        print(f"   종목 수: {stats.get('symbol_count', 0)}")
        print(f"   총 레코드: {stats.get('total_records', 0)}")
        print(f"   날짜 범위: {stats.get('date_range', {})}")
        
        # XCom에 결과 저장
        kwargs['ti'].xcom_push(key=f'batch_{batch_num}_stats', value=stats)
        
        return stats
        
    except Exception as e:
        print(f"❌ 배치 {batch_num} 수집 실패: {e}")
        raise
    finally:
        if 'collector' in locals():
            collector.close()

def merge_and_optimize_data(**kwargs):
    """수집된 데이터 병합 및 최적화"""
    from parquet_loader import ParquetDataLoader
    from batch_scheduler import DuckDBBatchScheduler, CheckpointConfig
    
    try:
        print("🔧 데이터 병합 및 최적화 시작...")
        
        # Parquet 로더 초기화
        parquet_loader = ParquetDataLoader("/data/parquet_storage")
        
        # 모든 배치를 메인 테이블로 병합
        success = parquet_loader.merge_batches_to_main(
            table_name="stock_data",
            db_path="/data/duckdb/stock_data.db",
            max_batches=50  # 최대 50개 배치까지 한 번에 병합
        )
        
        if not success:
            raise Exception("배치 병합 실패")
        
        # 저장소 최적화 (인덱스, 압축 등)
        parquet_loader.optimize_storage(
            table_name="stock_data",
            db_path="/data/duckdb/stock_data.db"
        )
        
        # Parquet 아카이브 생성 (백업)
        archive_path = parquet_loader.export_to_parquet_archive(
            table_name="stock_data",
            db_path="/data/duckdb/stock_data.db",
            partition_by="date"
        )
        
        # 최종 통계
        final_stats = parquet_loader.get_storage_stats(
            table_name="stock_data", 
            db_path="/data/duckdb/stock_data.db"
        )
        
        print("✅ 데이터 병합 및 최적화 완료:")
        print(f"   메인 테이블 레코드: {final_stats.get('main_table', {}).get('record_count', 0)}")
        print(f"   종목 수: {final_stats.get('main_table', {}).get('symbol_count', 0)}")
        print(f"   아카이브 경로: {archive_path}")
        
        # XCom에 결과 저장
        kwargs['ti'].xcom_push(key='final_stats', value=final_stats)
        kwargs['ti'].xcom_push(key='archive_path', value=archive_path)
        
        return final_stats
        
    except Exception as e:
        print(f"❌ 데이터 병합 및 최적화 실패: {e}")
        raise

def validate_data_quality(**kwargs):
    """데이터 품질 검증"""
    import duckdb
    from datetime import datetime, timedelta
    
    try:
        print("🔍 데이터 품질 검증 시작...")
        
        with duckdb.connect("/data/duckdb/stock_data.db") as conn:
            # 1. 기본 통계
            basic_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM stock_data
            """).fetchone()
            
            # 2. 데이터 완정성 확인
            data_quality = conn.execute("""
                SELECT 
                    COUNT(CASE WHEN open IS NULL THEN 1 END) as null_open,
                    COUNT(CASE WHEN high IS NULL THEN 1 END) as null_high,
                    COUNT(CASE WHEN low IS NULL THEN 1 END) as null_low,
                    COUNT(CASE WHEN close IS NULL THEN 1 END) as null_close,
                    COUNT(CASE WHEN volume IS NULL THEN 1 END) as null_volume
                FROM stock_data
            """).fetchone()
            
            # 3. 논리적 오류 확인
            logical_errors = conn.execute("""
                SELECT 
                    COUNT(CASE WHEN high < low THEN 1 END) as invalid_high_low,
                    COUNT(CASE WHEN open < 0 OR high < 0 OR low < 0 OR close < 0 THEN 1 END) as negative_prices,
                    COUNT(CASE WHEN volume < 0 THEN 1 END) as negative_volume
                FROM stock_data
            """).fetchone()
            
            # 4. 최근 데이터 확인
            recent_data = conn.execute("""
                SELECT COUNT(DISTINCT symbol) as symbols_with_recent_data
                FROM stock_data 
                WHERE date >= current_date - INTERVAL 30 DAY
            """).fetchone()
        
        # 검증 결과 정리
        validation_results = {
            'basic_stats': {
                'total_records': basic_stats[0],
                'unique_symbols': basic_stats[1],
                'date_range': {
                    'start': basic_stats[2],
                    'end': basic_stats[3]
                }
            },
            'data_quality': {
                'null_counts': {
                    'open': data_quality[0],
                    'high': data_quality[1],
                    'low': data_quality[2],
                    'close': data_quality[3],
                    'volume': data_quality[4]
                }
            },
            'logical_errors': {
                'invalid_high_low': logical_errors[0],
                'negative_prices': logical_errors[1],
                'negative_volume': logical_errors[2]
            },
            'recent_data': {
                'symbols_with_recent_data': recent_data[0]
            }
        }
        
        # 품질 기준 확인
        quality_issues = []
        
        if validation_results['basic_stats']['total_records'] < 100000:
            quality_issues.append("총 레코드 수가 너무 적음")
        
        if validation_results['basic_stats']['unique_symbols'] < 100:
            quality_issues.append("수집된 종목 수가 너무 적음")
        
        if validation_results['logical_errors']['invalid_high_low'] > 0:
            quality_issues.append("고가 < 저가인 잘못된 데이터 존재")
        
        if validation_results['logical_errors']['negative_prices'] > 0:
            quality_issues.append("음수 가격 데이터 존재")
        
        print("📊 데이터 품질 검증 결과:")
        print(f"   총 레코드: {validation_results['basic_stats']['total_records']:,}")
        print(f"   종목 수: {validation_results['basic_stats']['unique_symbols']:,}")
        print(f"   날짜 범위: {validation_results['basic_stats']['date_range']['start']} ~ {validation_results['basic_stats']['date_range']['end']}")
        print(f"   최근 데이터 종목: {validation_results['recent_data']['symbols_with_recent_data']}")
        
        if quality_issues:
            print("⚠️ 품질 이슈:")
            for issue in quality_issues:
                print(f"   - {issue}")
        else:
            print("✅ 데이터 품질 검증 통과")
        
        # XCom에 검증 결과 저장
        kwargs['ti'].xcom_push(key='validation_results', value=validation_results)
        kwargs['ti'].xcom_push(key='quality_issues', value=quality_issues)
        
        return validation_results
        
    except Exception as e:
        print(f"❌ 데이터 품질 검증 실패: {e}")
        raise

def create_completion_flag(**kwargs):
    """완료 플래그 생성 및 실행 중 플래그 제거"""
    import json
    from utils.dag_coordination import remove_bulk_collection_running_flag
    
    try:
        # 실행 중 플래그 제거
        remove_bulk_collection_running_flag(**kwargs)
        
        # 모든 통계 정보 수집
        final_stats = kwargs['ti'].xcom_pull(key='final_stats', task_ids='merge_and_optimize_data')
        validation_results = kwargs['ti'].xcom_pull(key='validation_results', task_ids='validate_data_quality')
        
        completion_info = {
            'completion_time': datetime.now().isoformat(),
            'dag_run_id': kwargs['dag_run'].run_id,
            'collection_type': 'bulk_5year_data',
            'final_stats': final_stats,
            'validation_results': validation_results,
            'success': True
        }
        
        # 완료 플래그 파일 생성
        flag_file = "/tmp/nasdaq_bulk_collection_complete.flag"
        with open(flag_file, 'w') as f:
            json.dump(completion_info, f, indent=2, default=str)
        
        print(f"✅ 완료 플래그 생성: {flag_file}")
        return completion_info
        
    except Exception as e:
        print(f"❌ 완료 플래그 생성 실패: {e}")
        raise

# Task 정의
check_resources = PythonOperator(
    task_id='check_system_resources',
    python_callable=check_system_resources,
    provide_context=True,
    dag=dag
)

init_storage = PythonOperator(
    task_id='initialize_optimized_storage',
    python_callable=initialize_optimized_storage,
    provide_context=True,
    dag=dag
)

collect_symbols = PythonOperator(
    task_id='collect_nasdaq_symbols',
    python_callable=collect_nasdaq_symbols_bulk,
    provide_context=True,
    dag=dag
)

# 병렬 데이터 수집 (3개 배치)
collect_batch_1 = PythonOperator(
    task_id='collect_5year_data_batch_1',
    python_callable=collect_5year_data_batch_1,
    provide_context=True,
    dag=dag
)

collect_batch_2 = PythonOperator(
    task_id='collect_5year_data_batch_2',
    python_callable=collect_5year_data_batch_2,
    provide_context=True,
    dag=dag
)

collect_batch_3 = PythonOperator(
    task_id='collect_5year_data_batch_3',
    python_callable=collect_5year_data_batch_3,
    provide_context=True,
    dag=dag
)

# 병합 및 최적화
merge_optimize = PythonOperator(
    task_id='merge_and_optimize_data',
    python_callable=merge_and_optimize_data,
    provide_context=True,
    dag=dag
)

# 데이터 품질 검증
validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag
)

# 완료 플래그 생성
create_flag = PythonOperator(
    task_id='create_completion_flag',
    python_callable=create_completion_flag,
    provide_context=True,
    dag=dag
)

# 성공 알림
success_notification = BashOperator(
    task_id='success_notification',
    bash_command='''
    echo "🎉 나스닥 5년치 대량 데이터 수집 완료!"
    echo "🕐 완료 시간: $(date)"
    echo "💾 저장 위치: /data/duckdb/stock_data.db"
    echo "📈 Parquet 아카이브: /data/parquet_storage/archive/"
    echo "📊 자세한 결과는 Airflow UI에서 확인하세요."
    ''',
    dag=dag
)

# Task 의존성 설정 - 배치 순차 실행으로 DuckDB 락 충돌 방지
check_resources >> init_storage >> collect_symbols >> collect_batch_1 >> collect_batch_2 >> collect_batch_3 >> merge_optimize >> validate_quality >> create_flag >> success_notification

if __name__ == "__main__":
    dag.cli()
