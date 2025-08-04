#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
통합 주식 데이터 수집 DAG (하이브리드 방식)
- 초기 수집: FinanceDataReader로 5년치 대량 수집 (1회 실행)
- 일별 운영: yfinance로 증분 수집 (매일 실행)
- 완벽한 데이터 일관성 보장
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
import os

# 프로젝트 경로 추가
sys.path.insert(0, '/opt/airflow/common')
sys.path.insert(0, '/home/grey1/stock-kafka3/common')

# DAG 기본 설정
default_args = {
    'owner': 'stock-pipeline',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hybrid_stock_data_collection',
    default_args=default_args,
    description='하이브리드 주식 데이터 수집 (FDR + yfinance)',
    schedule_interval='0 2 * * 1-5',  # 평일 오전 2시
    catchup=False,
    max_active_runs=1,
    tags=['stock', 'data-collection', 'hybrid']
)

# 데이터베이스 경로
DB_PATH = "/data/duckdb/stock_data.db"

def check_initial_data_exists(**context):
    """
    초기 데이터 존재 여부 확인
    
    Returns:
        'bulk_collection' 또는 'daily_collection'
    """
    try:
        import duckdb
        
        with duckdb.connect(DB_PATH) as conn:
            # 데이터베이스 파일 존재 및 데이터 확인
            try:
                result = conn.execute("""
                    SELECT COUNT(DISTINCT symbol), MIN(date), MAX(date)
                    FROM stock_data
                """).fetchone()
                
                symbol_count = result[0] if result else 0
                min_date = result[1] if result else None
                max_date = result[2] if result else None
                
                print(f"🔍 기존 데이터 확인: {symbol_count}개 종목, {min_date} ~ {max_date}")
                
                # 충분한 데이터가 있으면 일별 수집으로 분기
                if symbol_count >= 1:  # 임시로 1개 종목만 있어도 daily collection 실행
                    print("✅ 기존 데이터가 있음 -> 일별 증분 수집 실행")
                    return 'daily_incremental_collection'
                else:
                    print("⚠️ 기존 데이터 부족 -> 대량 수집 실행")
                    return 'bulk_collection_fdr'
                    
            except Exception as e:
                print(f"🆕 새 데이터베이스 -> 대량 수집 실행: {e}")
                return 'bulk_collection_fdr'
                
    except Exception as e:
        print(f"❌ 데이터베이스 확인 실패 -> 대량 수집 실행: {e}")
        return 'bulk_collection_fdr'

def bulk_collection_fdr_task(**context):
    """
    FinanceDataReader로 5년치 대량 수집
    """
    from bulk_data_collector import BulkDataCollector
    
    print("🚀 FinanceDataReader 대량 수집 시작 (5년치 데이터)")
    
    try:
        # 대량 수집기 초기화
        collector = BulkDataCollector(
            db_path=DB_PATH,
            batch_size=1000,
            max_workers=4
        )
        
        # 나스닥 종목 리스트 가져오기
        symbols = collector.get_nasdaq_symbols(limit=None)  # 전체 종목
        
        if not symbols:
            raise ValueError("나스닥 종목 리스트를 가져올 수 없습니다")
        
        print(f"📊 수집 대상: {len(symbols)}개 종목")
        
        # 대량 데이터 수집 (5년치)
        success = collector.collect_bulk_data(
            symbols=symbols,
            start_date='2019-01-01',  # 5년치
            batch_symbols=50
        )
        
        if not success:
            raise ValueError("대량 데이터 수집 실패")
        
        # 데이터베이스 최적화
        print("🔧 데이터베이스 최적화 중...")
        collector.optimize_database()
        
        # 통계 확인
        stats = collector.get_collection_stats()
        print(f"📈 수집 완료 통계: {stats}")
        
        # XCom에 결과 저장
        context['task_instance'].xcom_push(
            key='bulk_collection_result',
            value={
                'status': 'success',
                'symbols_collected': len(symbols),
                'statistics': stats
            }
        )
        
        print("✅ FinanceDataReader 대량 수집 완료")
        
    except Exception as e:
        print(f"❌ FinanceDataReader 대량 수집 실패: {e}")
        raise
    finally:
        if 'collector' in locals():
            collector.close()

def daily_incremental_collection_task(**context):
    """
    yfinance로 일별 증분 수집
    """
    from daily_incremental_collector import DailyIncrementalCollector
    
    print("🔄 yfinance 일별 증분 수집 시작")
    
    try:
        # 증분 수집기 초기화
        collector = DailyIncrementalCollector(
            db_path=DB_PATH,
            max_workers=5,
            batch_size=50
        )
        
        # 일별 배치 수집 실행
        result = collector.collect_daily_batch()
        
        if result['status'] != 'completed':
            raise ValueError(f"일별 수집 실패: {result.get('reason', 'unknown')}")
        
        print(f"📊 일별 수집 결과: {result['symbols_success']}/{result['symbols_attempted']} 성공")
        
        # 통계 확인
        stats = collector.get_collection_stats()
        print(f"📈 데이터베이스 통계: {stats}")
        
        # XCom에 결과 저장
        context['task_instance'].xcom_push(
            key='daily_collection_result',
            value={
                'status': 'success',
                'collection_stats': result,
                'database_stats': stats
            }
        )
        
        print("✅ yfinance 일별 증분 수집 완료")
        
    except Exception as e:
        print(f"❌ yfinance 일별 증분 수집 실패: {e}")
        raise
    finally:
        if 'collector' in locals():
            collector.close()

def validate_data_consistency_task(**context):
    """
    데이터 일관성 검증
    """
    import duckdb
    
    print("🔍 데이터 일관성 검증 시작")
    
    try:
        with duckdb.connect(DB_PATH) as conn:
            # 1. 기본 통계
            basic_stats = conn.execute("""
                SELECT 
                    COUNT(DISTINCT symbol) as symbol_count,
                    COUNT(*) as total_records,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM stock_data
            """).fetchone()
            
            print(f"📊 기본 통계: {basic_stats[0]}개 종목, {basic_stats[1]}개 레코드")
            print(f"📅 날짜 범위: {basic_stats[2]} ~ {basic_stats[3]}")
            
            # 2. 데이터 품질 검사
            quality_check = conn.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN close IS NULL OR close <= 0 THEN 1 END) as invalid_close,
                    COUNT(CASE WHEN volume < 0 THEN 1 END) as invalid_volume,
                    COUNT(CASE WHEN open > high OR low > close THEN 1 END) as invalid_ohlc
                FROM stock_data
            """).fetchone()
            
            print(f"📋 데이터 품질: 총 {quality_check[0]}개 중")
            print(f"   - 잘못된 종가: {quality_check[1]}개")
            print(f"   - 잘못된 거래량: {quality_check[2]}개") 
            print(f"   - 잘못된 OHLC: {quality_check[3]}개")
            
            # 3. 최신 데이터 확인
            recent_data = conn.execute("""
                SELECT symbol, MAX(date) as last_date
                FROM stock_data
                GROUP BY symbol
                ORDER BY last_date DESC
                LIMIT 5
            """).fetchall()
            
            print("📈 최신 데이터 샘플:")
            for symbol, last_date in recent_data:
                print(f"   - {symbol}: {last_date}")
            
            # 4. 누락 데이터 확인
            missing_check = conn.execute("""
                SELECT 
                    symbol,
                    COUNT(*) as record_count,
                    MAX(date) as last_date
                FROM stock_data
                GROUP BY symbol
                HAVING MAX(date) < CURRENT_DATE - INTERVAL '2 days'
                ORDER BY last_date
                LIMIT 10
            """).fetchall()
            
            if missing_check:
                print("⚠️ 최신 데이터가 누락된 종목들:")
                for symbol, count, last_date in missing_check:
                    print(f"   - {symbol}: {count}개 레코드, 마지막 데이터 {last_date}")
            
            # 검증 결과
            validation_result = {
                'basic_stats': {
                    'symbol_count': basic_stats[0],
                    'total_records': basic_stats[1],
                    'date_range': f"{basic_stats[2]} ~ {basic_stats[3]}"
                },
                'quality_issues': {
                    'invalid_close': quality_check[1],
                    'invalid_volume': quality_check[2],
                    'invalid_ohlc': quality_check[3]
                },
                'missing_data_symbols': len(missing_check)
            }
            
            # XCom에 결과 저장
            context['task_instance'].xcom_push(
                key='validation_result',
                value=validation_result
            )
            
            print("✅ 데이터 일관성 검증 완료")
            
    except Exception as e:
        print(f"❌ 데이터 일관성 검증 실패: {e}")
        raise

def create_data_replica_task(**context):
    """
    읽기 전용 데이터 복제본 생성
    """
    import shutil
    
    print("📄 읽기 전용 데이터 복제본 생성 중...")
    
    try:
        # 복제본 경로
        replica_path = DB_PATH.replace('.db', '_replica.db')
        
        # 기존 복제본 제거
        if os.path.exists(replica_path):
            os.remove(replica_path)
        
        # 파일 복사
        shutil.copy2(DB_PATH, replica_path)
        
        # 읽기 전용 권한 설정
        os.chmod(replica_path, 0o444)
        
        print(f"✅ 복제본 생성 완료: {replica_path}")
        
        # XCom에 결과 저장
        context['task_instance'].xcom_push(
            key='replica_path',
            value=replica_path
        )
        
    except Exception as e:
        print(f"❌ 복제본 생성 실패: {e}")
        raise

# 태스크 정의
check_data = BranchPythonOperator(
    task_id='check_initial_data',
    python_callable=check_initial_data_exists,
    dag=dag,
    doc_md="""
    ## 🔍 초기 데이터 확인
    
    **목적**: 기존 데이터 존재 여부에 따라 수집 방식 결정
    
    **분기 로직**:
    - 기존 데이터 충분 (100개 이상 종목) → 일별 증분 수집
    - 기존 데이터 부족 또는 없음 → FinanceDataReader 대량 수집
    
    **출력**: 'bulk_collection_fdr' 또는 'daily_incremental_collection'
    """
)

bulk_collection = PythonOperator(
    task_id='bulk_collection_fdr',
    python_callable=bulk_collection_fdr_task,
    dag=dag,
    doc_md="""
    ## 🚀 FinanceDataReader 대량 수집
    
    **목적**: 5년치 과거 데이터를 고속으로 대량 수집
    
    **처리 과정**:
    1. 나스닥 전체 종목 리스트 수집
    2. 5년치 OHLCV 데이터 병렬 수집
    3. yfinance 호환 형식으로 변환 저장
    4. 데이터베이스 최적화 실행
    
    **특징**: Adjusted Close 기준으로 yfinance와 완벽 호환
    """
)

daily_collection = PythonOperator(
    task_id='daily_incremental_collection',
    python_callable=daily_incremental_collection_task,
    dag=dag,
    doc_md="""
    ## 🔄 yfinance 일별 증분 수집
    
    **목적**: 기존 데이터에 최신 데이터를 증분 추가
    
    **처리 과정**:
    1. 기존 종목별 마지막 데이터 날짜 확인
    2. 누락된 날짜부터 현재까지 yfinance로 수집
    3. 중복 제거 후 데이터베이스 업데이트
    
    **특징**: auto_adjust=True로 기존 FDR 데이터와 일관성 보장
    """
)

validate_data = PythonOperator(
    task_id='validate_data_consistency',
    python_callable=validate_data_consistency_task,
    dag=dag,
    trigger_rule='none_failed_or_skipped',
    doc_md="""
    ## 🔍 데이터 일관성 검증
    
    **목적**: 수집된 데이터의 품질 및 일관성 검증
    
    **검증 항목**:
    - 기본 통계 (종목 수, 레코드 수, 날짜 범위)
    - 데이터 품질 (잘못된 가격, 거래량 등)
    - 최신성 (누락된 최신 데이터)
    - OHLC 논리 검증
    """
)

create_replica = PythonOperator(
    task_id='create_data_replica',
    python_callable=create_data_replica_task,
    dag=dag,
    doc_md="""
    ## 📄 읽기 전용 복제본 생성
    
    **목적**: Kafka Producer 등 읽기 전용 접근을 위한 DB 복제본 생성
    
    **처리 과정**:
    1. 메인 데이터베이스 파일 복사
    2. 읽기 전용 권한 설정
    3. 동시성 문제 방지
    """
)

# 더미 태스크 (분기 종료용)
end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
    trigger_rule='none_failed_or_skipped'
)

# 태스크 의존성 설정
check_data >> [bulk_collection, daily_collection]
bulk_collection >> validate_data
daily_collection >> validate_data
validate_data >> create_replica >> end_task
