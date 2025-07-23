#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os

# 플러그인 경로 추가
sys.path.insert(0, '/opt/airflow/plugins')

# 플러그인 임포트
from collect_nasdaq_symbols_api import NasdaqSymbolCollector
from collect_stock_data_yfinance import collect_stock_data_yfinance_task
from technical_indicators import calculate_technical_indicators_task

# 기본 인수 설정
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'nasdaq_daily_pipeline',
    default_args=default_args,
    description='🚀 나스닥 일일 데이터 수집 파이프라인 (Spark 기반)',
    schedule_interval='0 7 * * *',  # 한국시간 오전 7시 (미국 장마감 후)
    catchup=False,
    max_active_runs=1,
    tags=['nasdaq', 'stock', 'spark', 'technical-analysis']
)

# 데이터베이스 경로
DB_PATH = "/data/duckdb/stock_data.db"

# 1. 나스닥 심볼 수집
def collect_nasdaq_symbols_func(**kwargs):
    """나스닥 심볼 수집 함수"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import DuckDBManager
    
    print("🏃 나스닥 심볼 수집 시작...")
    
    db = None
    collector = None
    
    try:
        # DuckDB 연결 (새 파일 생성)
        print(f"📁 데이터베이스 연결: {DB_PATH}")
        db = DuckDBManager(DB_PATH)
        print("✅ DuckDB 연결 성공")
        
        # 실제 나스닥 API 수집 시도
        print("🔧 나스닥 API 컬렉터 초기화...")
        collector = NasdaqSymbolCollector()
        
        try:
            print("📊 나스닥 API에서 종목 데이터 수집 중...")
            symbols = collector.collect_symbols()
            
            if symbols and len(symbols) > 0:
                print(f"📈 API에서 수집된 심볼 수: {len(symbols)}개")
                
                # 첫 번째 종목 샘플 데이터 확인
                if len(symbols) > 0:
                    sample = symbols[0]
                    print(f"🔍 샘플 데이터 구조: {sample}")
                    print(f"🔍 사용 가능한 키들: {list(sample.keys())}")
                
                # 전체 종목 저장 (필터링 없음)
                print("💾 전체 종목을 데이터베이스에 저장 중...")
                db.save_nasdaq_symbols(symbols)
                
                result_symbols = symbols
                print(f"✅ 전체 {len(symbols)}개 종목 저장 완료")
            else:
                raise Exception("API에서 데이터를 가져올 수 없음")
                
        except Exception as api_error:
            print(f"⚠️ API 수집 실패: {api_error}")
            print("🔄 백업 데이터로 전환...")
            
            # 백업용 주요 종목 데이터 (실제 대형주들)
            backup_symbols = [
                {'symbol': 'AAPL', 'name': 'Apple Inc.', 'marketCap': '3500000000000', 'sector': 'Technology'},
                {'symbol': 'MSFT', 'name': 'Microsoft Corporation', 'marketCap': '3000000000000', 'sector': 'Technology'},
                {'symbol': 'GOOGL', 'name': 'Alphabet Inc.', 'marketCap': '2000000000000', 'sector': 'Technology'},
                {'symbol': 'AMZN', 'name': 'Amazon.com Inc.', 'marketCap': '1800000000000', 'sector': 'Consumer Discretionary'},
                {'symbol': 'TSLA', 'name': 'Tesla Inc.', 'marketCap': '800000000000', 'sector': 'Consumer Discretionary'},
                {'symbol': 'META', 'name': 'Meta Platforms Inc.', 'marketCap': '900000000000', 'sector': 'Technology'},
                {'symbol': 'NVDA', 'name': 'NVIDIA Corporation', 'marketCap': '1200000000000', 'sector': 'Technology'},
                {'symbol': 'NFLX', 'name': 'Netflix Inc.', 'marketCap': '200000000000', 'sector': 'Communication Services'},
                {'symbol': 'AMD', 'name': 'Advanced Micro Devices', 'marketCap': '250000000000', 'sector': 'Technology'},
                {'symbol': 'INTC', 'name': 'Intel Corporation', 'marketCap': '180000000000', 'sector': 'Technology'}
            ]
            
            print(f"📈 백업 심볼 수: {len(backup_symbols)}개")
            db.save_nasdaq_symbols(backup_symbols)
            result_symbols = backup_symbols
        
        print("✅ 데이터베이스 저장 완료")
        
        # 저장된 데이터 확인
        print("🔍 저장된 데이터 확인...")
        saved_symbols = db.get_active_symbols()
        print(f"📊 저장된 심볼: {saved_symbols}")
        
        # 수집 결과 반환
        result = {
            'total_symbols': len(result_symbols),
            'filtered_symbols': len(result_symbols),
            'saved_symbols': len(saved_symbols),
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"🎉 나스닥 심볼 수집 완료!")
        print(f"📊 결과: {result}")
        
        if db:
            db.close()
        return result
    
    except Exception as e:
        print(f"❌ 심볼 수집 오류: {e}")
        print(f"🔍 오류 타입: {type(e).__name__}")
        import traceback
        print(f"📜 상세 오류:\n{traceback.format_exc()}")
        
        if db:
            db.close()
        raise
        
# 2. 주가 데이터 수집
def collect_ohlcv_func(**kwargs):
    """yfinance 기반 주가 데이터 수집 함수"""
    print("📊 yfinance 기반 주가 데이터 수집 시작...")
    
    # 디버깅: DuckDB에서 심볼 조회
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import DuckDBManager
    
    try:
        # DB에서 심볼 확인
        db = DuckDBManager(DB_PATH)
        saved_symbols = db.get_active_symbols()
        print(f"🔍 DB에서 조회된 심볼: {saved_symbols}")
        print(f"🔍 심볼 타입: {type(saved_symbols)}")
        print(f"🔍 심볼 개수: {len(saved_symbols) if saved_symbols else 0}")
        
        if saved_symbols and len(saved_symbols) > 0:
            print(f"🔍 첫 번째 심볼: {saved_symbols[0]}")
            print(f"🔍 첫 번째 심볼 타입: {type(saved_symbols[0])}")
        
        db.close()
        
        # 원래 함수 호출
        result = collect_stock_data_yfinance_task(**kwargs)
        print(f"✅ yfinance 주가 데이터 수집 완료: {result}")
        return result
    except Exception as e:
        print(f"❌ yfinance 주가 데이터 수집 오류: {e}")
        import traceback
        print(f"📜 상세 오류:\n{traceback.format_exc()}")
        raise

# 3. 기술적 지표 계산
def calculate_indicators_func(**kwargs):
    """기술적 지표 계산 함수"""
    print("🚀 Spark 기반 기술적 지표 계산 시작...")
    
    try:
        result = calculate_technical_indicators_task(**kwargs)
        print(f"🎉 기술적 지표 계산 완료: {result}")
        return result
    except Exception as e:
        print(f"❌ 기술적 지표 계산 오류: {e}")
        import traceback
        print(f"📜 상세 오류:\n{traceback.format_exc()}")
        raise
    
# 태스크 정의
collect_symbols = PythonOperator(
    task_id='collect_nasdaq_symbols',
    python_callable=collect_nasdaq_symbols_func,
    dag=dag,
    doc_md="""
    ## 📊 나스닥 심볼 수집
    
    **목적**: 나스닥 거래소의 전체 활성 종목 정보 수집
    
    **처리 과정**:
    1. 나스닥 API에서 전체 종목 정보 조회
    2. 전체 종목을 DuckDB에 저장 (필터링 없음)
    3. 약 7000개 종목 수집
    
    **출력**: 수집된 종목 수, 저장된 종목 수
    """,
    retries=2,
    retry_delay=timedelta(minutes=3)
)

collect_ohlcv = PythonOperator(
    task_id='collect_ohlcv_data',
    python_callable=collect_ohlcv_func,
    dag=dag,
    doc_md="""
    ## � yfinance 기반 주가 데이터 수집 (OHLCV)
    
    **목적**: Yahoo Finance API로 고속 주가 데이터 수집
    
    **처리 과정**:
    1. 저장된 종목 리스트 조회
    2. yfinance API로 1년치 주가 데이터 수집 (1회 호출로 전체 기간)
    3. 데이터 정제 및 검증
    4. DuckDB에 배치 저장
    
    **데이터**: Open, High, Low, Close, Volume (일별)
    **기간**: 1년 (252 거래일)
    **장점**: KIS API 대비 5-10배 빠른 속도
    """,
    retries=3,
    retry_delay=timedelta(minutes=5)
)

calculate_indicators = PythonOperator(
    task_id='calculate_technical_indicators',
    python_callable=calculate_indicators_func,
    dag=dag,
    doc_md="""
    ## 🚀 Spark 기반 기술적 지표 계산
    
    **목적**: 고성능 분산 처리로 기술적 지표 계산
    
    **처리 과정**:
    1. DuckDB에서 주가 데이터 로드
    2. Spark DataFrame으로 변환
    3. 7가지 기술적 지표 병렬 계산:
       - 이동평균선 (SMA): 5, 20, 60, 112, 224, 448일
       - 지수이동평균 (EMA): 5, 20, 60일
       - 볼린저 밴드 (BB): 20일 ±2σ
       - MACD: 12-26-9 설정
       - RSI: 14일
       - CCI: 20일
       - OBV: 거래량 기반
    4. 결과를 DuckDB에 저장
    
    **성능**: Spark 분산 처리로 고속 계산
    """,
    retries=2,
    retry_delay=timedelta(minutes=10)
)

# 모든 작업 완료 후 관심종목 스캔 트리거
trigger_watchlist_analysis = TriggerDagRunOperator(
    task_id='trigger_daily_watchlist',
    trigger_dag_id='daily_watchlist_scanner',
    wait_for_completion=False,  # 완료까지 기다리지 않음
    dag=dag,
    doc_md="""
    ## 관심종목 분석 트리거
    
    - 데이터 수집 및 기술적 지표 계산 완료 후
    - 자동으로 관심종목 스캔 DAG 실행
    - 볼린저 밴드 상단 터치 종목 스캔
    """,
    retries=1
)

# 태스크 의존성 설정
collect_symbols >> collect_ohlcv >> calculate_indicators >> trigger_watchlist_analysis
