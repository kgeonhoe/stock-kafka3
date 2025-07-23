#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys
import os

# 환경에 따라 모듈 경로 설정
if '/opt/airflow/common' not in sys.path:
    sys.path.insert(0, '/opt/airflow/common')
if '/home/grey1/stock-kafka3/common' not in sys.path:
    sys.path.insert(0, '/home/grey1/stock-kafka3/common')

from technical_scanner import TechnicalScanner

def scan_and_update_watchlist(**context):
    """볼린저 밴드 상단 터치 종목 스캔 및 관심종목 업데이트"""
    
    # 스캔 날짜 (어제 날짜 사용 - 장마감 후 처리)
    scan_date = (datetime.now() - timedelta(days=1)).date()
    
    try:
        # 기술적 스캐너 초기화
        scanner = TechnicalScanner(db_path="/data/duckdb/stock_data.db")
        
        # 볼린저 밴드 상단 터치 종목 스캔
        watchlist_signals = scanner.update_daily_watchlist(scan_date)
        
        print(f"📈 {scan_date} 볼린저 밴드 상단 터치 종목: {len(watchlist_signals)}개")
        
        # 결과 상세 출력
        for signal in watchlist_signals[:10]:  # 상위 10개만 출력
            print(f"  - {signal['symbol']}: ${signal['close_price']:.2f} (상단선 대비 {signal['condition_value']:.3f})")
        
        # XCom에 결과 저장
        context['task_instance'].xcom_push(key='watchlist_count', value=len(watchlist_signals))
        context['task_instance'].xcom_push(key='scan_date', value=str(scan_date))
        
        return f"✅ 관심종목 스캔 완료: {len(watchlist_signals)}개"
        
    except Exception as e:
        print(f"❌ 관심종목 스캔 실패: {str(e)}")
        raise

def cleanup_old_watchlist(**context):
    """30일 이전 관심종목 데이터 정리"""
    
    try:
        scanner = TechnicalScanner(db_path="/data/duckdb/stock_data.db")
        
        # 30일 이전 데이터 삭제
        cutoff_date = (datetime.now() - timedelta(days=30)).date()
        
        result = scanner.db.conn.execute("""
            DELETE FROM daily_watchlist 
            WHERE date < ?
        """, (cutoff_date,))
        
        deleted_count = result.fetchone()[0] if result else 0
        scanner.db.conn.commit()
        
        print(f"🧹 {cutoff_date} 이전 관심종목 데이터 {deleted_count}개 삭제")
        
        return f"✅ 오래된 데이터 정리 완료: {deleted_count}개 삭제"
        
    except Exception as e:
        print(f"❌ 데이터 정리 실패: {str(e)}")
        raise

def send_watchlist_summary(**context):
    """관심종목 요약 정보 출력"""
    
    # 이전 태스크에서 결과 가져오기
    watchlist_count = context['task_instance'].xcom_pull(task_ids='scan_watchlist', key='watchlist_count')
    scan_date = context['task_instance'].xcom_pull(task_ids='scan_watchlist', key='scan_date')
    
    print(f"""
    📊 일별 관심종목 스캔 결과 ({scan_date})
    ================================
    🎯 볼린저 밴드 상단 터치: {watchlist_count}개
    📅 스캔 날짜: {scan_date}
    ⏰ 처리 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """)
    
    return f"요약 전송 완료"

# DAG 기본 설정
default_args = {
    'owner': 'stock-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'daily_watchlist_scanner',
    default_args=default_args,
    description='일별 기술적 지표 기반 관심종목 스캔 (nasdaq_daily_pipeline 의존)',
    schedule_interval=None,  # ExternalTaskSensor로 트리거되므로 None
    catchup=False,
    max_active_runs=1,
    tags=['stock', 'technical-analysis', 'watchlist', 'dependent']
)

# ⭐ DAG 간 의존성: nasdaq 데이터 수집 완료 대기
wait_for_nasdaq_data = ExternalTaskSensor(
    task_id='wait_for_nasdaq_data_collection',
    external_dag_id='nasdaq_daily_pipeline',
    external_task_id='collect_ohlcv_data',  # nasdaq_daily_pipeline의 주가 데이터 수집 태스크
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed'],
    timeout=7200,  # 2시간 타임아웃
    poke_interval=300,  # 5분마다 체크
    dag=dag,
    doc_md="""
    ## NASDAQ 데이터 수집 완료 대기
    
    - nasdaq_daily_pipeline DAG의 주가 데이터 수집이 완료된 후 실행
    - 최신 주식 데이터가 준비되면 기술적 스캔 시작
    - 타임아웃: 2시간, 체크 간격: 5분
    """
)

# 기술적 지표 계산 완료 대기 (추가 의존성)
wait_for_technical_indicators = ExternalTaskSensor(
    task_id='wait_for_technical_calculation',
    external_dag_id='nasdaq_daily_pipeline',
    external_task_id='calculate_technical_indicators',  # 기술적 지표 계산 태스크
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed'],
    timeout=3600,  # 1시간 타임아웃
    poke_interval=180,  # 3분마다 체크
    dag=dag,
    doc_md="""
    ## 기술적 지표 계산 완료 대기
    
    - nasdaq_daily_pipeline DAG의 기술적 지표 계산이 완료된 후 실행
    - RSI, 볼린저 밴드 등 기술적 지표가 준비되면 스캔 시작
    - 타임아웃: 1시간, 체크 간격: 3분
    """
)

# 태스크 정의
scan_task = PythonOperator(
    task_id='scan_watchlist',
    python_callable=scan_and_update_watchlist,
    dag=dag,
    doc_md="""
    ## 볼린저 밴드 상단 터치 종목 스캔
    
    - 전일 종가 기준으로 볼린저 밴드 상단선 98% 이상 터치한 종목 검색
    - 시가총액별 티어 분류 (대형주: 1, 중형주: 2, 소형주: 3)
    - daily_watchlist 테이블에 결과 저장
    """
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_watchlist,
    dag=dag,
    doc_md="""
    ## 오래된 관심종목 데이터 정리
    
    - 30일 이전 관심종목 데이터 삭제
    - 데이터베이스 용량 관리
    """
)

summary_task = PythonOperator(
    task_id='send_summary',
    python_callable=send_watchlist_summary,
    dag=dag,
    doc_md="""
    ## 스캔 결과 요약
    
    - 스캔 결과 통계 출력
    - 로그에 요약 정보 기록
    """
)

# 태스크 의존성 설정: 두 센서가 모두 완료된 후 스캔 실행
[wait_for_nasdaq_data, wait_for_technical_indicators] >> scan_task >> cleanup_task >> summary_task
