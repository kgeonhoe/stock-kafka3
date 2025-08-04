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
sys.path.insert(0, '/opt/airflow/common')
sys.path.insert(0, '/opt/airflow/dags')

# 플러그인 임포트
from collect_nasdaq_symbols_api import NasdaqSymbolCollector
from collect_stock_data_yfinance import collect_stock_data_yfinance_task
from technical_indicators import calculate_technical_indicators_task
from database import DuckDBManager
from utils.dag_coordination import BulkCollectionSensor, check_api_rate_limits

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
    description='🚀 나스닥 완전 파이프라인: 수집→분석→스캔→복제 (Kafka Ready)',
    schedule_interval='0 7 * * *',  # 한국시간 오전 7시 (미국 장마감 후)
    catchup=False,
    max_active_runs=1,
    tags=['nasdaq', 'stock', 'spark', 'technical-analysis', 'watchlist', 'kafka']
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
        
        # DB 파일 권한 설정
        if os.path.exists(DB_PATH):
            try:
                os.chmod(DB_PATH, 0o666)
                print("✅ DB 파일 권한 설정 완료 (666)")
            except PermissionError as pe:
                print(f"⚠️ 권한 변경 실패 (무시하고 계속): {pe}")
                current_permissions = oct(os.stat(DB_PATH).st_mode)[-3:]
                print(f"📋 현재 권한으로 진행: {current_permissions}")
        
        # 실제 나스닥 API 수집 시도
        print("🔧 나스닥 API 컬렉터 초기화...")
        collector = NasdaqSymbolCollector()
        
        # 증분 업데이트 확인 - 오늘 이미 수집했는지 체크
        if db.is_nasdaq_symbols_collected_today():
            last_collected = db.get_nasdaq_symbols_last_collected_date()
            print(f"✅ 오늘({last_collected}) 이미 NASDAQ 심볼 수집 완료 - 스킵")
            print("💡 증분 업데이트: 불필요한 API 호출 방지")
            
            # 기존 심볼 조회해서 반환
            existing_symbols_query = "SELECT symbol, name, market_cap, sector FROM nasdaq_symbols"
            existing_df = db.execute_query(existing_symbols_query)
            
            if not existing_df.empty:
                result_symbols = existing_df.to_dict('records')
                print(f"📋 기존 심볼 사용: {len(result_symbols)}개")
                return result_symbols
            else:
                print("⚠️ 기존 심볼이 없어서 새로 수집 진행")
        
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
        # DB 디렉토리 및 파일 권한 확인/설정
        import stat
        db_dir = os.path.dirname(DB_PATH)
        
        # 디렉토리 권한 확인
        if os.path.exists(db_dir):
            dir_permissions = oct(os.stat(db_dir).st_mode)[-3:]
            print(f"🔍 DB 디렉토리 권한: {dir_permissions}")
            
            try:
                os.chmod(db_dir, 0o777)
                print(f"✅ 디렉토리 권한 수정: 777")
            except PermissionError as pe:
                print(f"⚠️ 디렉토리 권한 변경 실패: {pe}")
        else:
            print(f"⚠️ DB 디렉토리가 존재하지 않음: {db_dir}")
            try:
                os.makedirs(db_dir, mode=0o777, exist_ok=True)
                print(f"✅ DB 디렉토리 생성: {db_dir}")
            except Exception as e:
                print(f"⚠️ 디렉토리 생성 실패: {e}")
        
        # DB 파일 권한 확인 및 수정
        if os.path.exists(DB_PATH):
            current_permissions = oct(os.stat(DB_PATH).st_mode)[-3:]
            print(f"🔍 현재 DB 파일 권한: {current_permissions}")
            
            try:
                os.chmod(DB_PATH, 0o666)
                print(f"✅ DB 파일 권한 수정: 666")
            except PermissionError as pe:
                print(f"⚠️ 파일 권한 변경 실패 (무시하고 계속): {pe}")
                print(f"📋 현재 권한으로 진행: {current_permissions}")
        else:
            print(f"⚠️ DB 파일이 존재하지 않음: {DB_PATH}")
            # 빈 파일 생성 시도
            try:
                with open(DB_PATH, 'a'):
                    pass
                os.chmod(DB_PATH, 0o666)
                print(f"✅ DB 파일 생성 및 권한 설정 완료")
            except Exception as e:
                print(f"⚠️ DB 파일 생성 실패: {e}")
        
        # DB 연결 시도
        print(f"🔗 DuckDB 연결 시도: {DB_PATH}")
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

# 4. 관심종목 스캔
def watchlist_scan_func(**kwargs):
    """볼린저 밴드 기반 관심종목 스캔 함수"""
    print("🎯 관심종목 스캔 시작...")
    
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import DuckDBManager
    from technical_scanner import TechnicalScanner
    
    try:
        # DB 연결
        db = DuckDBManager(DB_PATH)
        scanner = TechnicalScanner(db)
        
        # 데이터 가용성 확인
        data_status = scanner.check_data_availability()
        print(f"📊 데이터 상태: {data_status}")
        
        if not data_status.get('has_sufficient_data', False):
            print("⚠️ 충분한 데이터가 없어 스캔을 건너뜁니다.")
            return {'skipped': True, 'reason': 'insufficient_data'}
        
        # 볼린저 밴드 상단 터치 종목 스캔
        print("🔍 볼린저 밴드 상단 터치 종목 스캔 중...")
        bb_upper_stocks = scanner.scan_bollinger_band_upper_touch()
        
        # 결과 정리
        result = {
            'scan_type': 'bollinger_band_upper_touch',
            'total_candidates': len(bb_upper_stocks),
            'candidates': bb_upper_stocks[:10],  # 상위 10개만 로그에 출력
            'timestamp': datetime.now().isoformat(),
            'data_status': data_status
        }
        
        print(f"🎯 관심종목 스캔 완료!")
        print(f"📊 발견된 종목: {len(bb_upper_stocks)}개")
        if bb_upper_stocks:
            print(f"🔥 상위 5개 종목: {[stock['symbol'] for stock in bb_upper_stocks[:5]]}")
        
        db.close()
        return result
        
    except Exception as e:
        print(f"❌ 관심종목 스캔 오류: {e}")
        import traceback
        print(f"📜 상세 오류:\n{traceback.format_exc()}")
        if 'db' in locals():
            db.close()
        raise

# 5. 데이터베이스 복제 및 권한 설정
def create_replica_and_permissions_func(**kwargs):
    """DB 복제본 생성 및 Kafka Producer용 권한 설정 함수"""
    print("🔄 데이터베이스 복제 및 권한 설정 시작...")
    
    import shutil
    import stat
    
    try:
        # 복제본 경로 설정
        replica_path = "/data/duckdb/stock_data_replica.db"
        
        # 1. 기존 복제본 백업 (있다면)
        if os.path.exists(replica_path):
            backup_path = f"{replica_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            try:
                shutil.copy2(replica_path, backup_path)
                print(f"✅ 기존 복제본 백업: {backup_path}")
            except Exception as backup_error:
                print(f"⚠️ 백업 실패 (무시하고 계속): {backup_error}")
        
        # 2. 메인 DB를 복제본으로 복사
        if os.path.exists(DB_PATH):
            print(f"📁 메인 DB 복사: {DB_PATH} → {replica_path}")
            shutil.copy2(DB_PATH, replica_path)
            print("✅ 데이터베이스 복제 완료")
        else:
            print(f"❌ 메인 DB 파일이 없음: {DB_PATH}")
            return {'error': 'main_db_not_found'}
        
        # 3. 복제본 권한 설정 (읽기 전용으로 설정)
        try:
            # 모든 사용자에게 읽기 권한 부여
            os.chmod(replica_path, 0o644)  # rw-r--r--
            print("✅ 복제본 권한 설정 완료 (644 - 읽기 전용)")
            
            # 권한 확인
            permissions = oct(os.stat(replica_path).st_mode)[-3:]
            print(f"🔍 설정된 복제본 권한: {permissions}")
            
        except PermissionError as pe:
            print(f"⚠️ 권한 설정 실패 (무시하고 계속): {pe}")
            # 실패해도 파일은 존재하므로 계속 진행
        
        # 4. 디렉토리 권한도 확인 및 설정
        db_dir = os.path.dirname(replica_path)
        try:
            os.chmod(db_dir, 0o755)  # rwxr-xr-x
            print("✅ DB 디렉토리 권한 설정 완료 (755)")
        except PermissionError as pe:
            print(f"⚠️ 디렉토리 권한 설정 실패: {pe}")
        
        # 5. 파일 크기 및 상태 확인
        if os.path.exists(replica_path):
            file_size = os.path.getsize(replica_path)
            file_size_mb = file_size / (1024 * 1024)
            print(f"📊 복제본 크기: {file_size_mb:.2f} MB")
        
        result = {
            'replica_path': replica_path,
            'file_size_mb': file_size_mb if 'file_size_mb' in locals() else 0,
            'permissions_set': True,
            'timestamp': datetime.now().isoformat()
        }
        
        print("🎉 데이터베이스 복제 및 권한 설정 완료!")
        print(f"📁 복제본 위치: {replica_path}")
        print("🚀 Kafka Producer가 복제본을 읽을 수 있습니다!")
        
        return result
        
    except Exception as e:
        print(f"❌ 복제 및 권한 설정 오류: {e}")
        import traceback
        print(f"📜 상세 오류:\n{traceback.format_exc()}")
        raise
    
# 태스크 정의

# 1. 대량 수집 상태 확인 센서
bulk_collection_sensor = BulkCollectionSensor(
    task_id='wait_for_bulk_collection_completion',
    poke_interval=300,  # 5분마다 확인
    timeout=3600,       # 1시간 타임아웃
    mode='poke',
    dag=dag,
    doc_md="""
    ## 🚦 대량 수집 충돌 방지 센서
    
    **목적**: 나스닥 대량 수집(bulk collection)과의 API 충돌 방지
    
    **동작**:
    - 대량 수집이 실행 중이면 대기
    - 대량 수집이 완료되거나 실행 중이 아니면 진행
    - API 레이트 리미트 보호
    """
)

# 2. API 상태 확인
api_check = PythonOperator(
    task_id='check_api_status',
    python_callable=check_api_rate_limits,
    dag=dag,
    doc_md="""
    ## 🌐 API 상태 확인
    
    **목적**: Yahoo Finance API 레이트 리미트 상태 확인
    """
)

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
    ## 📊 yfinance 기반 주가 데이터 수집 (OHLCV)
    
    **목적**: Yahoo Finance API로 고속 주가 데이터 수집
    
    **처리 과정**:
    1. 저장된 종목 리스트 조회
    2. yfinance API로 5년치 주가 데이터 수집 (1회 호출로 전체 기간)
    3. 데이터 정제 및 검증
    4. DuckDB에 배치 저장
    
    **데이터**: Open, High, Low, Close, Volume (일별)
    **기간**: 5년 (약 1,260 거래일)
    **장점**: KIS API 대비 5-10배 빠른 속도, 장기 트렌드 분석 가능
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

# 관심종목 스캔 태스크
watchlist_scan = PythonOperator(
    task_id='watchlist_scan',
    python_callable=watchlist_scan_func,
    dag=dag,
    doc_md="""
    ## 🎯 관심종목 스캔
    
    **목적**: 기술적 분석을 통한 투자 후보 종목 발굴
    
    **처리 과정**:
    1. 데이터 가용성 확인
    2. 볼린저 밴드 상단 터치 종목 스캔
    3. 조건을 만족하는 종목 리스트 생성
    4. 상위 종목들을 관심종목으로 선정
    
    **스캔 조건**:
    - 볼린저 밴드 상단 근접 터치
    - 충분한 거래량
    - 기술적 지표 조합 분석
    
    **출력**: 관심종목 리스트와 분석 결과
    """,
    retries=2,
    retry_delay=timedelta(minutes=5)
)

# DB 복제 및 권한 설정 태스크
create_replica = PythonOperator(
    task_id='create_replica_and_permissions',
    python_callable=create_replica_and_permissions_func,
    dag=dag,
    doc_md="""
    ## 🔄 데이터베이스 복제 및 권한 설정
    
    **목적**: Kafka Producer용 읽기 전용 DB 복제본 생성
    
    **처리 과정**:
    1. 기존 복제본 백업 (있다면)
    2. 메인 DB를 복제본으로 복사
    3. 복제본 권한 설정 (644 - 읽기 전용)
    4. 디렉토리 권한 설정 (755)
    5. 파일 크기 및 상태 확인
    
    **결과**:
    - stock_data_replica.db 생성
    - Kafka Producer 접근 가능한 권한 설정
    - 실시간 스트리밍을 위한 준비 완료
    """,
    retries=2,
    retry_delay=timedelta(minutes=3)
)

# 파이프라인 완료 플래그 생성
def create_completion_flag_func(**kwargs):
    """파이프라인 완료 플래그 생성"""
    import json
    from datetime import datetime
    
    try:
        # 이전 Task들의 결과 수집
        collected_symbols = kwargs['ti'].xcom_pull(task_ids='collect_nasdaq_symbols')
        collected_ohlcv = kwargs['ti'].xcom_pull(task_ids='collect_stock_data_yfinance')
        calculated_indicators = kwargs['ti'].xcom_pull(task_ids='calculate_technical_indicators_spark')
        scanned_watchlist = kwargs['ti'].xcom_pull(task_ids='watchlist_scan_task')
        
        # 완료 정보 구성
        completion_info = {
            'completion_time': datetime.now().isoformat(),
            'dag_run_id': kwargs['dag_run'].run_id,
            'execution_date': str(kwargs['execution_date']),
            'pipeline_results': {
                'collected_symbols': collected_symbols or 0,
                'collected_ohlcv': collected_ohlcv or 0,
                'calculated_indicators': calculated_indicators or 0,
                'scanned_watchlist': scanned_watchlist or 0
            },
            'status': 'completed',
            'next_steps': ['redis_watchlist_sync', 'realtime_streaming']
        }
        
        # 플래그 파일 생성
        flag_file = "/tmp/nasdaq_pipeline_complete.flag"
        os.makedirs(os.path.dirname(flag_file), exist_ok=True)
        
        with open(flag_file, 'w') as f:
            f.write(json.dumps(completion_info, indent=2))
        
        print("🎉 나스닥 데이터 파이프라인 완료!")
        print(f"📊 처리 결과:")
        print(f"   수집된 심볼: {collected_symbols}개")
        print(f"   수집된 OHLCV: {collected_ohlcv}개")
        print(f"   계산된 지표: {calculated_indicators}개")
        print(f"   스캔된 관심종목: {scanned_watchlist}개")
        print(f"✅ 완료 플래그 생성: {flag_file}")
        
        # Redis 동기화 DAG 자동 트리거
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator
        print("🔄 Redis 관심종목 동기화 DAG 트리거 준비 완료")
        
        return completion_info
        
    except Exception as e:
        print(f"❌ 완료 플래그 생성 실패: {e}")
        raise

create_completion_flag = PythonOperator(
    task_id='create_completion_flag',
    python_callable=create_completion_flag_func,
    dag=dag,
    doc_md="""
    ## 🏁 파이프라인 완료 플래그 생성
    
    **목적**: 나스닥 데이터 파이프라인 완료를 다른 DAG에 알림
    
    **처리 과정**:
    1. 모든 이전 Task의 결과 수집
    2. 완료 정보를 JSON으로 구성
    3. /tmp/nasdaq_pipeline_complete.flag 파일 생성
    4. Redis 동기화 DAG 트리거 준비
    
    **출력 파일 내용**:
    - 완료 시간
    - 처리된 데이터 통계
    - 다음 단계 정보
    
    **연결 DAG**: redis_watchlist_sync (자동 트리거)
    """,
    retries=1,
    retry_delay=timedelta(minutes=2)
)

# Redis 동기화 DAG 자동 트리거
trigger_redis_sync = TriggerDagRunOperator(
    task_id='trigger_redis_watchlist_sync',
    trigger_dag_id='redis_watchlist_sync',
    conf={
        'triggered_by': 'nasdaq_daily_pipeline',
        'trigger_time': '{{ ts }}',
        'execution_date': '{{ ds }}'
    },
    dag=dag,
    doc_md="""
    ## 🔄 Redis 동기화 DAG 트리거
    
    **목적**: 메인 파이프라인 완료 후 자동으로 Redis 동기화 실행
    
    **트리거 조건**: 모든 데이터 처리 및 복제 완료 후
    **대상 DAG**: redis_watchlist_sync
    **전달 정보**: 실행 시간, 트리거 소스
    """
)

# 태스크 의존성 설정 - 완전한 파이프라인
# Task 의존성 설정 - 대량 수집 충돌 방지 포함
bulk_collection_sensor >> api_check >> collect_symbols >> collect_ohlcv >> calculate_indicators >> watchlist_scan >> create_replica >> create_completion_flag >> trigger_redis_sync
