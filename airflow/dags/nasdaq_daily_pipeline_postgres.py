#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# 기본 인수들
default_args = {
    'owner': 'stock-pipeline',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# DAG 정의
dag = DAG(
    'nasdaq_daily_pipeline_postgres',
    default_args=default_args,
    description='NASDAQ 일일 증분 데이터 수집 파이프라인 (PostgreSQL)',
    schedule='0 7 * * 1-5',  # 월~금 오전 7시 실행 (미국 장마감 후)
    catchup=False,
    max_active_runs=1,
    tags=['nasdaq', 'stock', 'spark', 'technical-analysis', 'watchlist', 'kafka', 'postgresql']
)

# 나스닥 심볼 수집
def collect_nasdaq_symbols_func(**kwargs):
    """나스닥 심볼 수집 함수 (PostgreSQL)"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import PostgreSQLManager
    
    print("🏃 나스닥 심볼 수집 시작...")
    
    db = None
    collector = None
    
    try:
        # PostgreSQL 연결
        print("📁 PostgreSQL 데이터베이스 연결...")
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 오늘 이미 수집되었는지 확인
        if db.is_nasdaq_symbols_collected_today():
            print("📅 오늘 이미 NASDAQ 심볼이 수집되었습니다. 건너뜁니다.")
            return "success"
        
        # 대량 데이터 수집기 초기화
        from bulk_data_collector import BulkDataCollector
        
        collector = BulkDataCollector()
        print("✅ BulkDataCollector 초기화 완료")
        
        # 나스닥 심볼 수집
        print("📊 나스닥 심볼 수집 시작...")
        symbols_collected = collector.collect_nasdaq_symbols()
        
        if symbols_collected > 0:
            print(f"✅ 나스닥 심볼 수집 완료: {symbols_collected}개")
            return "success"
        else:
            print("⚠️ 나스닥 심볼 수집 실패")
            return "failed"
            
    except Exception as e:
        print(f"❌ 나스닥 심볼 수집 오류: {e}")
        import traceback
        traceback.print_exc()
        return "failed"
        
    finally:
        if db:
            db.close()

# 주가 데이터 수집
def collect_stock_data_func(**kwargs):
    """주가 데이터 수집 함수 (PostgreSQL)"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import PostgreSQLManager
    from bulk_data_collector import BulkDataCollector
    
    print("🏃 주가 데이터 수집 시작...")
    
    db = None
    collector = None
    
    try:
        # PostgreSQL 연결
        print("📁 PostgreSQL 데이터베이스 연결...")
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 활성 심볼 조회
        symbols = db.get_active_symbols()
        print(f"📊 수집 대상 심볼: {len(symbols)}개")
        
        if not symbols:
            print("⚠️ 수집할 심볼이 없습니다.")
            return "no_symbols"
        
        # 대량 데이터 수집기 초기화
        collector = BulkDataCollector()
        print("✅ BulkDataCollector 초기화 완료")
        
        # 주가 데이터 수집 (최근 2일만 - 증분 수집)
        print("📈 일일 증분 주가 데이터 수집 시작...")
        collected_count = 0
        
        # 전체 심볼 대신 최근 활성 심볼만 (성능 최적화)
        for symbol in symbols[:200]:  # 상위 200개 활성 종목만
            try:
                success = collector.collect_stock_data_single(symbol, days_back=2)  # 최근 2일만
                if success:
                    collected_count += 1
                    print(f"✅ {symbol}: 수집 완료")
                else:
                    print(f"⚠️ {symbol}: 수집 실패")
            except Exception as e:
                print(f"❌ {symbol}: 오류 - {e}")
        
        print(f"✅ 주가 데이터 수집 완료: {collected_count}/{len(symbols[:100])}개")
        return "success"
        
    except Exception as e:
        print(f"❌ 주가 데이터 수집 오류: {e}")
        import traceback
        traceback.print_exc()
        return "failed"
        
    finally:
        if db:
            db.close()

# 기술적 지표 계산
def calculate_technical_indicators_func(**kwargs):
    """기술적 지표 계산 함수 (PostgreSQL)"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import PostgreSQLManager
    from technical_indicator_calculator_postgres import TechnicalIndicatorCalculatorPostgreSQL
    
    print("🏃 기술적 지표 계산 시작...")
    
    db = None
    
    try:
        # PostgreSQL 연결
        print("📁 PostgreSQL 데이터베이스 연결...")
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 활성 심볼 조회
        symbols = db.get_active_symbols()
        print(f"📊 계산 대상 심볼: {len(symbols)}개")
        
        if not symbols:
            print("⚠️ 계산할 심볼이 없습니다.")
            return "no_symbols"
        
        # 기술적 지표 계산기 초기화
        calculator = TechnicalIndicatorCalculatorPostgreSQL()
        print("✅ TechnicalIndicatorCalculator 초기화 완료")
        
        # 기술적 지표 계산
        calculated_count = 0
        
        for symbol in symbols[:50]:  # 배치 크기 제한
            try:
                # 최근 60일 데이터 조회
                stock_data = db.get_stock_data(symbol, days=60)
                
                if stock_data and len(stock_data) >= 20:  # 최소 20일 데이터 필요
                    # 기술적 지표 계산
                    indicators = calculator.calculate_all_indicators(stock_data)
                    
                    # 데이터베이스에 저장
                    for indicator_data in indicators:
                        db.save_technical_indicators(indicator_data)
                    
                    calculated_count += 1
                    print(f"✅ {symbol}: 기술적 지표 계산 완료")
                else:
                    print(f"⚠️ {symbol}: 데이터 부족 (최소 20일 필요)")
                    
            except Exception as e:
                print(f"❌ {symbol}: 기술적 지표 계산 오류 - {e}")
        
        print(f"✅ 기술적 지표 계산 완료: {calculated_count}/{len(symbols[:50])}개")
        return "success"
        
    except Exception as e:
        print(f"❌ 기술적 지표 계산 오류: {e}")
        import traceback
        traceback.print_exc()
        return "failed"
        
    finally:
        if db:
            db.close()

# 관심종목 스캔
def scan_watchlist_func(**kwargs):
    """관심종목 스캔 함수 (PostgreSQL)"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import PostgreSQLManager
    
    print("🏃 관심종목 스캔 시작...")
    
    db = None
    
    try:
        # PostgreSQL 연결
        print("📁 PostgreSQL 데이터베이스 연결...")
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 볼린저 밴드 상단 터치 종목 스캔
        scan_query = """
            SELECT DISTINCT s.symbol, s.close, t.bb_upper, t.bb_middle, t.bb_lower
            FROM stock_data s
            JOIN stock_data_technical_indicators t ON s.symbol = t.symbol AND s.date = t.date
            WHERE s.date = CURRENT_DATE - INTERVAL '1 day'
              AND s.close >= t.bb_upper * 0.99  -- 볼린저 밴드 상단 근처
              AND t.bb_upper IS NOT NULL
            ORDER BY s.symbol
        """
        
        result = db.execute_query(scan_query)
        
        if hasattr(result, 'empty') and not result.empty:
            watchlist_count = len(result)
            print(f"📊 관심종목 발견: {watchlist_count}개")
            
            # 관심종목 데이터베이스에 저장
            from datetime import date
            today = date.today()
            
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    for _, row in result.iterrows():
                        try:
                            cur.execute("""
                                INSERT INTO daily_watchlist (symbol, date, condition_type, condition_value)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (symbol, date, condition_type) DO NOTHING
                            """, (row['symbol'], today, 'bollinger_upper_touch', float(row['close'])))
                        except Exception as e:
                            print(f"⚠️ {row['symbol']}: 관심종목 저장 오류 - {e}")
                    
                    conn.commit()
            
            print(f"✅ 관심종목 스캔 완료: {watchlist_count}개 저장")
        else:
            print("📊 관심종목이 발견되지 않았습니다.")
        
        return "success"
        
    except Exception as e:
        print(f"❌ 관심종목 스캔 오류: {e}")
        import traceback
        traceback.print_exc()
        return "failed"
        
    finally:
        if db:
            db.close()

# 데이터베이스 상태 확인
def check_db_status_func(**kwargs):
    """데이터베이스 상태 확인 함수 (PostgreSQL)"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    from database import PostgreSQLManager
    
    print("🏃 데이터베이스 상태 확인 시작...")
    
    db = None
    
    try:
        # PostgreSQL 연결
        print("📁 PostgreSQL 데이터베이스 연결...")
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 테이블별 레코드 수 확인
        tables = ['nasdaq_symbols', 'stock_data', 'stock_data_technical_indicators', 'daily_watchlist']
        
        for table in tables:
            try:
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cur.fetchone()[0]
                        print(f"📊 {table}: {count:,}개 레코드")
            except Exception as e:
                print(f"❌ {table}: 조회 오류 - {e}")
        
        # 최신 데이터 날짜 확인
        try:
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(date) FROM stock_data")
                    latest_date = cur.fetchone()[0]
                    print(f"📅 최신 주가 데이터 날짜: {latest_date}")
        except Exception as e:
            print(f"❌ 최신 날짜 조회 오류: {e}")
        
        print("✅ 데이터베이스 상태 확인 완료")
        return "success"
        
    except Exception as e:
        print(f"❌ 데이터베이스 상태 확인 오류: {e}")
        import traceback
        traceback.print_exc()
        return "failed"
        
    finally:
        if db:
            db.close()

# Task 정의
collect_nasdaq_symbols = PythonOperator(
    task_id='collect_nasdaq_symbols',
    python_callable=collect_nasdaq_symbols_func,
    dag=dag,
)

collect_stock_data = PythonOperator(
    task_id='collect_stock_data',
    python_callable=collect_stock_data_func,
    dag=dag,
)

calculate_technical_indicators = PythonOperator(
    task_id='calculate_technical_indicators',
    python_callable=calculate_technical_indicators_func,
    dag=dag,
)

scan_watchlist = PythonOperator(
    task_id='scan_watchlist',
    python_callable=scan_watchlist_func,
    dag=dag,
)

check_db_status = PythonOperator(
    task_id='check_db_status',
    python_callable=check_db_status_func,
    dag=dag,
)

# Task 종속성 설정
collect_nasdaq_symbols >> collect_stock_data >> calculate_technical_indicators >> scan_watchlist >> check_db_status
