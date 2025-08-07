#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enhanced NASDAQ Bulk Data Collection DAG - PostgreSQL Version
대량 NASDAQ 데이터 수집 및 처리 파이프라인
"""

import os
import sys
from datetime import datetime, timedelta, date
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

# 기본 설정
default_args = {
    'owner': 'stock-pipeline',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': days_ago(1),
}

# DAG 정의
dag = DAG(
    'enhanced_nasdaq_bulk_collection_postgres',
    default_args=default_args,
    description='NASDAQ 전체 데이터 수집 (주식분할/배당 대응, PostgreSQL)',
    schedule_interval='0 7 * * *',  # 평일 오전 7시 실행 (daily bulk collection)
    catchup=False,
    max_active_runs=1,
    tags=['nasdaq', 'bulk-collection', 'postgresql', 'stock-data']
)

def collect_nasdaq_symbols_task(**kwargs):
    """NASDAQ 심볼 대량 수집"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    
    from database import PostgreSQLManager
    from bulk_data_collector import BulkDataCollector
    
    print("🚀 NASDAQ 심볼 대량 수집 시작...")
    
    try:
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 오늘 이미 수집되었는지 확인
        if db.is_nasdaq_symbols_collected_today():
            print("📅 오늘 이미 NASDAQ 심볼이 수집되었습니다.")
            return {"status": "already_collected", "count": 0}
        
        # 대량 수집기 초기화 (FinanceDataReader 사용으로 배치 크기 증가 가능)
        collector = BulkDataCollector(batch_size=200, max_workers=4)
        
        # NASDAQ 심볼 수집
        symbols_count = collector.collect_nasdaq_symbols()
        
        if symbols_count > 0:
            print(f"✅ NASDAQ 심볼 수집 완료: {symbols_count:,}개")
            return {"status": "success", "count": symbols_count}
        else:
            print("⚠️ 수집된 심볼이 없습니다.")
            return {"status": "no_data", "count": 0}
            
    except Exception as e:
        print(f"❌ NASDAQ 심볼 수집 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'db' in locals():
            db.close()
        if 'collector' in locals():
            collector.close()

def bulk_collect_stock_data_task(**kwargs):
    """주가 데이터 대량 수집"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    
    from database import PostgreSQLManager
    from bulk_data_collector import BulkDataCollector
    
    print("🚀 주가 데이터 대량 수집 시작...")
    
    try:
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 활성 심볼 조회 (전체)
        symbols = db.get_active_symbols()  # 전체 나스닥 심볼 수집
        print(f"📊 수집 대상 심볼: {len(symbols):,}개")
        
        if not symbols:
            print("⚠️ 수집할 심볼이 없습니다.")
            return {"status": "no_symbols", "success": 0, "failed": 0}
        
        # 대량 수집기 초기화 (FinanceDataReader 사용으로 배치 크기 증가 가능)
        collector = BulkDataCollector(batch_size=50, max_workers=4)
        
        # 배치 주가 데이터 수집 (과거 5년 데이터 - FinanceDataReader 사용)
        success_count, fail_count = collector.collect_stock_data_batch(
            symbols=symbols, 
            days_back=1825  # 5년 * 365일
        )
        
        print(f"✅ 주가 데이터 수집 완료:")
        print(f"  - 성공: {success_count:,}개")
        print(f"  - 실패: {fail_count:,}개")
        print(f"  - 성공률: {(success_count/(success_count+fail_count)*100):.1f}%")
        
        return {
            "status": "completed",
            "success": success_count,
            "failed": fail_count,
            "total": len(symbols)
        }
        
    except Exception as e:
        print(f"❌ 주가 데이터 수집 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'db' in locals():
            db.close()
        if 'collector' in locals():
            collector.close()

def calculate_technical_indicators_task(**kwargs):
    """기술적 지표 대량 계산"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    
    from database import PostgreSQLManager
    from technical_indicator_calculator_postgres import TechnicalIndicatorCalculatorPostgreSQL
    
    print("🚀 기술적 지표 대량 계산 시작...")
    
    try:
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 데이터가 있는 심볼 조회
        query = """
            SELECT DISTINCT symbol 
            FROM stock_data 
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY symbol
            LIMIT 200
        """
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                symbols = [row[0] for row in cur.fetchall()]
        
        print(f"📊 계산 대상 심볼: {len(symbols):,}개")
        
        if not symbols:
            print("⚠️ 계산할 심볼이 없습니다.")
            return {"status": "no_symbols", "calculated": 0}
        
        # 기술적 지표 계산기 초기화
        calculator = TechnicalIndicatorCalculatorPostgreSQL()
        
        calculated_count = 0
        for i, symbol in enumerate(symbols):
            try:
                # 최근 60일 데이터 조회
                stock_data = db.get_stock_data(symbol, days=60)
                
                if stock_data and len(stock_data) >= 20:
                    # 기술적 지표 계산
                    indicators = calculator.calculate_all_indicators(stock_data)
                    
                    # 배치 저장
                    if indicators:
                        saved_count = calculator.save_indicators_batch(indicators)
                        if saved_count > 0:
                            calculated_count += 1
                    
                    if (i + 1) % 50 == 0:
                        print(f"📈 진행률: {i+1}/{len(symbols)} ({((i+1)/len(symbols)*100):.1f}%)")
                
            except Exception as e:
                print(f"⚠️ {symbol}: 기술적 지표 계산 오류 - {e}")
        
        print(f"✅ 기술적 지표 계산 완료: {calculated_count:,}개")
        
        return {
            "status": "completed",
            "calculated": calculated_count,
            "total": len(symbols)
        }
        
    except Exception as e:
        print(f"❌ 기술적 지표 계산 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'db' in locals():
            db.close()

def generate_daily_watchlist_task(**kwargs):
    """일일 관심종목 생성"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    
    from database import PostgreSQLManager
    from datetime import date
    
    print("🚀 일일 관심종목 생성 시작...")
    
    try:
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        today = date.today()
        watchlist_conditions = [
            {
                'name': 'bollinger_upper_touch',
                'query': """
                    SELECT DISTINCT s.symbol, s.close, t.bb_upper, t.bb_middle, t.bb_lower
                    FROM stock_data s
                    JOIN stock_data_technical_indicators t ON s.symbol = t.symbol AND s.date = t.date
                    WHERE s.date = CURRENT_DATE - INTERVAL '1 day'
                      AND s.close >= t.bb_upper * 0.99
                      AND t.bb_upper IS NOT NULL
                    ORDER BY s.symbol
                    LIMIT 50
                """
            },
            {
                'name': 'rsi_oversold',
                'query': """
                    SELECT DISTINCT s.symbol, s.close, t.rsi
                    FROM stock_data s
                    JOIN stock_data_technical_indicators t ON s.symbol = t.symbol AND s.date = t.date
                    WHERE s.date = CURRENT_DATE - INTERVAL '1 day'
                      AND t.rsi <= 30
                      AND t.rsi IS NOT NULL
                    ORDER BY s.symbol
                    LIMIT 30
                """
            },
            {
                'name': 'volume_spike',
                'query': """
                    SELECT DISTINCT s1.symbol, s1.close, s1.volume,
                           AVG(s2.volume) as avg_volume
                    FROM stock_data s1
                    JOIN stock_data s2 ON s1.symbol = s2.symbol 
                        AND s2.date BETWEEN s1.date - INTERVAL '20 days' AND s1.date - INTERVAL '1 day'
                    WHERE s1.date = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY s1.symbol, s1.close, s1.volume
                    HAVING s1.volume > AVG(s2.volume) * 2
                    ORDER BY s1.symbol
                    LIMIT 30
                """
            }
        ]
        
        total_added = 0
        
        for condition in watchlist_conditions:
            try:
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(condition['query'])
                        results = cur.fetchall()
                        
                        for row in results:
                            try:
                                cur.execute("""
                                    INSERT INTO daily_watchlist (symbol, date, condition_type, condition_value)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (symbol, date, condition_type) DO NOTHING
                                """, (row[0], today, condition['name'], float(row[1])))
                                
                            except Exception as e:
                                print(f"⚠️ {row[0]}: 관심종목 저장 오류 - {e}")
                        
                        conn.commit()
                        added_count = len(results)
                        total_added += added_count
                        print(f"✅ {condition['name']}: {added_count}개 추가")
                        
            except Exception as e:
                print(f"❌ {condition['name']} 조건 처리 오류: {e}")
        
        print(f"✅ 일일 관심종목 생성 완료: 총 {total_added}개")
        
        return {
            "status": "completed",
            "total_added": total_added,
            "date": str(today)
        }
        
    except Exception as e:
        print(f"❌ 일일 관심종목 생성 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'db' in locals():
            db.close()

def cleanup_old_data_task(**kwargs):
    """오래된 데이터 정리"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    
    from database import PostgreSQLManager
    from bulk_data_collector import BulkDataCollector
    
    print("🚀 오래된 데이터 정리 시작...")
    
    try:
        # 대량 수집기 초기화 (정리 기능 사용)
        collector = BulkDataCollector()
        
        # 365일 이상 된 데이터 정리
        deleted_count = collector.cleanup_old_data(days_to_keep=365)
        
        print(f"✅ 오래된 데이터 정리 완료: {deleted_count:,}개 레코드 삭제")
        
        return {
            "status": "completed",
            "deleted_count": deleted_count
        }
        
    except Exception as e:
        print(f"❌ 데이터 정리 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'collector' in locals():
            collector.close()

def check_and_adjust_splits_task(**kwargs):
    """주식분할/배당 체크 및 데이터 조정"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    
    from database import PostgreSQLManager
    import yfinance as yf
    from datetime import date, timedelta
    
    print("🚀 주식분할/배당 체크 및 조정 시작...")
    
    try:
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 최근 거래 있는 모든 심볼들 조회
        query = """
            SELECT DISTINCT symbol 
            FROM stock_data 
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY symbol
            -- 모든 활성 종목 체크 (주식분할은 예측 불가능하므로)
        """
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                symbols = [row[0] for row in cur.fetchall()]
        
        print(f"📊 분할/배당 체크 대상: {len(symbols)}개 심볼")
        
        splits_detected = 0
        adjustments_made = 0
        error_count = 0
        
        # 배치 처리를 위한 진행률 표시
        for i, symbol in enumerate(symbols):
            try:
                # 진행률 표시 (매 50개마다)
                if (i + 1) % 50 == 0:
                    print(f"📈 진행률: {i+1}/{len(symbols)} ({((i+1)/len(symbols)*100):.1f}%)")
                
                # API 제한 방지를 위한 기본 대기 (매 요청마다)
                import time
                time.sleep(0.1)  # 100ms 대기
                
                # 우선 FinanceDataReader로 시도 (더 안정적)
                split_detected = False
                try:
                    import FinanceDataReader as fdr
                    
                    # FDR로 최근 30일간 데이터 확인하여 분할 감지
                    end_date = date.today()
                    start_date = end_date - timedelta(days=30)
                    
                    df = fdr.DataReader(symbol, start=start_date, end=end_date)
                    if not df.empty and len(df) > 1:
                        # 가격 점프로 분할 감지 (전일 대비 40% 이상 변화시)
                        df['price_change'] = df['Close'].pct_change()
                        potential_splits = df[abs(df['price_change']) > 0.4]  # 40% 이상 변화
                        
                        if not potential_splits.empty:
                            for split_date, row in potential_splits.iterrows():
                                change_ratio = 1 + row['price_change']
                                if change_ratio < 0.7:  # 가격이 30% 이상 떨어짐 (분할)
                                    split_ratio = 1 / change_ratio  # 분할 비율 추정
                                    splits_detected += 1
                                    split_detected = True
                                    print(f"🔍 {symbol}: FDR에서 분할 감지 - {split_date.date()}: 추정 비율 {split_ratio:.2f}")
                                    
                                    adjusted = adjust_historical_data(db, symbol, split_date.date(), split_ratio)
                                    if adjusted:
                                        adjustments_made += 1
                                        print(f"✅ {symbol}: {split_date.date()} 분할 조정 완료 (추정 비율: {split_ratio:.2f})")
                except:
                    pass  # FDR 실패시 yfinance로 폴백
                
                # FDR에서 분할을 찾지 못했으면 yfinance로 폴백
                if not split_detected:
                    # yfinance로 최근 1개월간 분할/배당 정보 확인
                    ticker = yf.Ticker(symbol)
                    
                    # 주식분할 정보
                    splits = ticker.splits
                    if not splits.empty:
                        recent_splits = splits[splits.index >= (date.today() - timedelta(days=30))]
                        
                        if not recent_splits.empty:
                            splits_detected += 1
                            print(f"🔍 {symbol}: yfinance에서 주식분할 발견 - {recent_splits.to_dict()}")
                            
                            # 분할 비율에 따른 과거 데이터 조정
                            for split_date, split_ratio in recent_splits.items():
                                if split_ratio != 1.0:  # 실제 분할이 있는 경우
                                    adjusted = adjust_historical_data(db, symbol, split_date.date(), split_ratio)
                                    if adjusted:
                                        adjustments_made += 1
                                        print(f"✅ {symbol}: {split_date.date()} 분할 조정 완료 (비율: {split_ratio})")
                
                    # 배당 정보 (큰 특별배당의 경우에만 조정)
                    dividends = ticker.dividends
                    if not dividends.empty:
                        recent_dividends = dividends[dividends.index >= (date.today() - timedelta(days=7))]
                        
                        # 특별배당 (일반 배당 대비 3배 이상) 체크
                        if not recent_dividends.empty:
                            avg_dividend = dividends.tail(8).mean()  # 최근 2년 평균
                            for div_date, div_amount in recent_dividends.items():
                                if div_amount > avg_dividend * 3:  # 특별배당으로 판단
                                    print(f"🔍 {symbol}: 특별배당 발견 - {div_date.date()}: ${div_amount}")
                                    # 특별배당 조정은 선택적으로 구현 가능
                
            except Exception as e:
                error_count += 1
                print(f"⚠️ {symbol}: 분할/배당 체크 오류 - {e}")
                
                # API 제한 오류 처리
                if "Rate limited" in str(e) or "Too Many Requests" in str(e):
                    print(f"🔄 API 요청 제한 감지, 5초 대기...")
                    import time
                    time.sleep(5)
                elif error_count % 10 == 0:
                    print(f"🔄 API 오류 {error_count}개 발생, 3초 대기...")
                    import time
                    time.sleep(3)
                continue
        
        print(f"✅ 주식분할/배당 체크 완료:")
        print(f"  - 총 체크 종목: {len(symbols)}개")
        print(f"  - 분할 발견: {splits_detected}개")
        print(f"  - 조정 완료: {adjustments_made}개")
        print(f"  - 오류 발생: {error_count}개")
        
        return {
            "status": "completed",
            "splits_detected": splits_detected,
            "adjustments_made": adjustments_made
        }
        
    except Exception as e:
        print(f"❌ 주식분할/배당 체크 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'db' in locals():
            db.close()

def adjust_historical_data(db, symbol: str, split_date: date, split_ratio: float) -> bool:
    """주식분할에 따른 과거 데이터 조정"""
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # 분할 이전 데이터만 조정
                adjust_query = """
                    UPDATE stock_data 
                    SET 
                        open = open / %s,
                        high = high / %s,
                        low = low / %s,
                        close = close / %s,
                        volume = volume * %s
                    WHERE symbol = %s 
                      AND date < %s
                      AND open IS NOT NULL
                """
                
                cur.execute(adjust_query, (
                    split_ratio, split_ratio, split_ratio, split_ratio,
                    split_ratio, symbol, split_date
                ))
                
                adjusted_rows = cur.rowcount
                
                # 기술적 지표도 조정
                if adjusted_rows > 0:
                    indicator_query = """
                        UPDATE stock_data_technical_indicators
                        SET
                            sma_20 = sma_20 / %s,
                            sma_50 = sma_50 / %s,
                            ema_12 = ema_12 / %s,
                            ema_26 = ema_26 / %s,
                            macd = macd / %s,
                            macd_signal = macd_signal / %s,
                            bb_upper = bb_upper / %s,
                            bb_middle = bb_middle / %s,
                            bb_lower = bb_lower / %s
                        WHERE symbol = %s 
                          AND date < %s
                    """
                    
                    cur.execute(indicator_query, (
                        split_ratio, split_ratio, split_ratio, split_ratio,
                        split_ratio, split_ratio, split_ratio, split_ratio,
                        split_ratio, symbol, split_date
                    ))
                
                conn.commit()
                return adjusted_rows > 0
                
    except Exception as e:
        print(f"❌ {symbol} 데이터 조정 오류: {e}")
        return False

def database_status_check_task(**kwargs):
    """데이터베이스 상태 확인"""
    import sys
    sys.path.insert(0, '/opt/airflow/common')
    
    from database import PostgreSQLManager
    from bulk_data_collector import BulkDataCollector
    
    print("🚀 데이터베이스 상태 확인 시작...")
    
    try:
        # 대량 수집기 초기화
        collector = BulkDataCollector()
        
        # 수집 상태 조회
        status = collector.get_collection_status()
        
        print("📊 데이터베이스 상태:")
        print(f"  - 총 심볼 수: {status.get('total_symbols', 0):,}개")
        print(f"  - 총 주가 레코드: {status.get('total_stock_records', 0):,}개")
        print(f"  - 기술적 지표 레코드: {status.get('technical_indicators_records', 0):,}개")
        print(f"  - 최신 데이터 날짜: {status.get('latest_date', 'N/A')}")
        print(f"  - 오늘 데이터가 있는 심볼: {status.get('symbols_with_today_data', 0):,}개")
        
        return {
            "status": "completed",
            "database_status": status
        }
        
    except Exception as e:
        print(f"❌ 상태 확인 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'collector' in locals():
            collector.close()

# Task 정의
collect_symbols = PythonOperator(
    task_id='collect_nasdaq_symbols',
    python_callable=collect_nasdaq_symbols_task,
    dag=dag,
)

check_splits = PythonOperator(
    task_id='check_and_adjust_splits',
    python_callable=check_and_adjust_splits_task,
    dag=dag,
)

collect_stock_data = PythonOperator(
    task_id='bulk_collect_stock_data',
    python_callable=bulk_collect_stock_data_task,
    dag=dag,
)

calculate_indicators = PythonOperator(
    task_id='calculate_technical_indicators',
    python_callable=calculate_technical_indicators_task,
    dag=dag,
)

generate_watchlist = PythonOperator(
    task_id='generate_daily_watchlist',
    python_callable=generate_daily_watchlist_task,
    dag=dag,
)

cleanup_data = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data_task,
    dag=dag,
)

status_check = PythonOperator(
    task_id='database_status_check',
    python_callable=database_status_check_task,
    dag=dag,
)

# Task 종속성 정의 (주식분할 체크를 데이터 수집 전에 실행)
collect_symbols >> check_splits >> collect_stock_data >> calculate_indicators >> generate_watchlist >> cleanup_data >> status_check
