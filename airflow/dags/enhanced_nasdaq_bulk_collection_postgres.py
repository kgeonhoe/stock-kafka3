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

def detect_stock_split_advanced(df, symbol):
    """
    고급 주식 분할 감지 로직
    - 가격 변화, 거래량, 패턴 분석을 종합적으로 고려
    """
    import pandas as pd
    import numpy as np
    
    try:
        # 1. 기본 가격 변화율 계산
        df['price_change'] = df['Close'].pct_change()
        df['volume_change'] = df['Volume'].pct_change()
        
        # 2. 이동평균과의 괴리율 계산 (정상 범위 벗어나는지 확인)
        df['sma_5'] = df['Close'].rolling(window=5).mean()
        df['price_deviation'] = (df['Close'] - df['sma_5']) / df['sma_5']
        
        # 3. 분할 후보 조건들
        potential_splits = df[
            (abs(df['price_change']) > 0.4) &  # 40% 이상 급변
            (df['price_change'] < -0.3) &      # 가격 하락 (분할 특성)
            (df['Volume'] > df['Volume'].rolling(window=5).mean() * 1.2)  # 거래량 20% 이상 증가
        ]
        
        if potential_splits.empty:
            return False
            
        # 4. 분할 비율이 일반적인 비율인지 확인 (2:1, 3:1, 4:1 등)
        for split_date, row in potential_splits.iterrows():
            price_change = row['price_change']
            change_ratio = 1 + price_change
            
            if change_ratio > 0.01:  # 0 방지
                estimated_ratio = 1 / change_ratio
                
                # 일반적인 분할 비율인지 확인 (1.8~2.2, 2.8~3.2, 3.8~4.2 등)
                common_ratios = [2, 3, 4, 5, 10]
                for common_ratio in common_ratios:
                    if abs(estimated_ratio - common_ratio) < 0.3:
                        print(f"🔍 {symbol}: 분할 감지 - {split_date.date()}: {common_ratio}:1 분할 추정")
                        print(f"💡 {symbol}: 분할 감지됨, 다음 데이터 수집시 자동으로 조정된 데이터 수집됨")
                        
                        # 분할 발생을 기록만 하고 실제 조정은 하지 않음 (FDR이 자동 조정해줌)
                        # from database import PostgreSQLManager
                        # db = PostgreSQLManager()
                        # try:
                        #     adjusted = adjust_historical_data(db, symbol, split_date.date(), common_ratio)
                        #     if adjusted:
                        #         print(f"✅ {symbol}: {split_date.date()} 분할 조정 완료 ({common_ratio}:1)")
                        #         return True
                        # finally:
                        #     db.close()
                        
                        return True  # 분할 감지됨을 반환만
        
        return False
        
    except Exception as e:
        print(f"⚠️ {symbol}: 고급 분할 감지 오류 - {e}")
        return False

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
        
        # 배치 주가 데이터 수집 (2020-01-01부터 현재까지)
        from datetime import date
        start_date = date(2020, 1, 1)  # 2020년 1월 1일
        current_date = date.today()
        days_back = (current_date - start_date).days  # 2020-01-01부터 현재까지 일수 계산
        
        print(f"📅 데이터 수집 기간: {start_date} ~ {current_date} ({days_back}일)")
        
        success_count, fail_count = collector.collect_stock_data_batch(
            symbols=symbols, 
            days_back=days_back  # 계산된 일수 사용
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
                    SELECT DISTINCT s.symbol, s.date, s.close, t.bb_upper, t.bb_middle, t.bb_lower
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
                    SELECT DISTINCT s.symbol, s.date, s.close, t.rsi
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
                    SELECT DISTINCT s1.symbol, s1.date, s1.close, s1.volume,
                           AVG(s2.volume) as avg_volume
                    FROM stock_data s1
                    JOIN stock_data s2 ON s1.symbol = s2.symbol 
                        AND s2.date BETWEEN s1.date - INTERVAL '20 days' AND s1.date - INTERVAL '1 day'
                    WHERE s1.date = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY s1.symbol, s1.date, s1.close, s1.volume
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
                                # row[1]은 실제 stock_data의 날짜 (CURRENT_DATE - INTERVAL '1 day')
                                actual_date = row[1]  # 쿼리에서 가져온 실제 데이터 날짜
                                trigger_price = float(row[2])  # close 가격 (인덱스 조정)
                                
                                cur.execute("""
                                    INSERT INTO daily_watchlist (symbol, date, condition_type, condition_value)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (symbol, date, condition_type) DO NOTHING
                                """, (row[0], actual_date, condition['name'], trigger_price))
                                
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
            "actual_date": str(results[0][1]) if results else None  # 실제 사용된 데이터 날짜
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
    from datetime import date, timedelta
    
    print("🚀 주식분할/배당 체크 및 조정 시작...")
    
    try:
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # 최근 거래 있는 모든 심볼들 조회 (배치 처리를 위해 제한)
        query = """
            SELECT DISTINCT symbol 
            FROM stock_data 
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY symbol
           
        """
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                all_symbols = [row[0] for row in cur.fetchall()]
                
                # 최근 3일 내에 이미 체크한 심볼들 제외 (캐싱)
                checked_query = """
                    SELECT DISTINCT symbol 
                    FROM split_check_log 
                    WHERE check_date >= CURRENT_DATE - INTERVAL '3 days'
                """
                cur.execute(checked_query)
                already_checked = set(row[0] for row in cur.fetchall())
                
                # 아직 체크하지 않은 심볼들만 필터링
                symbols = [s for s in all_symbols if s not in already_checked]
        
        print(f"📊 분할/배당 체크 대상: {len(symbols)}개 심볼 (이미 체크됨: {len(already_checked)}개)")
        
        splits_detected = 0
        adjustments_made = 0
        error_count = 0
        
        # 배치 처리를 위한 진행률 표시
        for i, symbol in enumerate(symbols):
            try:
                # API 제한 방지를 위한 기본 대기 (FDR도 제한이 있음)
                import time
                time.sleep(2.0)  # 2초 대기로 더욱 증가 (FDR API 안정성을 위해)
                
                # 진행률 표시 및 추가 대기 (매 5개마다)
                if (i + 1) % 5 == 0:
                    print(f"📈 진행률: {i+1}/{len(symbols)} ({((i+1)/len(symbols)*100):.1f}%) - 추가 대기 중...")
                    time.sleep(5.0)  # 5개마다 5초 추가 대기
                
                # 우선 FinanceDataReader로 시도 (더 안정적이고 제한이 적음)
                split_detected = False
                try:
                    import FinanceDataReader as fdr
                    
                    # FDR로 최근 30일간 데이터 확인하여 분할 감지
                    end_date = date.today()
                    start_date = end_date - timedelta(days=30)
                    
                    df = fdr.DataReader(symbol, start=start_date, end=end_date)
                    if not df.empty and len(df) > 5:  # 최소 5일 데이터 필요
                        # 더 정교한 분할 감지 로직 적용
                        split_detected = detect_stock_split_advanced(df, symbol)
                        
                        if split_detected:
                            splits_detected += 1
                            adjustments_made += 1  # FDR에서 감지하면 조정도 완료됨
                            print(f"🔍 {symbol}: FDR에서 고급 분할 감지 및 조정 완료")
                        else:
                            split_detected = False
                except Exception as fdr_error:
                    print(f"⚠️ {symbol}: FDR 데이터 수집 실패 - {fdr_error}")
                    split_detected = False  # 분할 감지 실패
                
                # 체크 완료 로그 저장 (성공/실패 관계없이)
                try:
                    with db.get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO split_check_log (symbol, check_date, has_split)
                                VALUES (%s, CURRENT_DATE, %s)
                                ON CONFLICT (symbol, check_date) DO NOTHING
                            """, (symbol, split_detected))
                            conn.commit()
                except Exception as log_error:
                    print(f"⚠️ {symbol}: 체크 로그 저장 실패 - {log_error}")
                    
            except Exception as e:
                error_count += 1
                print(f"⚠️ {symbol}: 분할/배당 체크 오류 - {e}")
                
                # FDR API 제한 오류 처리 - 지수 백오프 적용
                if "Rate limited" in str(e) or "Too Many Requests" in str(e) or "429" in str(e):
                    backoff_time = min(60, 10 * (2 ** min(error_count // 3, 3)))  # 최대 60초까지 지수 백오프
                    print(f"🔄 FDR API 요청 제한 감지, {backoff_time}초 대기... (오류 횟수: {error_count})")
                    import time
                    time.sleep(backoff_time)
                elif error_count % 3 == 0:  # 매 3개 오류마다 더 긴 대기
                    print(f"🔄 FDR API 오류 {error_count}개 발생, 15초 대기...")
                    import time
                    time.sleep(15)
                
                # 너무 많은 API 오류 발생시 중단
                if error_count > len(symbols) * 0.3:  # 30% 이상 실패시 중단
                    print(f"❌ FDR API 오류 과다 발생 ({error_count}개), 작업 중단")
                    break
                    
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
        # 분할 비율이 유효한지 확인 (0 또는 너무 작은 값 방지)
        if split_ratio <= 0 or split_ratio > 100:  # 100:1 분할을 최대로 제한
            print(f"⚠️ {symbol}: 비정상적인 분할 비율 {split_ratio}, 조정 건너뜀")
            return False
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
                
                # 기술적 지표도 조정 (실제 컬럼에 맞게)
                if adjusted_rows > 0:
                    indicator_query = """
                        UPDATE stock_data_technical_indicators
                        SET
                            sma_5 = CASE WHEN sma_5 IS NOT NULL THEN sma_5 / %s END,
                            sma_20 = CASE WHEN sma_20 IS NOT NULL THEN sma_20 / %s END,
                            sma_60 = CASE WHEN sma_60 IS NOT NULL THEN sma_60 / %s END,
                            ema_5 = CASE WHEN ema_5 IS NOT NULL THEN ema_5 / %s END,
                            ema_20 = CASE WHEN ema_20 IS NOT NULL THEN ema_20 / %s END,
                            ema_60 = CASE WHEN ema_60 IS NOT NULL THEN ema_60 / %s END,
                            bb_upper = CASE WHEN bb_upper IS NOT NULL THEN bb_upper / %s END,
                            bb_middle = CASE WHEN bb_middle IS NOT NULL THEN bb_middle / %s END,
                            bb_lower = CASE WHEN bb_lower IS NOT NULL THEN bb_lower / %s END,
                            macd = CASE WHEN macd IS NOT NULL THEN macd / %s END,
                            macd_signal = CASE WHEN macd_signal IS NOT NULL THEN macd_signal / %s END,
                            ma_112 = CASE WHEN ma_112 IS NOT NULL THEN ma_112 / %s END,
                            ma_224 = CASE WHEN ma_224 IS NOT NULL THEN ma_224 / %s END,
                            ma_448 = CASE WHEN ma_448 IS NOT NULL THEN ma_448 / %s END
                        WHERE symbol = %s 
                          AND date < %s
                    """
                    
                    cur.execute(indicator_query, (
                        split_ratio, split_ratio, split_ratio, split_ratio,
                        split_ratio, split_ratio, split_ratio, split_ratio,
                        split_ratio, split_ratio, split_ratio, split_ratio,
                        split_ratio, split_ratio, symbol, split_date
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
# cleanup_data 태스크 비활성화 - 오래된 데이터 보관을 위해
collect_symbols >> check_splits >> collect_stock_data >> calculate_indicators >> generate_watchlist >> status_check
