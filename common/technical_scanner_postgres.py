#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import sys
import os

# 절대 경로로 database 모듈 임포트
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
from database import PostgreSQLManager

class TechnicalScannerPostgreSQL:
    """기술적 지표 기반 종목 스캔 (PostgreSQL 버전)"""
    
    def __init__(self):
        self.db = PostgreSQLManager()
    
    def scan_bollinger_band_signals(self, scan_date: date = None) -> List[Dict[str, Any]]:
        """볼린저 밴드 상단 터치 종목 스캔"""
        if scan_date is None:
            scan_date = date.today()
        
        # 먼저 간단한 테스트 쿼리로 데이터 확인
        test_query = """
        SELECT COUNT(*) 
        FROM stock_data_technical_indicators t
        JOIN stock_data s ON t.symbol = s.symbol AND t.date = s.date
        JOIN nasdaq_symbols n ON t.symbol = n.symbol
        WHERE t.date = %s AND t.bb_upper IS NOT NULL
        """
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(test_query, (scan_date,))
                    count_result = cur.fetchone()
                    print(f"🔍 사용 가능한 데이터: {count_result[0] if count_result else 0}개")
        except Exception as test_error:
            print(f"⚠️ 테스트 쿼리 오류: {test_error}")
            
        query = """
        SELECT 
            t.symbol,
            t.date,
            s.close,
            t.bb_upper,
            t.bb_middle,
            t.bb_lower,
            CASE WHEN s.close >= t.bb_upper * 0.98 THEN 1 ELSE 0 END as upper_touch,
            COALESCE(n.market_cap, 'N/A') as market_cap,
            CASE 
                WHEN n.market_cap IS NULL OR n.market_cap = '' THEN 3
                WHEN n.market_cap ~ '^\\$[0-9]+(\\.?[0-9]*)T' THEN 1
                WHEN n.market_cap ~ '^\\$[0-9]+(\\.?[0-9]*)B' THEN 
                    CASE WHEN CAST(REPLACE(REPLACE(n.market_cap, '$', ''), 'B', '') AS DECIMAL) >= 100 THEN 1 ELSE 2 END
                ELSE 3
            END as tier
        FROM stock_data_technical_indicators t
        JOIN stock_data s ON t.symbol = s.symbol AND t.date = s.date
        JOIN nasdaq_symbols n ON t.symbol = n.symbol
        WHERE t.date = %s
          AND t.bb_upper IS NOT NULL
          AND s.close IS NOT NULL
          AND s.close >= t.bb_upper * 0.98
        ORDER BY (s.close / t.bb_upper) DESC
        LIMIT 50
        """
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (scan_date,))
                    results = cur.fetchall()
                    
            print(f"🔍 볼린저 밴드 쿼리 결과: {len(results)}개 행")
            if results:
                print(f"🔍 첫 번째 행: {results[0]}")
                print(f"🔍 첫 번째 행 길이: {len(results[0])}")
                print(f"🔍 각 항목 타입: {[type(item) for item in results[0]]}")
            
            watchlist = []
            for i, row in enumerate(results):
                try:
                    if i < 3:  # 처음 3개 행만 디버그 출력
                        print(f"🔍 행 {i}: 길이={len(row)}, 내용={row}")
                    
                    # 안전하게 각 인덱스에 접근
                    symbol = row[0] if len(row) > 0 else 'UNKNOWN'
                    date_val = row[1] if len(row) > 1 else scan_date
                    close_price = float(row[2]) if len(row) > 2 and row[2] else 0
                    bb_upper = float(row[3]) if len(row) > 3 and row[3] else 0
                    bb_middle = float(row[4]) if len(row) > 4 and row[4] else 0
                    bb_lower = float(row[5]) if len(row) > 5 and row[5] else 0
                    upper_touch = row[6] if len(row) > 6 else 0
                    market_cap = row[7] if len(row) > 7 else ''
                    tier = int(row[8]) if len(row) > 8 and row[8] else 3
                    
                    watchlist.append({
                        'symbol': symbol,
                        'date': date_val,
                        'close_price': close_price,
                        'bb_upper': bb_upper,
                        'condition_type': 'bollinger_upper_touch',
                        'condition_value': close_price / bb_upper if bb_upper > 0 else 0,
                        'market_cap_tier': tier
                    })
                    
                except (IndexError, ValueError, TypeError) as row_error:
                    print(f"❌ 행 처리 오류 (행 {i}): {row_error}")
                    print(f"   행 길이: {len(row) if row else 'None'}")
                    print(f"   행 내용: {row}")
                    continue
            
            print(f"✅ 처리 완료: {len(watchlist)}개 신호")
            return watchlist
            
        except Exception as e:
            print(f"❌ 볼린저 밴드 스캔 오류: {e}")
            return []
    
    def scan_rsi_oversold_signals(self, scan_date: date = None) -> List[Dict[str, Any]]:
        """RSI 과매도 신호 스캔"""
        if scan_date is None:
            scan_date = date.today()
        
        query = """
        SELECT 
            t.symbol,
            t.date,
            s.close,
            t.rsi,
            n.market_cap,
            CASE 
                WHEN n.market_cap IS NULL OR n.market_cap = '' THEN 3
                WHEN n.market_cap ~ '^\\$[0-9]+(\\.?[0-9]*)T' THEN 1  -- Trillion
                WHEN n.market_cap ~ '^\\$[0-9]+(\\.?[0-9]*)B' THEN 
                    CASE WHEN CAST(REPLACE(REPLACE(n.market_cap, '$', ''), 'B', '') AS DECIMAL) >= 100 THEN 1 ELSE 2 END
                ELSE 3
            END as tier
        FROM stock_data_technical_indicators t
        JOIN nasdaq_symbols n ON t.symbol = n.symbol
        JOIN stock_data s ON t.symbol = s.symbol AND t.date = s.date
        WHERE t.date = %s
          AND t.rsi IS NOT NULL
          AND t.rsi <= 30
        ORDER BY t.rsi ASC
        """
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (scan_date,))
                    results = cur.fetchall()
            
            watchlist = []
            for row in results:
                watchlist.append({
                    'symbol': row[0],
                    'date': row[1],
                    'close_price': row[2],
                    'rsi': row[3],
                    'condition_type': 'rsi_oversold',
                    'condition_value': row[3],
                    'market_cap': row[4],
                    'market_cap_tier': row[5]
                })
            
            return watchlist
            
        except Exception as e:
            print(f"❌ RSI 과매도 스캔 오류: {e}")
            return []
    
    def scan_macd_bullish_signals(self, scan_date: date = None) -> List[Dict[str, Any]]:
        """MACD 강세 신호 스캔"""
        if scan_date is None:
            scan_date = date.today()
        
        query = """
        SELECT 
            t.symbol,
            t.date,
            s.close,
            t.macd,
            t.macd_signal,
            n.market_cap,
            CASE 
                WHEN n.market_cap IS NULL OR n.market_cap = '' THEN 3
                WHEN n.market_cap ~ '^\\$[0-9]+(\\.?[0-9]*)T' THEN 1  -- Trillion
                WHEN n.market_cap ~ '^\\$[0-9]+(\\.?[0-9]*)B' THEN 
                    CASE WHEN CAST(REPLACE(REPLACE(n.market_cap, '$', ''), 'B', '') AS DECIMAL) >= 100 THEN 1 ELSE 2 END
                ELSE 3
            END as tier
        FROM stock_data_technical_indicators t
        JOIN nasdaq_symbols n ON t.symbol = n.symbol
        JOIN stock_data s ON t.symbol = s.symbol AND t.date = s.date
        WHERE t.date = %s
          AND t.macd IS NOT NULL
          AND t.macd_signal IS NOT NULL
          AND t.macd > t.macd_signal
          AND t.macd > 0
        ORDER BY (t.macd - t.macd_signal) DESC
        """
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (scan_date,))
                    results = cur.fetchall()
            
            watchlist = []
            for row in results:
                watchlist.append({
                    'symbol': row[0],
                    'date': row[1],
                    'close_price': row[2],
                    'macd': row[3],
                    'macd_signal': row[4],
                    'condition_type': 'macd_bullish',
                    'condition_value': row[3] - row[4],  # MACD 히스토그램
                    'market_cap': row[5],
                    'market_cap_tier': row[6]
                })
            
            return watchlist
            
        except Exception as e:
            print(f"❌ MACD 강세 스캔 오류: {e}")
            return []
    
    def update_daily_watchlist(self, scan_date: date = None):
        """일별 관심종목 업데이트"""
        if scan_date is None:
            scan_date = date.today()
        
        all_signals = []
        
        # 볼린저 밴드 신호 스캔
        bb_signals = self.scan_bollinger_band_signals(scan_date)
        all_signals.extend(bb_signals)
        
        # RSI 과매도 신호 스캔
        rsi_signals = self.scan_rsi_oversold_signals(scan_date)
        all_signals.extend(rsi_signals)
        
        # MACD 강세 신호 스캔
        macd_signals = self.scan_macd_bullish_signals(scan_date)
        all_signals.extend(macd_signals)
        
        # 데이터베이스에 저장
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    for signal in all_signals:
                        cur.execute("""
                            INSERT INTO daily_watchlist 
                            (symbol, date, condition_type, condition_value, market_cap_tier)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, date, condition_type) DO UPDATE SET
                                condition_value = EXCLUDED.condition_value,
                                market_cap_tier = EXCLUDED.market_cap_tier
                        """, (
                            signal['symbol'],
                            signal['date'],
                            signal['condition_type'],
                            signal['condition_value'],
                            signal.get('market_cap_tier', 3)
                        ))
                    conn.commit()
            
            print(f"📈 {scan_date} 관심종목 {len(all_signals)}개 업데이트 완료")
            return all_signals
            
        except Exception as e:
            print(f"❌ 관심종목 업데이트 오류: {e}")
            return []
    
    def get_daily_watchlist(self, scan_date: date = None, condition_type: str = None) -> List[Dict[str, Any]]:
        """일별 관심종목 조회"""
        if scan_date is None:
            scan_date = date.today()
        
        query = """
        SELECT 
            w.symbol,
            w.date,
            w.condition_type,
            w.condition_value,
            w.market_cap_tier,
            s.close,
            n.name
        FROM daily_watchlist w
        JOIN stock_data s ON w.symbol = s.symbol AND w.date = s.date
        JOIN nasdaq_symbols n ON w.symbol = n.symbol
        WHERE w.date = %s
        """
        
        params = [scan_date]
        
        if condition_type:
            query += " AND w.condition_type = %s"
            params.append(condition_type)
        
        query += " ORDER BY w.condition_value DESC"
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    results = cur.fetchall()
            
            watchlist = []
            for row in results:
                watchlist.append({
                    'symbol': row[0],
                    'date': row[1],
                    'condition_type': row[2],
                    'condition_value': row[3],
                    'market_cap_tier': row[4],
                    'close_price': row[5],
                    'name': row[6]
                })
            
            return watchlist
            
        except Exception as e:
            print(f"❌ 관심종목 조회 오류: {e}")
            return []
    
    def get_top_performers(self, scan_date: date = None, limit: int = 20) -> List[Dict[str, Any]]:
        """상위 성과 종목 조회"""
        if scan_date is None:
            scan_date = date.today()
        
        query = """
        SELECT 
            s.symbol,
            s.date,
            s.close,
            s.volume,
            n.name,
            n.market_cap,
            -- 전일 대비 변화율 계산
            COALESCE(
                (s.close - prev.close) / prev.close * 100, 0
            ) as change_percent
        FROM stock_data s
        JOIN nasdaq_symbols n ON s.symbol = n.symbol
        LEFT JOIN stock_data prev ON s.symbol = prev.symbol 
            AND prev.date = s.date - INTERVAL '1 day'
        WHERE s.date = %s
          AND s.close IS NOT NULL
          AND s.volume > 100000  -- 최소 거래량 조건
        ORDER BY change_percent DESC
        LIMIT %s
        """
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (scan_date, limit))
                    results = cur.fetchall()
            
            performers = []
            for row in results:
                performers.append({
                    'symbol': row[0],
                    'date': row[1],
                    'close_price': row[2],
                    'volume': row[3],
                    'name': row[4],
                    'market_cap': row[5],
                    'change_percent': row[6]
                })
            
            return performers
            
        except Exception as e:
            print(f"❌ 상위 성과 종목 조회 오류: {e}")
            return []
    
    def close(self):
        """리소스 정리"""
        self.db.close()


# 호환성을 위한 별칭
TechnicalScanner = TechnicalScannerPostgreSQL
