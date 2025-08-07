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
        
        query = """
        WITH bb_signals AS (
            SELECT 
                t.symbol,
                t.date,
                s.close,
                t.bb_upper,
                t.bb_middle,
                t.bb_lower,
                -- 상단 터치 조건: 현재가가 상단선의 98% 이상
                CASE WHEN s.close >= t.bb_upper * 0.98 THEN 1 ELSE 0 END as upper_touch,
                n.market_cap,
                CASE 
                    WHEN CAST(REPLACE(REPLACE(REPLACE(n.market_cap, '$', ''), 'B', ''), 'T', '') AS DECIMAL) >= 100 THEN 1
                    WHEN CAST(REPLACE(REPLACE(REPLACE(n.market_cap, '$', ''), 'B', ''), 'T', '') AS DECIMAL) >= 10 THEN 2
                    ELSE 3
                END as tier
            FROM stock_data_technical_indicators t
            JOIN nasdaq_symbols n ON t.symbol = n.symbol
            JOIN stock_data s ON t.symbol = s.symbol AND t.date = s.date
            WHERE t.date = %s
              AND t.bb_upper IS NOT NULL
              AND s.close IS NOT NULL
        )
        SELECT * FROM bb_signals WHERE upper_touch = 1
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
                    'bb_upper': row[3],
                    'condition_type': 'bollinger_upper_touch',
                    'condition_value': row[2] / row[3] if row[3] else 0,  # 상단선 대비 비율
                    'market_cap_tier': row[8]
                })
            
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
            n.market_cap
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
                    'market_cap': row[4]
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
            t.macd_line,
            t.macd_signal,
            n.market_cap
        FROM stock_data_technical_indicators t
        JOIN nasdaq_symbols n ON t.symbol = n.symbol
        JOIN stock_data s ON t.symbol = s.symbol AND t.date = s.date
        WHERE t.date = %s
          AND t.macd_line IS NOT NULL
          AND t.macd_signal IS NOT NULL
          AND t.macd_line > t.macd_signal
          AND t.macd_line > 0
        ORDER BY (t.macd_line - t.macd_signal) DESC
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
                    'macd_line': row[3],
                    'macd_signal': row[4],
                    'condition_type': 'macd_bullish',
                    'condition_value': row[3] - row[4],  # MACD 히스토그램
                    'market_cap': row[5]
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
