#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import duckdb
import os
from typing import List, Dict, Any
from datetime import datetime

class DuckDBManager:
    """DuckDB 데이터베이스 관리 클래스"""
    
    def __init__(self, db_path: str = "/data/duckdb/stock_data.db"):
        """
        DuckDB 매니저 초기화
        
        Args:
            db_path: DuckDB 파일 경로
        """
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = duckdb.connect(db_path)
        self._create_tables()
    
    def _create_tables(self):
        """필요한 테이블 생성"""
        # 나스닥 종목 테이블
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nasdaq_symbols (
                symbol VARCHAR PRIMARY KEY,
                name VARCHAR,
                market_cap VARCHAR,
                sector VARCHAR,
                collected_at TIMESTAMP
            )
        """)
        
        # 주가 데이터 테이블
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                symbol VARCHAR,
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                PRIMARY KEY (symbol, date)
            )
        """)
        
        # 기술적 지표 테이블
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_data_technical_indicators (
                symbol VARCHAR,
                date DATE,
                bb_upper DOUBLE,
                bb_middle DOUBLE,
                bb_lower DOUBLE,
                macd DOUBLE,
                macd_signal DOUBLE,
                macd_histogram DOUBLE,
                rsi DOUBLE,
                sma_5 DOUBLE,
                sma_20 DOUBLE,
                sma_60 DOUBLE,
                ema_5 DOUBLE,
                ema_20 DOUBLE,
                ema_60 DOUBLE,
                cci DOUBLE,
                obv BIGINT,
                ma_112 DOUBLE,
                ma_224 DOUBLE,
                ma_448 DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)
        
        # 일별 관심종목 테이블
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_watchlist (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                condition_type VARCHAR(50) NOT NULL,  -- 'bollinger_upper_touch', 'rsi_oversold' 등
                condition_value DECIMAL(10,4),
                market_cap_tier INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date, condition_type)
            )
        """)
        
        # 기술적 지표 계산용 뷰
        self.conn.execute("""
            CREATE VIEW IF NOT EXISTS stock_technical_indicators AS
            SELECT 
                symbol,
                date,
                close as close_price,
                -- 볼린저 밴드 (20일 이동평균 ± 2 표준편차)
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as bb_middle,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) + 2 * STDDEV(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as bb_upper,
                AVG(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) - 2 * STDDEV(close) OVER (
                    PARTITION BY symbol 
                    ORDER BY date 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as bb_lower,
                -- RSI 계산용 기본 데이터
                close - LAG(close) OVER (
                    PARTITION BY symbol ORDER BY date
                ) as price_change
            FROM stock_data
            ORDER BY symbol, date
        """)
    
    def save_nasdaq_symbols(self, symbols: List[Dict[str, Any]]):
        """
        나스닥 심볼 데이터 저장
        
        Args:
            symbols: 나스닥 심볼 데이터 리스트
        """
        now = datetime.now()
        for symbol_data in symbols:
            self.conn.execute("""
                INSERT INTO nasdaq_symbols (symbol, name, market_cap, sector, collected_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    market_cap = EXCLUDED.market_cap,
                    sector = EXCLUDED.sector,
                    collected_at = EXCLUDED.collected_at
            """, (
                symbol_data.get('symbol'),
                symbol_data.get('name'),
                symbol_data.get('marketCap'),
                symbol_data.get('sector'),
                now
            ))
    
    def get_existing_dates(self, symbol: str, days_back: int = 30) -> set:
        """
        특정 종목의 기존 데이터 날짜 조회
        
        Args:
            symbol: 종목 심볼
            days_back: 최근 며칠 데이터를 확인할지
            
        Returns:
            기존 데이터가 있는 날짜들의 set
        """
        try:
            from datetime import date, timedelta
            
            # 최근 N일간의 날짜 범위
            end_date = date.today()
            start_date = end_date - timedelta(days=days_back)
            
            result = self.conn.execute("""
                SELECT date FROM stock_data
                WHERE symbol = ? AND date >= ? AND date <= ?
                ORDER BY date DESC
            """, (symbol, start_date, end_date)).fetchall()
            
            return {row[0] for row in result}
            
        except Exception as e:
            print(f"⚠️ {symbol}: 기존 날짜 조회 오류 - {e}")
            return set()

    def get_latest_date(self, symbol: str):
        """
        특정 종목의 최신 데이터 날짜 조회
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            최신 데이터 날짜 (없으면 None)
        """
        try:
            result = self.conn.execute("""
                SELECT MAX(date) FROM stock_data
                WHERE symbol = ?
            """, (symbol,)).fetchone()
            
            return result[0] if result and result[0] else None
            
        except Exception as e:
            print(f"⚠️ {symbol}: 최신 날짜 조회 오류 - {e}")
            return None

    def save_stock_data(self, stock_data: Dict[str, Any]):
        """
        주가 데이터 저장
        
        Args:
            stock_data: 주가 데이터
        """
        self.conn.execute("""
            INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """, (
            stock_data.get('symbol'),
            stock_data.get('date'),
            stock_data.get('open'),
            stock_data.get('high'),
            stock_data.get('low'),
            stock_data.get('close'),
            stock_data.get('volume')
        ))
    
    def save_technical_indicators(self, indicator_data: Dict[str, Any]):
        """
        기술적 지표 데이터 저장
        
        Args:
            indicator_data: 기술적 지표 데이터
        """
        self.conn.execute("""
            INSERT INTO stock_data_technical_indicators 
            (symbol, date, bb_upper, bb_middle, bb_lower, macd, macd_signal, 
            macd_histogram, rsi, sma_5, sma_20, sma_60, ema_5, ema_20, ema_60, 
            cci, obv, ma_112, ma_224, ma_448)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, date) DO UPDATE SET
                bb_upper = EXCLUDED.bb_upper,
                bb_middle = EXCLUDED.bb_middle,
                bb_lower = EXCLUDED.bb_lower,
                macd = EXCLUDED.macd,
                macd_signal = EXCLUDED.macd_signal,
                macd_histogram = EXCLUDED.macd_histogram,
                rsi = EXCLUDED.rsi,
                sma_5 = EXCLUDED.sma_5,
                sma_20 = EXCLUDED.sma_20,
                sma_60 = EXCLUDED.sma_60,
                ema_5 = EXCLUDED.ema_5,
                ema_20 = EXCLUDED.ema_20,
                ema_60 = EXCLUDED.ema_60,
                cci = EXCLUDED.cci,
                obv = EXCLUDED.obv,
                ma_112 = EXCLUDED.ma_112,
                ma_224 = EXCLUDED.ma_224,
                ma_448 = EXCLUDED.ma_448
        """, (
            indicator_data.get('symbol'),
            indicator_data.get('date'),
            indicator_data.get('bb_upper'),
            indicator_data.get('bb_middle'),
            indicator_data.get('bb_lower'),
            indicator_data.get('macd'),
            indicator_data.get('macd_signal'),
            indicator_data.get('macd_histogram'),
            indicator_data.get('rsi'),
            indicator_data.get('sma_5'),
            indicator_data.get('sma_20'),
            indicator_data.get('sma_60'),
            indicator_data.get('ema_5'),
            indicator_data.get('ema_20'),
            indicator_data.get('ema_60'),
            indicator_data.get('cci'),
            indicator_data.get('obv'),
            indicator_data.get('ma_112'),
            indicator_data.get('ma_224'),
            indicator_data.get('ma_448')
        ))
    
    def get_active_symbols(self) -> List[str]:
        """활성 심볼 목록 조회"""
        result = self.conn.execute("""
            SELECT symbol FROM nasdaq_symbols
        """).fetchall()
        return [row[0] for row in result]
    
    def execute_query(self, query: str, params=None):
        """
        쿼리 실행 후 결과 반환
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터 (옵션)
            
        Returns:
            결과 리스트
        """
        try:
            # DuckDB의 fetchdf() 시도 (pandas가 있는 경우)
            if params:
                result = self.conn.execute(query, params).fetchdf()
            else:
                result = self.conn.execute(query).fetchdf()
            return result
        except Exception:
            # pandas가 없거나 오류 발생 시 일반 결과 반환
            if params:
                result = self.conn.execute(query, params).fetchall()
            else:
                result = self.conn.execute(query).fetchall()
            
            # 간단한 리스트 반환 클래스
            class QueryResult:
                def __init__(self, data):
                    self.data = data
                
                @property
                def empty(self):
                    return len(self.data) == 0
                
                def __getitem__(self, key):
                    if key == 'symbol':
                        return [row[0] for row in self.data]
                    return None
                
                def tolist(self):
                    return [row[0] for row in self.data]
            
            return QueryResult(result)
    
    def get_stock_data(self, symbol: str, days: int = 60):
        """
        특정 종목의 주가 데이터 조회
        
        Args:
            symbol: 종목 심볼
            days: 조회할 날짜 수
        """
        result = self.conn.execute("""
            SELECT * FROM stock_data
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT ?
        """, (symbol, days)).fetchall()
        return result
    
    def close(self):
        """연결 종료"""
        self.conn.close()
