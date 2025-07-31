#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import duckdb
import os
import json
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
        
        # 동적 지표 관리자 초기화
        self.indicator_manager = DynamicIndicatorManager(self)
    
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
    
    def is_nasdaq_symbols_collected_today(self) -> bool:
        """
        오늘 날짜에 NASDAQ 심볼이 이미 수집되었는지 확인
        
        Returns:
            오늘 수집되었으면 True, 아니면 False
        """
        from datetime import date
        today = date.today()
        
        result = self.conn.execute("""
            SELECT COUNT(*) as count 
            FROM nasdaq_symbols 
            WHERE collected_at::date = ?
        """, (today,)).fetchone()
        
        count = result[0] if result else 0
        return count > 0
    
    def get_nasdaq_symbols_last_collected_date(self):
        """
        NASDAQ 심볼이 마지막으로 수집된 날짜 조회
        
        Returns:
            마지막 수집 날짜 또는 None
        """
        result = self.conn.execute("""
            SELECT collected_at::date as last_date 
            FROM nasdaq_symbols 
            ORDER BY collected_at DESC 
            LIMIT 1
        """).fetchone()
        
        return result[0] if result else None

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
    
    def save_stock_data_batch(self, stock_data_list: List[Dict[str, Any]]) -> int:
        """
        주가 데이터 배치 저장 (성능 최적화)
        
        Args:
            stock_data_list: 주가 데이터 리스트
            
        Returns:
            저장된 레코드 수
        """
        if not stock_data_list:
            return 0
        
        try:
            # 배치 데이터 준비
            batch_values = []
            for stock_data in stock_data_list:
                batch_values.append((
                    stock_data.get('symbol'),
                    stock_data.get('date'),
                    stock_data.get('open'),
                    stock_data.get('high'),
                    stock_data.get('low'),
                    stock_data.get('close'),
                    stock_data.get('volume')
                ))
            
            # 배치 INSERT 실행
            self.conn.executemany("""
                INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """, batch_values)
            
            return len(batch_values)
            
        except Exception as e:
            print(f"배치 저장 오류: {e}")
            raise
    
    def save_technical_indicators(self, indicator_data: Dict[str, Any]):
        """
        동적 기술적 지표 데이터 저장
        """
        # 모든 가능한 지표 컬럼 가져오기
        all_indicators = self.indicator_manager.get_all_indicator_names()
        
        # 기본 컬럼
        base_columns = ['symbol', 'date']
        columns = base_columns + all_indicators
        
        # 플레이스홀더 생성
        placeholders = ', '.join(['?'] * len(columns))
        
        # UPDATE SET 절 생성
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in all_indicators])
        
        # 데이터 준비
        values = [
            indicator_data.get('symbol'),
            indicator_data.get('date')
        ] + [indicator_data.get(col) for col in all_indicators]
        
        try:
            self.conn.execute(f"""
                INSERT INTO stock_data_technical_indicators 
                ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT (symbol, date) DO UPDATE SET
                {update_set}
            """, values)
        except Exception as e:
            print(f"❌ 지표 저장 오류: {e}")
    
    def add_indicator(self, category: str, name: str, indicator_type: str, **kwargs):
        """새로운 지표 추가 (편의 메서드)"""
        config = {"type": indicator_type, **kwargs}
        self.indicator_manager.add_new_indicator(category, name, config)
    
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

class DynamicIndicatorManager:
    """동적 기술적 지표 관리 클래스"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.config_path = "/data/technical_indicators.json"
        self.indicators_config = self._load_indicators_config()
        self._ensure_all_columns()
    
    def _load_indicators_config(self) -> Dict:
        """지표 설정 파일 로드"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print("⚠️ 지표 설정 파일이 없어 기본 설정을 생성합니다.")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict:
        """기본 설정 생성"""
        default_config = {
            "indicators": {
                "existing": {
                    "bb_upper": {"type": "BB_UPPER", "period": 20},
                    "bb_middle": {"type": "BB_MIDDLE", "period": 20},
                    "bb_lower": {"type": "BB_LOWER", "period": 20},
                    "macd": {"type": "MACD", "fast": 12, "slow": 26},
                    "rsi": {"type": "RSI", "period": 14}
                }
            }
        }
        
        # 설정 파일 저장
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        return default_config
    
    def get_all_indicator_names(self) -> List[str]:
        """모든 지표명 반환"""
        indicators = []
        for category in self.indicators_config.get("indicators", {}).values():
            indicators.extend(category.keys())
        return indicators
    
    def _ensure_all_columns(self):
        """모든 지표 컬럼이 테이블에 존재하는지 확인 후 추가"""
        all_indicators = self.get_all_indicator_names()
        
        for indicator_name in all_indicators:
            self._add_column_if_not_exists(indicator_name)
    
    def _add_column_if_not_exists(self, column_name: str):
        """컬럼이 없으면 추가"""
        try:
            # 컬럼 존재 여부 확인
            existing = self.db.conn.execute("""
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_name = 'stock_data_technical_indicators' 
                AND column_name = ?
            """, (column_name,)).fetchone()[0]
            
            if existing == 0:
                self.db.conn.execute(f"""
                    ALTER TABLE stock_data_technical_indicators 
                    ADD COLUMN {column_name} DOUBLE
                """)
                print(f"✅ 새 지표 컬럼 추가: {column_name}")
                
        except Exception as e:
            print(f"⚠️ 컬럼 {column_name} 추가 실패: {e}")
    
    def add_new_indicator(self, category: str, name: str, config: Dict):
        """새로운 지표 추가"""
        # 설정 파일 업데이트
        if category not in self.indicators_config["indicators"]:
            self.indicators_config["indicators"][category] = {}
        
        self.indicators_config["indicators"][category][name] = config
        
        # 설정 파일 저장
        with open(self.config_path, 'w') as f:
            json.dump(self.indicators_config, f, indent=2)
        
        # 테이블에 컬럼 추가
        self._add_column_if_not_exists(name)
        
        print(f"✅ 새 지표 추가됨: {name} ({category})")
