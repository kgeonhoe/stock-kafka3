#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import os
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, date
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
from contextlib import contextmanager

class PostgreSQLManager:
    """PostgreSQL 데이터베이스 관리 클래스 (DuckDB 대체)"""
    
    def __init__(self, 
                 host: str = None,
                 port: int = None,
                 user: str = None,
                 password: str = None,
                 database: str = None):
        """
        PostgreSQL 매니저 초기화
        
        Args:
            host: PostgreSQL 호스트 (기본값: 환경변수에서 읽음)
            port: PostgreSQL 포트 (기본값: 환경변수에서 읽음)
            user: PostgreSQL 사용자 (기본값: 환경변수에서 읽음)
            password: PostgreSQL 비밀번호 (기본값: 환경변수에서 읽음)
            database: PostgreSQL 데이터베이스 (기본값: 환경변수에서 읽음)
        """
        # 환경변수에서 연결 정보 읽기
        self.host = host or os.getenv('POSTGRES_STOCK_HOST', 'localhost')
        self.port = port or int(os.getenv('POSTGRES_STOCK_PORT', '5433'))
        self.user = user or os.getenv('POSTGRES_STOCK_USER', 'stock_user')
        self.password = password or os.getenv('POSTGRES_STOCK_PASSWORD', 'stock_password')
        self.database = database or os.getenv('POSTGRES_STOCK_DB', 'stock_data')
        
        # 연결 파라미터
        self.connection_params = {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database
        }
        
        # 연결 테스트
        self._test_connection()
        
        # 동적 지표 관리자 초기화
        self.indicator_manager = DynamicIndicatorManager(self)
        
        print(f"✅ PostgreSQL 연결 성공: {self.host}:{self.port}/{self.database}")
    
    def _test_connection(self):
        """연결 테스트"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    if result[0] != 1:
                        raise Exception("연결 테스트 실패")
        except Exception as e:
            raise Exception(f"PostgreSQL 연결 실패: {e}")
    
    @contextmanager
    def get_connection(self):
        """컨텍스트 매니저를 사용한 연결 관리"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
    
    def save_nasdaq_symbols(self, symbols: List[Dict[str, Any]]):
        """
        나스닥 심볼 데이터 저장
        
        Args:
            symbols: 나스닥 심볼 데이터 리스트
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                now = datetime.now()
                for symbol_data in symbols:
                    cur.execute("""
                        INSERT INTO nasdaq_symbols (symbol, name, market_cap, sector, collected_at)
                        VALUES (%s, %s, %s, %s, %s)
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
                conn.commit()
    
    def is_nasdaq_symbols_collected_today(self) -> bool:
        """
        오늘 날짜에 NASDAQ 심볼이 이미 수집되었는지 확인
        
        Returns:
            오늘 수집되었으면 True, 아니면 False
        """
        today = date.today()
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) as count 
                    FROM nasdaq_symbols 
                    WHERE collected_at::date = %s
                """, (today,))
                
                result = cur.fetchone()
                count = result[0] if result else 0
                return count > 0
    
    def get_nasdaq_symbols_last_collected_date(self):
        """
        NASDAQ 심볼이 마지막으로 수집된 날짜 조회
        
        Returns:
            마지막 수집 날짜 또는 None
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT collected_at::date as last_date 
                    FROM nasdaq_symbols 
                    ORDER BY collected_at DESC 
                    LIMIT 1
                """)
                
                result = cur.fetchone()
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
            from datetime import timedelta
            
            # 최근 N일간의 날짜 범위
            end_date = date.today()
            start_date = end_date - timedelta(days=days_back)
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT date FROM stock_data
                        WHERE symbol = %s AND date >= %s AND date <= %s
                        ORDER BY date DESC
                    """, (symbol, start_date, end_date))
                    
                    result = cur.fetchall()
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
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT MAX(date) FROM stock_data
                        WHERE symbol = %s
                    """, (symbol,))
                    
                    result = cur.fetchone()
                    
                    if result and result[0]:
                        date_value = result[0]
                        # 이미 날짜 객체인 경우 그대로 반환
                        if hasattr(date_value, 'year'):
                            return date_value
                        # 문자열인 경우 날짜 객체로 변환
                        elif isinstance(date_value, str):
                            from datetime import datetime
                            return datetime.strptime(date_value, '%Y-%m-%d').date()
                        else:
                            print(f"⚠️ {symbol}: 예상치 못한 날짜 형식 - {type(date_value)}: {date_value}")
                            return None
                    else:
                        return None
            
        except Exception as e:
            print(f"⚠️ {symbol}: 최신 날짜 조회 오류 - {e}")
            return None

    def save_stock_data(self, stock_data: Dict[str, Any]):
        """
        주가 데이터 저장
        
        Args:
            stock_data: 주가 데이터
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
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
                conn.commit()
    
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
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, """
                        INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                    """, batch_values)
                    conn.commit()
            
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
        placeholders = ', '.join(['%s'] * len(columns))
        
        # UPDATE SET 절 생성
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in all_indicators])
        
        # 데이터 준비
        values = [
            indicator_data.get('symbol'),
            indicator_data.get('date')
        ] + [indicator_data.get(col) for col in all_indicators]
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO stock_data_technical_indicators 
                        ({', '.join(columns)})
                        VALUES ({placeholders})
                        ON CONFLICT (symbol, date) DO UPDATE SET
                        {update_set}
                    """, values)
                    conn.commit()
        except Exception as e:
            print(f"❌ 지표 저장 오류: {e}")
    
    def add_indicator(self, category: str, name: str, indicator_type: str, **kwargs):
        """새로운 지표 추가 (편의 메서드)"""
        config = {"type": indicator_type, **kwargs}
        self.indicator_manager.add_new_indicator(category, name, config)
    
    def get_active_symbols(self) -> List[str]:
        """활성 심볼 목록 조회"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM nasdaq_symbols")
                result = cur.fetchall()
                return [row[0] for row in result]
    
    def execute_query(self, query: str, params=None):
        """
        쿼리 실행 후 결과 반환
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터 (옵션)
            
        Returns:
            결과 리스트 또는 DataFrame
        """
        try:
            if PANDAS_AVAILABLE:
                # psycopg2 연결을 사용하여 pandas DataFrame 생성
                with self.get_connection() as conn:
                    if params:
                        result = pd.read_sql_query(query, conn, params=params)
                    else:
                        result = pd.read_sql_query(query, conn)
                    return result
            else:
                # pandas가 없는 경우 일반 결과 반환
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        if params:
                            cur.execute(query, params)
                        else:
                            cur.execute(query)
                        result = cur.fetchall()
                        
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
        except Exception as e:
            print(f"쿼리 실행 오류: {e}")
            raise
    
    def get_stock_data(self, symbol: str, days: int = 60):
        """
        특정 종목의 주가 데이터 조회
        
        Args:
            symbol: 종목 심볼
            days: 조회할 날짜 수
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, date, open, high, low, close, volume
                    FROM (
                        SELECT symbol, date, open, high, low, close, volume
                        FROM stock_data
                        WHERE symbol = %s
                        ORDER BY date DESC
                        LIMIT %s
                    ) AS recent_data
                    ORDER BY date ASC
                """, (symbol, days))
                
                columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
                results = []
                
                for row in cur.fetchall():
                    stock_dict = {
                        columns[i]: row[i] for i in range(len(columns))
                    }
                    results.append(stock_dict)
                
                return results
    
    def close(self):
        """연결 종료 (컨텍스트 매니저를 사용하므로 실제로는 불필요)"""
        pass

class DynamicIndicatorManager:
    """동적 기술적 지표 관리 클래스 (PostgreSQL 버전)"""
    
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
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # 컬럼 존재 여부 확인
                    cur.execute("""
                        SELECT COUNT(*) FROM information_schema.columns 
                        WHERE table_name = 'stock_data_technical_indicators' 
                        AND column_name = %s
                    """, (column_name,))
                    
                    existing = cur.fetchone()[0]
                    
                    if existing == 0:
                        cur.execute(f"""
                            ALTER TABLE stock_data_technical_indicators 
                            ADD COLUMN {column_name} NUMERIC(12,4)
                        """)
                        conn.commit()
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


# 호환성을 위한 별칭 (기존 DuckDBManager 대신 사용)
DuckDBManager = PostgreSQLManager
