#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FinanceDataReader 기반 대량 데이터 수집기 (yfinance 호환)
- HDD 병목 최적화: 배치 처리, 체크포인트 관리
- 메모리 효율성: 배치 단위 I/O, 가비지 컬렉션 강화
- yfinance 호환성: FDR adj_close를 메인 close로 변환하여 시스템 일관성 확보
- 이중 저장: yfinance 호환 형식(stock_data) + FDR 원본(stock_data_fdr_raw)
"""
- 배치 처리로 I/O 병목 최소화
- 메모리 효율적 처리
"""

import pandas as pd
import FinanceDataReader as fdr
import duckdb
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import threading

class BulkDataCollector:
    """대량 데이터 수집 및 최적화된 저장"""
    
    def __init__(self, db_path: str = "/data/duckdb/stock_data.db", 
                 batch_size: int = 5000, max_workers: int = 4):
        """
        초기화
        
        Args:
            db_path: DuckDB 파일 경로
            batch_size: 배치 크기 (메모리 제한 고려)
            max_workers: 최대 워커 스레드 수
        """
        self.db_path = db_path
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # 디렉토리 생성
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 로깅 설정
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # 쓰기 전용 연결 (단일 스레드)
        self._write_lock = threading.Lock()
        self._write_conn = None
        
        self._initialize_database()
    
    def _initialize_database(self):
        """데이터베이스 초기화 및 최적화 설정"""
        with self._get_write_connection() as conn:
            # 메모리 및 성능 최적화 설정
            conn.execute("PRAGMA memory_limit='1GB'")  # 메모리 제한
            conn.execute("PRAGMA threads=2")           # 스레드 제한
            conn.execute("PRAGMA checkpoint_threshold='1GB'")  # 체크포인트 임계값
            
            # yfinance 호환성을 위한 데이터 구조 생성
            # ⚠️ 중요: yfinance auto_adjust=True와 동일한 스키마 사용
            # close = Adjusted Close (분할/배당 반영)로 시스템 일관성 확보
            
            # 1. 메인 테이블 (yfinance 호환 스키마)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,            -- FDR adj_open (분할/배당 반영)
                    high DOUBLE,            -- FDR adj_high (분할/배당 반영)
                    low DOUBLE,             -- FDR adj_low (분할/배당 반영)
                    close DOUBLE,           -- FDR adj_close (분할/배당 반영) = yfinance close
                    volume BIGINT,          -- FDR volume (원본 유지)
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 2. FDR 원본 데이터 보관용 테이블 (분석 참고용)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data_fdr_raw (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,            -- FDR 원본 Open (분할/배당 미반영)
                    high DOUBLE,            -- FDR 원본 High (분할/배당 미반영)
                    low DOUBLE,             -- FDR 원본 Low (분할/배당 미반영)
                    close DOUBLE,           -- FDR 원본 Close (분할/배당 미반영)
                    volume BIGINT,          -- FDR volume
                    adj_close DOUBLE,       -- FDR Adj Close (분할/배당 반영)
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 인덱스 생성 (yfinance 호환 테이블과 FDR 원본 테이블)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_data_symbol_date 
                ON stock_data(symbol, date)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_data_fdr_raw_symbol_date 
                ON stock_data_fdr_raw(symbol, date)
            """)
            
            self.logger.info("데이터베이스 초기화 완료")
    
    def _get_write_connection(self):
        """쓰기 전용 연결 반환"""
        if self._write_conn is None:
            self._write_conn = duckdb.connect(self.db_path)
        return self._write_conn
    
    def get_nasdaq_symbols(self, limit: Optional[int] = None) -> List[str]:
        """
        나스닥 종목 리스트 가져오기
        
        Args:
            limit: 제한할 종목 수 (테스트용)
        
        Returns:
            종목 심볼 리스트
        """
        try:
            # FinanceDataReader로 나스닥 종목 리스트 가져오기
            self.logger.info("나스닥 종목 리스트 수집 중...")
            nasdaq_df = fdr.StockListing('NASDAQ')
            
            # 활성 종목만 필터링 (거래량 있는 종목)
            active_symbols = nasdaq_df['Symbol'].tolist()
            
            if limit:
                active_symbols = active_symbols[:limit]
            
            self.logger.info(f"수집된 나스닥 종목 수: {len(active_symbols)}")
            return active_symbols
            
        except Exception as e:
            self.logger.error(f"나스닥 종목 리스트 수집 실패: {e}")
            return []
    
    def collect_single_symbol_data(self, symbol: str, 
                                 start_date: str = '2019-01-01',
                                 end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        단일 종목 데이터 수집
        
        Args:
            symbol: 종목 심볼
            start_date: 시작 날짜
            end_date: 종료 날짜
        
        Returns:
            데이터프레임 또는 None
        """
        try:
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            # FinanceDataReader로 데이터 수집
            df = fdr.DataReader(symbol, start_date, end_date)
            
            if df.empty:
                self.logger.warning(f"종목 {symbol}: 데이터 없음")
                return None
            
            # 인덱스를 날짜 컬럼으로 변환
            df = df.reset_index()
            df['symbol'] = symbol
            
            # 컬럼명 통일 (FinanceDataReader 컬럼명 확인)
            if 'Date' in df.columns:
                df.rename(columns={'Date': 'date'}, inplace=True)
            elif df.index.name == 'Date' or 'date' not in df.columns:
                # 인덱스가 날짜인 경우 다시 처리
                if 'date' not in df.columns:
                    df['date'] = df.index
            
            # 나머지 컬럼명 통일 - FDR 실제 컬럼명에 맞춤
            column_mapping = {
                'Open': 'open',
                'High': 'high', 
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Adj Close': 'adj_close'  # FDR의 실제 컬럼명
            }
            
            df.rename(columns=column_mapping, inplace=True)
            
            # yfinance 호환성을 위한 데이터 변환
            # ⚠️ 중요: FDR adj_close를 메인 close로 사용하여 yfinance와 일관성 확보
            
            # Adj Close 기반 OHLC 조정 계산 (yfinance와 동일한 방식)
            if 'adj_close' in df.columns and 'close' in df.columns:
                # 조정 비율 계산 (adj_close / close)
                adjustment_ratio = df['adj_close'] / df['close']
                
                # 모든 OHLC 가격을 조정 (yfinance auto_adjust=True와 동일한 로직)
                df['raw_open'] = df['open'].copy()      # 원본 보관
                df['raw_high'] = df['high'].copy()
                df['raw_low'] = df['low'].copy()
                df['raw_close'] = df['close'].copy()
                
                # Adjusted OHLC 계산 (yfinance 방식)
                df['open'] = df['open'] * adjustment_ratio
                df['high'] = df['high'] * adjustment_ratio  
                df['low'] = df['low'] * adjustment_ratio
                df['close'] = df['adj_close']  # close = adj_close (yfinance 방식)
                
                self.logger.debug(f"종목 {symbol}: Adjusted OHLC 변환 완료 (yfinance 호환)")
            
            # 최종 컬럼 정리
            required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
            yfinance_compat_df = df[required_columns]
            
            # 원본 데이터 보관용 (분석 참고)
            raw_columns = ['symbol', 'date', 'raw_open', 'raw_high', 'raw_low', 'raw_close', 'volume', 'adj_close']
            if all(col in df.columns for col in ['raw_open', 'raw_high', 'raw_low', 'raw_close', 'adj_close']):
                raw_df = df[['symbol', 'date'] + [col for col in raw_columns[2:] if col in df.columns]]
                # 컬럼명 정리 (raw_ prefix 제거)
                raw_df.columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adj_close']
            else:
                raw_df = None
            
            # 데이터 타입 최적화 
            yfinance_compat_df['date'] = pd.to_datetime(yfinance_compat_df['date']).dt.date
            yfinance_compat_df['volume'] = yfinance_compat_df['volume'].fillna(0).astype('int64')
            
            # NaN 제거
            yfinance_compat_df = yfinance_compat_df.dropna()
            
            self.logger.info(f"종목 {symbol}: {len(yfinance_compat_df)}개 레코드 수집 (yfinance 호환 형식)")
            
            # 두 개의 데이터프레임 반환: yfinance 호환용, 원본 보관용
            result = {
                'yfinance_compat': yfinance_compat_df,
                'raw_data': raw_df
            }
            return result
            
        except Exception as e:
            self.logger.error(f"종목 {symbol} 데이터 수집 실패: {e}")
            return None
    
    def bulk_insert_data(self, data_batch: List[Dict]):
        """
        yfinance 호환 배치 데이터 삽입 (HDD 최적화)
        
        Args:
            data_batch: {'yfinance_compat': df, 'raw_data': df} 형태의 딕셔너리 리스트
        """
        if not data_batch:
            return
        
        try:
            # yfinance 호환 데이터와 원본 데이터 분리
            yfinance_dfs = []
            raw_dfs = []
            
            for data_dict in data_batch:
                if data_dict is None:
                    continue
                    
                if 'yfinance_compat' in data_dict and data_dict['yfinance_compat'] is not None:
                    yfinance_dfs.append(data_dict['yfinance_compat'])
                
                if 'raw_data' in data_dict and data_dict['raw_data'] is not None:
                    raw_dfs.append(data_dict['raw_data'])
            
            with self._write_lock:
                with self._get_write_connection() as conn:
                    # 1. 메인 테이블에 yfinance 호환 데이터 저장
                    if yfinance_dfs:
                        combined_yfinance_df = pd.concat(yfinance_dfs, ignore_index=True)
                        
                        # stock_data 테이블 (yfinance 호환 스키마)에 저장
                        conn.execute("""
                            INSERT INTO stock_data (symbol, date, open, high, low, close, volume, collected_at)
                            SELECT symbol, date, open, high, low, close, volume, CURRENT_TIMESTAMP
                            FROM combined_yfinance_df
                        """)
                        
                        self.logger.info(f"yfinance 호환 데이터 저장: {len(combined_yfinance_df)} 레코드")
                    
                    # 2. 원본 데이터 보관 (선택적)
                    if raw_dfs:
                        combined_raw_df = pd.concat(raw_dfs, ignore_index=True)
                        
                        # stock_data_fdr_raw 테이블에 원본 데이터 저장
                        conn.execute("""
                            INSERT INTO stock_data_fdr_raw (symbol, date, open, high, low, close, volume, adj_close, collected_at)
                            SELECT symbol, date, open, high, low, close, volume, adj_close, CURRENT_TIMESTAMP
                            FROM combined_raw_df
                        """)
                        
                        self.logger.info(f"FDR 원본 데이터 저장: {len(combined_raw_df)} 레코드")
                    
                    # 주기적 체크포인트 (WAL flush)
                    conn.execute("CHECKPOINT")
                    
            self.logger.info("배치 삽입 완료 (yfinance 호환 + 원본 데이터 보관)")
            
            # 메모리 정리 강화
            if 'combined_yfinance_df' in locals():
                del combined_yfinance_df
            if 'combined_raw_df' in locals():
                del combined_raw_df
            gc.collect()
            
        except Exception as e:
            self.logger.error(f"배치 삽입 실패: {e}")
            raise
    
    def collect_bulk_data(self, symbols: List[str], 
                         start_date: str = '2019-01-01',
                         batch_symbols: int = 50) -> bool:
        """
        대량 데이터 수집 (병렬 처리 + 배치 저장)
        
        Args:
            symbols: 종목 리스트
            start_date: 시작 날짜
            batch_symbols: 배치 처리할 종목 수
        
        Returns:
            성공 여부
        """
        total_symbols = len(symbols)
        processed_count = 0
        failed_count = 0
        
        self.logger.info(f"대량 데이터 수집 시작: {total_symbols}개 종목")
        
        try:
            # 종목을 배치로 나누어 처리
            for i in range(0, total_symbols, batch_symbols):
                batch_symbols_list = symbols[i:i + batch_symbols]
                self.logger.info(f"배치 {i//batch_symbols + 1} 처리 중: {len(batch_symbols_list)}개 종목")
                
                # 병렬 데이터 수집
                data_batch = []
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 작업 제출
                    future_to_symbol = {
                        executor.submit(
                            self.collect_single_symbol_data, 
                            symbol, 
                            start_date
                        ): symbol 
                        for symbol in batch_symbols_list
                    }
                    
                    # 결과 수집
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            df = future.result(timeout=60)  # 60초 타임아웃
                            if df is not None:
                                data_batch.append(df)
                                processed_count += 1
                            else:
                                failed_count += 1
                                
                        except Exception as e:
                            self.logger.error(f"종목 {symbol} 처리 실패: {e}")
                            failed_count += 1
                
                # 배치 데이터 저장
                if data_batch:
                    self.bulk_insert_data(data_batch)
                
                # 진행률 출력
                progress = (i + len(batch_symbols_list)) / total_symbols * 100
                self.logger.info(f"진행률: {progress:.1f}% ({processed_count} 성공, {failed_count} 실패)")
                
                # 메모리 정리
                del data_batch
                gc.collect()
                
                # HDD 부하 방지를 위한 휴식
                time.sleep(1)
            
            self.logger.info(f"대량 데이터 수집 완료: {processed_count} 성공, {failed_count} 실패")
            return True
            
        except Exception as e:
            self.logger.error(f"대량 데이터 수집 실패: {e}")
            return False
    
    def optimize_database(self):
        """데이터베이스 최적화 (수집 완료 후) - yfinance 호환 구조"""
        try:
            with self._write_lock:
                with self._get_write_connection() as conn:
                    self.logger.info("데이터베이스 최적화 시작...")
                    
                    # 1. 최종 체크포인트
                    conn.execute("CHECKPOINT")
                    
                    # 2. 통계 업데이트 (yfinance 호환 테이블과 원본 테이블)
                    conn.execute("ANALYZE stock_data")
                    conn.execute("ANALYZE stock_data_fdr_raw")
                    
                    # 3. 중복 제거 (yfinance 호환 테이블)
                    conn.execute("""
                        CREATE TABLE stock_data_clean AS 
                        SELECT DISTINCT symbol, date, open, high, low, close, volume, 
                               MIN(collected_at) as collected_at
                        FROM stock_data
                        GROUP BY symbol, date, open, high, low, close, volume
                    """)
                    
                    # 4. 원본 테이블 교체
                    conn.execute("DROP TABLE stock_data")
                    conn.execute("ALTER TABLE stock_data_clean RENAME TO stock_data")
                    
                    # 5. 인덱스 재생성 (yfinance 호환 테이블)
                    conn.execute("""
                        CREATE INDEX idx_stock_data_symbol_date 
                        ON stock_data(symbol, date)
                    """)
                    
                    # 6. 원본 데이터 테이블 중복 제거 (선택적)
                    try:
                        conn.execute("""
                            CREATE TABLE stock_data_fdr_raw_clean AS 
                            SELECT DISTINCT symbol, date, open, high, low, close, volume, adj_close,
                                   MIN(collected_at) as collected_at
                            FROM stock_data_fdr_raw
                            GROUP BY symbol, date, open, high, low, close, volume, adj_close
                        """)
                        
                        conn.execute("DROP TABLE stock_data_fdr_raw")
                        conn.execute("ALTER TABLE stock_data_fdr_raw_clean RENAME TO stock_data_fdr_raw")
                        
                        conn.execute("""
                            CREATE INDEX idx_stock_data_fdr_raw_symbol_date 
                            ON stock_data_fdr_raw(symbol, date)
                        """)
                    except Exception as e:
                        self.logger.warning(f"원본 데이터 테이블 최적화 건너뜀: {e}")
                    
                    self.logger.info("데이터베이스 최적화 완료 (yfinance 호환 + FDR 원본)")
                    
        except Exception as e:
            self.logger.error(f"데이터베이스 최적화 실패: {e}")
    
    def get_collection_stats(self) -> Dict:
        """수집 통계 조회 - yfinance 호환 구조 기준"""
        try:
            with duckdb.connect(self.db_path) as conn:
                # yfinance 호환 테이블 기준 통계
                symbol_count = conn.execute("""
                    SELECT COUNT(DISTINCT symbol) FROM stock_data
                """).fetchone()[0]
                
                total_records = conn.execute("""
                    SELECT COUNT(*) FROM stock_data
                """).fetchone()[0]
                
                date_range = conn.execute("""
                    SELECT MIN(date), MAX(date) FROM stock_data
                """).fetchone()
                
                # 최근 수집 시간
                last_collection = conn.execute("""
                    SELECT MAX(collected_at) FROM stock_data
                """).fetchone()[0]
                
                # FDR 원본 데이터 통계 (있는 경우)
                fdr_stats = {}
                try:
                    fdr_records = conn.execute("""
                        SELECT COUNT(*) FROM stock_data_fdr_raw
                    """).fetchone()[0]
                    fdr_stats = {'fdr_raw_records': fdr_records}
                except:
                    fdr_stats = {'fdr_raw_records': 0}
                
                return {
                    'symbols': symbol_count,
                    'total_records': total_records,
                    'date_range': {
                        'start': date_range[0],
                        'end': date_range[1]
                    },
                    'last_collection': last_collection,
                    'data_type': 'yfinance_compatible',
                    **fdr_stats
                }
                
        except Exception as e:
            self.logger.error(f"통계 조회 실패: {e}")
            return {}
    
    def close(self):
        """연결 정리"""
        if self._write_conn:
            self._write_conn.close()
            self._write_conn = None


# 사용 예시 및 테스트 - yfinance 호환 방식
if __name__ == "__main__":
    # 테스트 실행
    collector = BulkDataCollector(
        db_path="/tmp/test_stock_yfinance_compat.db",
        batch_size=1000,
        max_workers=2
    )
    
    try:
        print("🔧 yfinance 호환 FDR 대량 수집기 테스트 시작")
        
        # 소량 테스트 (상위 10개 종목)
        symbols = collector.get_nasdaq_symbols(limit=10)
        
        if symbols:
            print(f"📊 테스트 종목: {symbols}")
            
            success = collector.collect_bulk_data(
                symbols=symbols,
                start_date='2022-01-01',
                batch_symbols=5
            )
            
            if success:
                print("🔧 데이터베이스 최적화 중...")
                collector.optimize_database()
                
                stats = collector.get_collection_stats()
                print(f"📈 수집 통계: {stats}")
                print("✅ yfinance 호환 FDR 수집 테스트 완료")
            else:
                print("❌ 데이터 수집 실패")
        else:
            print("❌ 종목 리스트 가져오기 실패")
    
    finally:
        collector.close()
