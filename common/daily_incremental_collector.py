#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
일별 증분 데이터 수집기 (yfinance 기반)
- FinanceDataReader 대량 수집 이후 일별 업데이트용
- yfinance auto_adjust=True 사용으로 기존 FDR 데이터와 완벽 호환
- Airflow DAG에서 사용하기 위한 최적화
"""

import pandas as pd
import yfinance as yf
import duckdb
import os
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import threading
from pathlib import Path

class DailyIncrementalCollector:
    """일별 증분 데이터 수집기 (yfinance 기반)"""
    
    def __init__(self, db_path: str = "/data/duckdb/stock_data.db", 
                 max_workers: int = 5, batch_size: int = 50):
        """
        초기화
        
        Args:
            db_path: DuckDB 파일 경로
            max_workers: 최대 워커 스레드 수 (yfinance API 제한 고려)
            batch_size: 배치 크기
        """
        self.db_path = db_path
        self.max_workers = max_workers
        self.batch_size = batch_size
        
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
            conn.execute('SET memory_limit="1GB"')
            conn.execute("SET threads=2")
            
            # yfinance 호환 테이블 확인 및 생성
            # ⚠️ 중요: FinanceDataReader bulk_data_collector와 동일한 스키마 사용
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,            -- yfinance adjusted open (auto_adjust=True)
                    high DOUBLE,            -- yfinance adjusted high (auto_adjust=True)
                    low DOUBLE,             -- yfinance adjusted low (auto_adjust=True)
                    close DOUBLE,           -- yfinance adjusted close (auto_adjust=True)
                    volume BIGINT,          -- yfinance volume
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            # 증분 수집을 위한 메타데이터 테이블
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_collection_log (
                    collection_date DATE,
                    symbols_attempted INTEGER,
                    symbols_success INTEGER,
                    symbols_failed INTEGER,
                    last_data_date DATE,
                    collection_started_at TIMESTAMP,
                    collection_completed_at TIMESTAMP,
                    notes TEXT
                )
            """)
            
            self.logger.info("데이터베이스 초기화 완료 (yfinance 증분 수집)")
    
    def _get_write_connection(self):
        """쓰기 전용 연결 반환"""
        if self._write_conn is None:
            self._write_conn = duckdb.connect(self.db_path)
        return self._write_conn
    
    def get_symbols_for_update(self) -> List[str]:
        """
        업데이트가 필요한 종목 리스트 가져오기
        
        Returns:
            종목 심볼 리스트
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                # 기존 데이터가 있는 종목들 조회
                symbols_df = conn.execute("""
                    SELECT DISTINCT symbol 
                    FROM stock_data 
                    ORDER BY symbol
                """).fetchdf()
                
                if symbols_df.empty:
                    self.logger.warning("기존 데이터가 없습니다. bulk_data_collector로 먼저 데이터를 수집하세요.")
                    return []
                
                symbols = symbols_df['symbol'].tolist()
                self.logger.info(f"업데이트 대상 종목 수: {len(symbols)}")
                return symbols
                
        except Exception as e:
            self.logger.error(f"종목 리스트 조회 실패: {e}")
            return []
    
    def get_last_data_date(self, symbol: str) -> Optional[date]:
        """
        특정 종목의 마지막 데이터 날짜 조회
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            마지막 데이터 날짜 또는 None
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute("""
                    SELECT MAX(date) 
                    FROM stock_data 
                    WHERE symbol = ?
                """, (symbol,)).fetchone()
                
                if result and result[0]:
                    return result[0]
                return None
                
        except Exception as e:
            self.logger.error(f"종목 {symbol} 마지막 날짜 조회 실패: {e}")
            return None
    
    def collect_single_symbol_incremental(self, symbol: str, 
                                        start_date: Optional[date] = None) -> Dict:
        """
        단일 종목 증분 데이터 수집
        
        Args:
            symbol: 종목 심볼
            start_date: 시작 날짜 (None이면 자동 계산)
        
        Returns:
            수집 결과 딕셔너리
        """
        try:
            # 시작 날짜 결정
            if start_date is None:
                last_date = self.get_last_data_date(symbol)
                if last_date:
                    # 마지막 날짜 다음날부터 수집
                    start_date = last_date + timedelta(days=1)
                else:
                    # 데이터가 없으면 최근 5일
                    start_date = datetime.now().date() - timedelta(days=5)
            
            end_date = datetime.now().date()
            
            # 수집할 데이터가 없으면 건너뛰기
            if start_date >= end_date:
                return {
                    'symbol': symbol,
                    'status': 'skipped',
                    'reason': 'no_new_data',
                    'records_added': 0,
                    'last_date': start_date - timedelta(days=1)
                }
            
            # yfinance로 데이터 수집 (auto_adjust=True 기본값)
            ticker = yf.Ticker(symbol)
            
            # 날짜 범위 설정 (yfinance는 문자열 형식 선호)
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')  # 종료일 포함
            
            hist = ticker.history(start=start_str, end=end_str, auto_adjust=True)
            
            if hist.empty:
                return {
                    'symbol': symbol,
                    'status': 'no_data',
                    'reason': 'empty_response',
                    'records_added': 0,
                    'last_date': None
                }
            
            # 데이터 전처리
            hist = hist.reset_index()
            hist['symbol'] = symbol
            
            # 컬럼명 정리 (yfinance auto_adjust=True 사용 시)
            # auto_adjust=True이면 모든 OHLC가 이미 조정된 값
            column_mapping = {
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',  # 이미 adjusted close
                'Volume': 'volume'
            }
            hist.rename(columns=column_mapping, inplace=True)
            
            # 필요한 컬럼만 선택
            required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
            hist = hist[required_columns]
            
            # 데이터 타입 변환
            hist['date'] = pd.to_datetime(hist['date']).dt.date
            hist['volume'] = hist['volume'].fillna(0).astype('int64')
            
            # NaN 제거
            hist = hist.dropna()
            
            if hist.empty:
                return {
                    'symbol': symbol,
                    'status': 'no_data',
                    'reason': 'empty_after_cleaning',
                    'records_added': 0,
                    'last_date': None
                }
            
            # 데이터베이스에 저장 (UPSERT 방식)
            records_added = self._save_incremental_data(hist)
            
            return {
                'symbol': symbol,
                'status': 'success',
                'records_added': records_added,
                'last_date': hist['date'].max(),
                'date_range': f"{hist['date'].min()} ~ {hist['date'].max()}"
            }
            
        except Exception as e:
            return {
                'symbol': symbol,
                'status': 'error',
                'reason': str(e),
                'records_added': 0,
                'last_date': None
            }
    
    def _save_incremental_data(self, df: pd.DataFrame) -> int:
        """
        증분 데이터 저장 (중복 방지)
        
        Args:
            df: 저장할 데이터프레임
            
        Returns:
            실제 추가된 레코드 수
        """
        try:
            with self._write_lock:
                with self._get_write_connection() as conn:
                    # 기존 데이터와 중복 제거를 위한 UPSERT
                    # 1. 임시 테이블에 새 데이터 저장
                    conn.execute("DROP TABLE IF EXISTS temp_new_data")
                    conn.execute("""
                        CREATE TEMP TABLE temp_new_data AS 
                        SELECT * FROM df
                    """)
                    
                    # 2. 기존 데이터 삭제 (같은 symbol, date)
                    conn.execute("""
                        DELETE FROM stock_data 
                        WHERE (symbol, date) IN (
                            SELECT symbol, date FROM temp_new_data
                        )
                    """)
                    
                    # 3. 새 데이터 삽입 (중복시 업데이트)
                    conn.execute("""
                        INSERT INTO stock_data (symbol, date, open, high, low, close, volume, collected_at)
                        SELECT symbol, date, open, high, low, close, volume, CURRENT_TIMESTAMP
                        FROM temp_new_data
                        ON CONFLICT (symbol, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            collected_at = EXCLUDED.collected_at
                    """)
                    
                    # 4. 체크포인트
                    conn.execute("CHECKPOINT")
                    
                    return len(df)
                    
        except Exception as e:
            self.logger.error(f"데이터 저장 실패: {e}")
            return 0
    
    def collect_daily_batch(self, target_date: Optional[date] = None) -> Dict:
        """
        일별 배치 수집 (모든 종목)
        
        Args:
            target_date: 수집할 날짜 (None이면 오늘)
            
        Returns:
            수집 결과 통계
        """
        if target_date is None:
            target_date = datetime.now().date()
        
        collection_start = datetime.now()
        self.logger.info(f"일별 배치 수집 시작: {target_date}")
        
        # 업데이트 대상 종목 조회
        symbols = self.get_symbols_for_update()
        if not symbols:
            return {
                'status': 'failed',
                'reason': 'no_symbols',
                'collection_date': target_date,
                'symbols_attempted': 0,
                'symbols_success': 0,
                'symbols_failed': 0
            }
        
        total_symbols = len(symbols)
        success_count = 0
        failed_count = 0
        results = []
        
        try:
            # 배치 단위로 병렬 처리
            for i in range(0, total_symbols, self.batch_size):
                batch_symbols = symbols[i:i + self.batch_size]
                self.logger.info(f"배치 {i//self.batch_size + 1} 처리 중: {len(batch_symbols)}개 종목")
                
                batch_results = []
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 작업 제출
                    future_to_symbol = {
                        executor.submit(
                            self.collect_single_symbol_incremental, 
                            symbol,
                            target_date  # 특정 날짜부터 수집
                        ): symbol 
                        for symbol in batch_symbols
                    }
                    
                    # 결과 수집
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            result = future.result(timeout=30)  # 30초 타임아웃
                            batch_results.append(result)
                            
                            if result['status'] == 'success':
                                success_count += 1
                                self.logger.info(f"✅ {symbol}: {result['records_added']}개 레코드 추가")
                            elif result['status'] == 'skipped':
                                self.logger.info(f"⏭️ {symbol}: {result['reason']}")
                            else:
                                failed_count += 1
                                self.logger.warning(f"❌ {symbol}: {result['reason']}")
                                
                        except Exception as e:
                            failed_count += 1
                            self.logger.error(f"❌ {symbol} 처리 실패: {e}")
                            batch_results.append({
                                'symbol': symbol,
                                'status': 'error',
                                'reason': f'timeout_or_exception: {e}',
                                'records_added': 0
                            })
                
                results.extend(batch_results)
                
                # 진행률 출력
                progress = (i + len(batch_symbols)) / total_symbols * 100
                self.logger.info(f"진행률: {progress:.1f}% ({success_count} 성공, {failed_count} 실패)")
                
                # API 제한 방지 휴식
                time.sleep(1)
                
                # 메모리 정리
                gc.collect()
            
            collection_end = datetime.now()
            duration = (collection_end - collection_start).total_seconds()
            
            # 수집 로그 저장
            self._save_collection_log(target_date, total_symbols, success_count, failed_count, 
                                    collection_start, collection_end, f"Duration: {duration:.1f}s")
            
            summary = {
                'status': 'completed',
                'collection_date': target_date,
                'symbols_attempted': total_symbols,
                'symbols_success': success_count,
                'symbols_failed': failed_count,
                'duration_seconds': duration,
                'results': results
            }
            
            self.logger.info(f"일별 배치 수집 완료: {success_count}/{total_symbols} 성공 ({duration:.1f}초)")
            return summary
            
        except Exception as e:
            self.logger.error(f"일별 배치 수집 실패: {e}")
            return {
                'status': 'failed',
                'reason': str(e),
                'collection_date': target_date,
                'symbols_attempted': total_symbols,
                'symbols_success': success_count,
                'symbols_failed': failed_count
            }
    
    def _save_collection_log(self, collection_date: date, attempted: int, success: int, failed: int,
                           start_time: datetime, end_time: datetime, notes: str = ""):
        """수집 로그 저장"""
        try:
            with self._write_lock:
                with self._get_write_connection() as conn:
                    conn.execute("""
                        INSERT INTO daily_collection_log 
                        (collection_date, symbols_attempted, symbols_success, symbols_failed, 
                         last_data_date, collection_started_at, collection_completed_at, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (collection_date, attempted, success, failed, collection_date, 
                          start_time, end_time, notes))
                    
                    conn.execute("CHECKPOINT")
                    
        except Exception as e:
            self.logger.error(f"수집 로그 저장 실패: {e}")
    
    def get_collection_stats(self) -> Dict:
        """수집 통계 조회"""
        try:
            with duckdb.connect(self.db_path) as conn:
                # 전체 데이터 통계
                main_stats = conn.execute("""
                    SELECT 
                        COUNT(DISTINCT symbol) as symbol_count,
                        COUNT(*) as total_records,
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date,
                        MAX(collected_at) as last_collection
                    FROM stock_data
                """).fetchone()
                
                # 최근 수집 로그
                recent_logs = conn.execute("""
                    SELECT * FROM daily_collection_log 
                    ORDER BY collection_date DESC 
                    LIMIT 5
                """).fetchdf()
                
                return {
                    'symbol_count': main_stats[0] if main_stats else 0,
                    'total_records': main_stats[1] if main_stats else 0,
                    'date_range': {
                        'start': main_stats[2] if main_stats else None,
                        'end': main_stats[3] if main_stats else None
                    },
                    'last_collection': main_stats[4] if main_stats else None,
                    'recent_collections': recent_logs.to_dict('records') if not recent_logs.empty else []
                }
                
        except Exception as e:
            self.logger.error(f"통계 조회 실패: {e}")
            return {}
    
    def close(self):
        """연결 정리"""
        if self._write_conn:
            self._write_conn.close()
            self._write_conn = None


# 사용 예시 및 테스트 - yfinance 일별 증분 수집
if __name__ == "__main__":
    # 테스트 실행
    collector = DailyIncrementalCollector(
        db_path="/tmp/test_daily_incremental.db",
        max_workers=3,
        batch_size=10
    )
    
    try:
        print("🔄 yfinance 일별 증분 수집기 테스트 시작")
        
        # 일별 배치 수집 실행
        result = collector.collect_daily_batch()
        
        print(f"📊 수집 결과: {result}")
        
        # 통계 확인
        stats = collector.get_collection_stats()
        print(f"📈 데이터베이스 통계: {stats}")
        
        print("✅ yfinance 일별 증분 수집 테스트 완료")
    
    finally:
        collector.close()
