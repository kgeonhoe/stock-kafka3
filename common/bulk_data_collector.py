#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FinanceDataReader 기반 대량 데이터 수집기 (yfinance 호환) - PostgreSQL 버전
- PostgreSQL 최적화: 배치 처리, 연결 풀링
- 메모리 효율성: 배치 단위 I/O, 가비지 컬렉션 강화
- yfinance 호환성: FDR adj_close를 메인 close로 변환하여 시스템 일관성 확보
"""

import pandas as pd
import FinanceDataReader as fdr
import psycopg2
import psycopg2.extras
import os
import time
import random
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import threading
from database import PostgreSQLManager

class BulkDataCollector:
    """대량 데이터 수집 및 최적화된 저장 (yfinance 호환) - PostgreSQL 버전"""
    
    def __init__(self, batch_size: int = 1000, max_workers: int = 4):
        """
        초기화
        
        Args:
            batch_size: 배치 크기 (메모리 제한 고려)
            max_workers: 최대 워커 스레드 수
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # 로깅 설정
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # 데이터베이스 매니저 초기화
        self.db = PostgreSQLManager()
        
        # 쓰기 전용 연결 (단일 스레드)
        self._write_lock = threading.Lock()
        
        self.logger.info(f"✅ BulkDataCollector 초기화 완료 (배치 크기: {batch_size})")
    
    def collect_nasdaq_symbols(self) -> int:
        """
        NASDAQ 심볼 수집
        
        Returns:
            수집된 심볼 수
        """
        try:
            self.logger.info("📊 NASDAQ 심볼 수집 시작...")
            
            # 오늘 이미 수집되었는지 확인
            if self.db.is_nasdaq_symbols_collected_today():
                self.logger.info("📅 오늘 이미 NASDAQ 심볼이 수집되었습니다.")
                return 0
            
            # FinanceDataReader로 NASDAQ 심볼 수집
            try:
                nasdaq_symbols = fdr.StockListing('NASDAQ')
                if nasdaq_symbols.empty:
                    self.logger.warning("⚠️ NASDAQ 심볼을 가져올 수 없습니다.")
                    return 0
                
                # 필요한 컬럼만 선택하고 이름 변경
                symbols_data = []
                for _, row in nasdaq_symbols.iterrows():
                    symbols_data.append({
                        'symbol': row.get('Symbol', ''),
                        'name': row.get('Name', ''),
                        'marketCap': str(row.get('MarketCap', '')),
                        'sector': row.get('Sector', '')
                    })
                
                # 데이터베이스에 저장
                self.db.save_nasdaq_symbols(symbols_data)
                
                self.logger.info(f"✅ NASDAQ 심볼 수집 완료: {len(symbols_data)}개")
                return len(symbols_data)
                
            except Exception as e:
                self.logger.error(f"❌ NASDAQ 심볼 수집 오류: {e}")
                return 0
                
        except Exception as e:
            self.logger.error(f"❌ NASDAQ 심볼 수집 전체 오류: {e}")
            return 0
    
    def collect_stock_data_single(self, symbol: str, days_back: int = 30) -> bool:
        """
        단일 종목 주가 데이터 수집 (FinanceDataReader 사용)
        
        Args:
            symbol: 종목 심볼
            days_back: 수집할 과거 날짜 수
            
        Returns:
            성공 여부
        """
        try:
            # 종료 날짜 설정 (오늘)
            end_date = date.today()
            start_date = end_date - timedelta(days=days_back)
            
            # 기존 데이터 확인
            existing_dates = self.db.get_existing_dates(symbol, days_back)
            
            try:
                # FinanceDataReader로 데이터 수집 (yfinance 대신)
                self.logger.info(f"� {symbol}: FinanceDataReader로 데이터 수집 중...")
                df = fdr.DataReader(symbol, start=start_date, end=end_date)
                
                if df.empty:
                    self.logger.warning(f"⚠️ {symbol}: 데이터가 없습니다.")
                    return False
                
                # 데이터 변환 (FDR은 다른 컬럼명 사용)
                stock_data_list = []
                for date_idx, row in df.iterrows():
                    data_date = date_idx.date()
                    
                    # 이미 있는 날짜는 건너뛰기
                    if data_date in existing_dates:
                        continue
                    
                    # FDR 컬럼명에 맞게 수정
                    stock_data = {
                        'symbol': symbol,
                        'date': data_date,
                        'open': float(row['Open']) if pd.notna(row['Open']) else None,
                        'high': float(row['High']) if pd.notna(row['High']) else None,
                        'low': float(row['Low']) if pd.notna(row['Low']) else None,
                        'close': float(row['Close']) if pd.notna(row['Close']) else None,  # FDR의 Close는 이미 조정된 가격
                        'volume': int(row['Volume']) if pd.notna(row['Volume']) else None,
                        'timestamp': datetime.now().isoformat()  # timestamp 필드 추가
                    }
                    
                    # None 값이 너무 많으면 건너뛰기
                    if sum(v is None for v in [stock_data['open'], stock_data['high'], 
                                              stock_data['low'], stock_data['close']]) >= 3:
                        continue
                    
                    stock_data_list.append(stock_data)
                
                if stock_data_list:
                    # 배치 저장
                    saved_count = self.db.save_stock_data_batch(stock_data_list)
                    self.logger.info(f"✅ {symbol}: {saved_count}개 레코드 저장")
                    return True
                else:
                    self.logger.info(f"📅 {symbol}: 새로운 데이터 없음")
                    return True
                        
            except Exception as e:
                self.logger.error(f"❌ {symbol}: FinanceDataReader 데이터 수집 오류 - {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ {symbol}: 전체 수집 오류 - {e}")
            return False
    
    def collect_stock_data_batch(self, symbols: List[str], days_back: int = 30) -> Tuple[int, int]:
        """
        배치 주가 데이터 수집
        
        Args:
            symbols: 종목 심볼 리스트
            days_back: 수집할 과거 날짜 수
            
        Returns:
            (성공 수, 실패 수)
        """
        try:
            self.logger.info(f"📊 배치 주가 데이터 수집 시작: {len(symbols)}개 종목")
            
            success_count = 0
            fail_count = 0
            
            # 배치 단위로 처리
            for i in range(0, len(symbols), self.batch_size):
                batch_symbols = symbols[i:i + self.batch_size]
                self.logger.info(f"📦 배치 {i//self.batch_size + 1}: {len(batch_symbols)}개 종목 처리 중...")
                
                # 병렬 처리
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_symbol = {
                        executor.submit(self.collect_stock_data_single, symbol, days_back): symbol
                        for symbol in batch_symbols
                    }
                    
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            success = future.result()
                            if success:
                                success_count += 1
                            else:
                                fail_count += 1
                        except Exception as e:
                            self.logger.error(f"❌ {symbol}: 처리 중 오류 - {e}")
                            fail_count += 1
                
                # 메모리 정리
                gc.collect()
                
                # 배치 간 짧은 대기 (FinanceDataReader는 Rate Limit 없음)
                time.sleep(0.5)  # 0.5초 대기
            
            self.logger.info(f"✅ 배치 수집 완료: 성공 {success_count}개, 실패 {fail_count}개")
            return success_count, fail_count
            
        except Exception as e:
            self.logger.error(f"❌ 배치 수집 전체 오류: {e}")
            return 0, len(symbols)
    
    def get_nasdaq_symbols_from_db(self, limit: Optional[int] = None) -> List[str]:
        """
        데이터베이스에서 NASDAQ 심볼 조회
        
        Args:
            limit: 조회할 최대 개수 (None이면 전체)
            
        Returns:
            심볼 리스트
        """
        try:
            query = "SELECT symbol FROM nasdaq_symbols ORDER BY symbol"
            if limit:
                query += f" LIMIT {limit}"
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    result = cur.fetchall()
                    return [row[0] for row in result]
                    
        except Exception as e:
            self.logger.error(f"❌ 심볼 조회 오류: {e}")
            return []
    
    def get_collection_status(self) -> Dict[str, any]:
        """
        수집 상태 정보 조회
        
        Returns:
            상태 정보 딕셔너리
        """
        try:
            status = {}
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # 심볼 수
                    cur.execute("SELECT COUNT(*) FROM nasdaq_symbols")
                    status['total_symbols'] = cur.fetchone()[0]
                    
                    # 주가 데이터 수
                    cur.execute("SELECT COUNT(*) FROM stock_data")
                    status['total_stock_records'] = cur.fetchone()[0]
                    
                    # 최신 데이터 날짜
                    cur.execute("SELECT MAX(date) FROM stock_data")
                    result = cur.fetchone()
                    status['latest_date'] = result[0] if result[0] else None
                    
                    # 오늘 데이터가 있는 심볼 수
                    today = date.today()
                    cur.execute("SELECT COUNT(DISTINCT symbol) FROM stock_data WHERE date = %s", (today,))
                    status['symbols_with_today_data'] = cur.fetchone()[0]
                    
                    # 기술적 지표 데이터 수
                    cur.execute("SELECT COUNT(*) FROM stock_data_technical_indicators")
                    status['technical_indicators_records'] = cur.fetchone()[0]
            
            return status
            
        except Exception as e:
            self.logger.error(f"❌ 상태 조회 오류: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """
        오래된 데이터 정리
        
        Args:
            days_to_keep: 보관할 날짜 수
            
        Returns:
            삭제된 레코드 수
        """
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    # 주가 데이터 삭제
                    cur.execute("DELETE FROM stock_data WHERE date < %s", (cutoff_date,))
                    stock_deleted = cur.rowcount
                    
                    # 기술적 지표 데이터 삭제
                    cur.execute("DELETE FROM stock_data_technical_indicators WHERE date < %s", (cutoff_date,))
                    indicators_deleted = cur.rowcount
                    
                    conn.commit()
                    
                    total_deleted = stock_deleted + indicators_deleted
                    self.logger.info(f"✅ 오래된 데이터 정리 완료: {total_deleted}개 레코드 삭제")
                    return total_deleted
                    
        except Exception as e:
            self.logger.error(f"❌ 데이터 정리 오류: {e}")
            return 0
    
    def close(self):
        """리소스 정리"""
        try:
            if hasattr(self, 'db'):
                self.db.close()
            self.logger.info("✅ BulkDataCollector 리소스 정리 완료")
        except Exception as e:
            self.logger.error(f"❌ 리소스 정리 오류: {e}")
    
    def __enter__(self):
        """컨텍스트 매니저 진입"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close()
