#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
날짜 데이터 복구 스크립트
1970-01-01로 잘못 저장된 날짜를 올바른 날짜로 복구합니다.
"""

import duckdb
import pandas as pd
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime, timedelta
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_date_data():
    """날짜 데이터 복구"""
    
    db_path = '/home/grey1/stock-kafka3/data/duckdb/stock_data_replica.db'
    
    try:
        # 데이터베이스 연결
        con = duckdb.connect(database=db_path, read_only=False)
        
        # 1. 잘못된 데이터 확인
        logger.info("=== 현재 데이터 상태 확인 ===")
        wrong_date_count = con.execute("""
            SELECT COUNT(*) FROM stock_data WHERE date = '1970-01-01'
        """).fetchone()[0]
        logger.info(f"잘못된 날짜 데이터: {wrong_date_count:,}개")
        
        # 2. 샘플 종목으로 테스트 (AAPL)
        logger.info("\n=== 샘플 종목 데이터 복구 테스트 (AAPL) ===")
        
        # 현재 AAPL 레코드 수 확인
        aapl_count = con.execute("""
            SELECT COUNT(*) FROM stock_data WHERE symbol = 'AAPL'
        """).fetchone()[0]
        logger.info(f"AAPL 현재 레코드 수: {aapl_count}")
        
        # 3. FinanceDataReader로 AAPL 5년 데이터 다시 수집 (날짜 제대로)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=5*365)
        
        logger.info(f"AAPL 데이터 재수집 중... ({start_date} ~ {end_date})")
        
        # FDR로 데이터 수집
        try:
            df_fdr = fdr.DataReader('AAPL', start_date, end_date)
            
            if not df_fdr.empty:
                # 인덱스를 date 컬럼으로 변환
                df_fdr.reset_index(inplace=True)
                df_fdr['symbol'] = 'AAPL'
                
                # 컬럼명 정리
                df_fdr.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'change', 'symbol']
                df_fdr = df_fdr[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
                
                # 날짜 형식 확인
                logger.info(f"재수집된 데이터 샘플:")
                logger.info(f"- 날짜 범위: {df_fdr['date'].min()} ~ {df_fdr['date'].max()}")
                logger.info(f"- 레코드 수: {len(df_fdr)}")
                logger.info(f"- 첫 5개 날짜: {df_fdr['date'].head().tolist()}")
                
                # 4. 기존 AAPL 데이터 삭제
                con.execute("DELETE FROM stock_data WHERE symbol = 'AAPL'")
                logger.info("기존 AAPL 데이터 삭제 완료")
                
                # 5. 새 데이터 삽입
                con.execute("""
                    INSERT INTO stock_data (symbol, date, open, high, low, close, volume, collected_at)
                    SELECT symbol, date, open, high, low, close, volume, CURRENT_TIMESTAMP
                    FROM df_fdr
                """)
                logger.info(f"새 AAPL 데이터 삽입 완료: {len(df_fdr)}개")
                
                # 6. 결과 확인
                new_aapl_data = con.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(date) as min_date,
                        MAX(date) as max_date,
                        COUNT(DISTINCT date) as unique_dates
                    FROM stock_data 
                    WHERE symbol = 'AAPL'
                """).fetchone()
                
                logger.info("=== AAPL 복구 결과 ===")
                logger.info(f"총 레코드: {new_aapl_data[0]}")
                logger.info(f"날짜 범위: {new_aapl_data[1]} ~ {new_aapl_data[2]}")
                logger.info(f"고유 날짜 수: {new_aapl_data[3]}")
                
            else:
                logger.error("AAPL 데이터 수집 실패 - 빈 DataFrame")
                
        except Exception as e:
            logger.error(f"AAPL 데이터 재수집 오류: {e}")
        
        con.close()
        logger.info("날짜 복구 테스트 완료")
        
    except Exception as e:
        logger.error(f"날짜 복구 오류: {e}")

def create_full_recovery_plan():
    """전체 데이터 복구 계획 생성"""
    
    db_path = '/home/grey1/stock-kafka3/data/duckdb/stock_data_replica.db'
    
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        
        # 모든 종목 목록 조회
        symbols = con.execute("""
            SELECT DISTINCT symbol FROM stock_data 
            ORDER BY symbol
        """).fetchall()
        
        logger.info(f"=== 전체 복구 계획 ===")
        logger.info(f"복구 대상 종목 수: {len(symbols)}")
        logger.info(f"예상 소요 시간: {len(symbols) * 2 / 60:.1f}분 (종목당 2초)")
        
        # 배치 단위로 나누기 (20개씩)
        batch_size = 20
        batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]
        logger.info(f"배치 수: {len(batches)}개 (배치당 {batch_size}개 종목)")
        
        con.close()
        
        return batches
        
    except Exception as e:
        logger.error(f"복구 계획 생성 오류: {e}")
        return []

if __name__ == "__main__":
    logger.info("날짜 데이터 복구 시작")
    
    # 1. 샘플 테스트
    fix_date_data()
    
    # 2. 전체 복구 계획
    batches = create_full_recovery_plan()
    
    if batches:
        response = input(f"\n전체 {len([s for batch in batches for s in batch])}개 종목을 복구하시겠습니까? (y/n): ")
        if response.lower() == 'y':
            logger.info("전체 복구는 별도 스크립트로 실행해주세요.")
        else:
            logger.info("복구를 취소했습니다.")
