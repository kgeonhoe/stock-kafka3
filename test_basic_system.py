#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
간단한 FinanceDataReader 테스트 스크립트
- 기본 동작 확인
- 소량 데이터 수집
"""

import sys
import os
sys.path.insert(0, '/home/grey1/stock-kafka3/common')

import FinanceDataReader as fdr
import pandas as pd
import duckdb
from datetime import datetime

def test_basic_collection():
    """기본 데이터 수집 테스트"""
    print("🚀 FinanceDataReader 기본 테스트 시작")
    
    # 1. 나스닥 종목 리스트 수집
    print("📊 나스닥 종목 리스트 수집 중...")
    nasdaq_df = fdr.StockListing('NASDAQ')
    print(f"✅ 나스닥 종목 수: {len(nasdaq_df)}")
    
    # 2. 상위 3개 종목 데이터 수집
    top_symbols = ['AAPL', 'MSFT', 'NVDA']
    
    print("📈 샘플 종목 데이터 수집 중...")
    for symbol in top_symbols:
        try:
            # 최근 1년 데이터 수집
            df = fdr.DataReader(symbol, '2024-01-01', '2024-12-31')
            print(f"   {symbol}: {len(df)} 레코드")
            print(f"   컬럼: {list(df.columns)}")
            print(f"   인덱스: {df.index.name}")
            print(f"   날짜 범위: {df.index.min()} ~ {df.index.max()}")
            
            # 샘플 데이터 출력
            print(f"   샘플 데이터:")
            print(df.head(2))
            print()
            
        except Exception as e:
            print(f"❌ {symbol} 수집 실패: {e}")
    
    print("✅ 기본 테스트 완료!")

def test_duckdb_integration():
    """DuckDB 연동 테스트"""
    print("💾 DuckDB 연동 테스트 시작")
    
    try:
        # 임시 데이터베이스 생성
        db_path = "/tmp/test_stock.db"
        
        with duckdb.connect(db_path) as conn:
            # 테이블 생성
            conn.execute("""
                CREATE TABLE IF NOT EXISTS test_stock_data (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT
                )
            """)
            
            # AAPL 데이터 수집
            df = fdr.DataReader('AAPL', '2024-12-01', '2024-12-31')
            
            # 데이터 정리
            df = df.reset_index()
            df['symbol'] = 'AAPL'
            
            # 날짜 컬럼 처리 (인덱스가 날짜인 경우)
            if 'Date' in df.columns:
                df['date'] = pd.to_datetime(df['Date']).dt.date
            elif df.index.name == 'Date' or 'date' not in df.columns:
                # 인덱스를 날짜 컬럼으로 변환
                if hasattr(df.index, 'date'):
                    df['date'] = df.index.date
                else:
                    df['date'] = pd.to_datetime(df.index).date
            
            # 컬럼명 소문자로 변경
            df.columns = [col.lower() for col in df.columns]
            
            # 날짜 컬럼이 이미 있는 경우 타입 확인 및 변환
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
            
            # 필요한 컬럼만 선택
            required_cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
            df = df[[col for col in required_cols if col in df.columns]]
            
            print(f"📊 삽입할 데이터: {len(df)} 레코드")
            print(f"컬럼: {list(df.columns)}")
            
            # DuckDB에 삽입
            conn.execute("INSERT INTO test_stock_data SELECT * FROM df")
            
            # 조회 테스트
            result = conn.execute("SELECT COUNT(*) FROM test_stock_data").fetchone()
            print(f"✅ DuckDB 저장 완료: {result[0]} 레코드")
            
            # 데이터베이스 크기 확인
            if os.path.exists(db_path):
                size_mb = os.path.getsize(db_path) / (1024 * 1024)
                print(f"💾 데이터베이스 크기: {size_mb:.2f}MB")
            
    except Exception as e:
        print(f"❌ DuckDB 연동 실패: {e}")
        import traceback
        traceback.print_exc()

def test_parquet_workflow():
    """Parquet 워크플로우 테스트"""
    print("📦 Parquet 워크플로우 테스트 시작")
    
    try:
        # MSFT 데이터 수집
        df = fdr.DataReader('MSFT', '2024-11-01', '2024-12-31')
        df = df.reset_index()
        df['symbol'] = 'MSFT'
        
        # Parquet 파일로 저장
        parquet_path = "/tmp/test_msft.parquet"
        df.to_parquet(parquet_path, index=False)
        
        print(f"✅ Parquet 저장 완료: {parquet_path}")
        
        # 파일 크기 확인
        if os.path.exists(parquet_path):
            size_kb = os.path.getsize(parquet_path) / 1024
            print(f"💾 Parquet 크기: {size_kb:.2f}KB")
        
        # Parquet에서 읽기 테스트
        df_read = pd.read_parquet(parquet_path)
        print(f"📖 Parquet 읽기 완료: {len(df_read)} 레코드")
        
        # DuckDB에서 Parquet 직접 읽기
        with duckdb.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
            print(f"🦆 DuckDB Parquet 읽기: {result[0]} 레코드")
        
    except Exception as e:
        print(f"❌ Parquet 워크플로우 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("=" * 60)
    print("나스닥 데이터 수집 시스템 기본 테스트")
    print("=" * 60)
    print()
    
    # 1. 기본 수집 테스트
    test_basic_collection()
    print()
    
    # 2. DuckDB 연동 테스트
    test_duckdb_integration()
    print()
    
    # 3. Parquet 워크플로우 테스트
    test_parquet_workflow()
    print()
    
    print("🎉 모든 기본 테스트 완료!")
