#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DuckDB 스키마 수정 및 재시작 스크립트
"""

import duckdb
import os
import shutil
from datetime import datetime

def fix_schema_and_restart():
    """스키마 문제 해결 및 시스템 재시작"""
    
    db_path = "/data/duckdb/stock_data.db"
    backup_path = f"/data/duckdb/stock_data_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    
    try:
        print("🔧 DuckDB 스키마 수정 시작...")
        
        # 1. 기존 DB 백업
        if os.path.exists(db_path):
            print(f"📦 기존 DB 백업: {backup_path}")
            shutil.copy2(db_path, backup_path)
            
            # 기존 데이터 확인
            with duckdb.connect(db_path) as conn:
                try:
                    result = conn.execute("SELECT COUNT(*) as record_count, COUNT(DISTINCT symbol) as symbol_count FROM stock_data").fetchone()
                    print(f"📊 기존 데이터: {result[0]:,} 레코드, {result[1]} 종목")
                except:
                    print("⚠️ 기존 데이터 확인 불가 (스키마 오류)")
        
        # 2. 기존 DB 제거
        if os.path.exists(db_path):
            os.remove(db_path)
            print("🗑️ 기존 DB 파일 제거 완료")
        
        # 3. 새로운 스키마로 DB 생성
        with duckdb.connect(db_path) as conn:
            print("🔨 새로운 스키마로 테이블 생성...")
            
            # stock_data 테이블 (PRIMARY KEY 포함)
            conn.execute("""
                CREATE TABLE stock_data (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            # nasdaq_symbols 테이블
            conn.execute("""
                CREATE TABLE nasdaq_symbols (
                    symbol VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    market_cap BIGINT,
                    sector VARCHAR,
                    industry VARCHAR,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # stock_data_fdr_raw 테이블 (FDR 원본 데이터)
            conn.execute("""
                CREATE TABLE stock_data_fdr_raw (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    adj_close DOUBLE,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            # daily_collection_log 테이블
            conn.execute("""
                CREATE TABLE daily_collection_log (
                    collection_date DATE,
                    symbols_processed INTEGER,
                    successful_symbols INTEGER,
                    failed_symbols INTEGER,
                    total_records_added INTEGER,
                    execution_time_seconds DOUBLE,
                    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (collection_date)
                )
            """)
            
            print("✅ 새로운 스키마 생성 완료")
            
            # 테이블 정보 확인
            tables = conn.execute("SHOW TABLES").fetchall()
            print(f"📋 생성된 테이블: {[table[0] for table in tables]}")
            
        print("🎉 스키마 수정 완료!")
        print(f"💾 백업 파일: {backup_path}")
        print("🔄 이제 DAG를 다시 실행할 수 있습니다.")
        
        return True
        
    except Exception as e:
        print(f"❌ 스키마 수정 실패: {e}")
        return False

if __name__ == "__main__":
    fix_schema_and_restart()
