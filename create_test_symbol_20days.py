#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DuckDB에서 특정 종목의 최근 20일 데이터만 유지하는 스크립트
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
import sys

def create_test_symbol_with_20days(db_path='/data/duckdb/stock_data_replica.db', symbol='AAPL'):
    """특정 종목의 최근 20일 데이터만 유지하는 함수"""
    try:
        print(f"🔧 {symbol} 종목의 최근 20일 데이터만 남기는 작업 시작...")
        
        # DuckDB 연결
        conn = duckdb.connect(db_path)
        print(f"✅ DuckDB 연결 성공: {db_path}")
        
        # 1. 먼저 해당 종목이 있는지 확인
        symbol_exists = conn.execute(f"SELECT COUNT(*) FROM stock_data WHERE symbol = '{symbol}'").fetchone()[0]
        
        if symbol_exists > 0:
            print(f"ℹ️ {symbol} 종목 데이터가 이미 존재합니다. 최근 20일만 남기고 삭제합니다.")
            
            # 2. 해당 종목의 최근 20일만 남기고 삭제
            conn.execute(f"""
                DELETE FROM stock_data 
                WHERE symbol = '{symbol}' 
                AND date NOT IN (
                    SELECT date 
                    FROM stock_data 
                    WHERE symbol = '{symbol}'
                    ORDER BY date DESC 
                    LIMIT 20
                )
            """)
            
            # 3. 결과 확인
            count = conn.execute(f"SELECT COUNT(*) FROM stock_data WHERE symbol = '{symbol}'").fetchone()[0]
            print(f"✅ {symbol} 종목의 최근 20일 데이터만 남김: {count}일")
            
            # 4. 남은 데이터 기간 확인
            date_range = conn.execute(f"""
                SELECT MIN(date) as start_date, MAX(date) as end_date
                FROM stock_data
                WHERE symbol = '{symbol}'
            """).fetchone()
            
            print(f"📅 데이터 기간: {date_range[0]} ~ {date_range[1]}")
            
            return True
            
        else:
            print(f"⚠️ {symbol} 종목 데이터가 존재하지 않습니다.")
            
            # AMZN으로부터 데이터를 복사해서 새로운 종목 생성
            amzn_exists = conn.execute("SELECT COUNT(*) FROM stock_data WHERE symbol = 'AMZN'").fetchone()[0]
            
            if amzn_exists > 0:
                print(f"ℹ️ AMZN 데이터에서 복사해서 {symbol} 종목 생성을 시작합니다.")
                
                # AMZN 데이터 가져오기 (최근 20일)
                amzn_data = conn.execute("""
                    SELECT date, open, high, low, close, volume
                    FROM stock_data
                    WHERE symbol = 'AMZN'
                    ORDER BY date DESC
                    LIMIT 20
                """).fetchdf()
                
                # AAPL 데이터 생성 (약간의 변화 추가)
                import numpy as np
                
                for index, row in amzn_data.iterrows():
                    # 약간의 변동성 추가 (±5% 범위)
                    variation = np.random.uniform(0.95, 1.05)
                    
                    conn.execute(f"""
                        INSERT INTO stock_data (symbol, date, open, high, low, close, volume, created_at)
                        VALUES (
                            '{symbol}',
                            '{row['date']}',
                            {row['open'] * variation},
                            {row['high'] * variation},
                            {row['low'] * variation},
                            {row['close'] * variation},
                            {int(row['volume'] * variation)},
                            CURRENT_TIMESTAMP
                        )
                    """)
                
                # 결과 확인
                count = conn.execute(f"SELECT COUNT(*) FROM stock_data WHERE symbol = '{symbol}'").fetchone()[0]
                print(f"✅ {symbol} 종목 생성 완료: {count}일 데이터")
                
                # 데이터 기간 확인
                date_range = conn.execute(f"""
                    SELECT MIN(date) as start_date, MAX(date) as end_date
                    FROM stock_data
                    WHERE symbol = '{symbol}'
                """).fetchone()
                
                print(f"📅 데이터 기간: {date_range[0]} ~ {date_range[1]}")
                
                # nasdaq_symbols 테이블에도 추가
                conn.execute(f"""
                    INSERT INTO nasdaq_symbols 
                    (symbol, name, market_cap, sector, industry, market_cap_tier, is_active, created_at, updated_at)
                    VALUES (
                        '{symbol}',
                        'Apple Inc.',
                        2500000000000,
                        'Technology',
                        'Consumer Electronics',
                        1,
                        true,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (symbol) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP
                """)
                
                return True
            else:
                print("❌ AMZN 데이터가 없어서 샘플 생성 불가")
                return False
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
            print("✅ DuckDB 연결 종료")

if __name__ == "__main__":
    db_path = '/data/duckdb/stock_data_replica.db'
    symbol = 'AAPL'  # 기본값
    
    # 명령행 인자로 종목을 받을 수도 있음
    if len(sys.argv) > 1:
        symbol = sys.argv[1].upper()
    
    success = create_test_symbol_with_20days(db_path, symbol)
    
    if success:
        print(f"🎉 {symbol} 종목의 20일 테스트 데이터 준비 완료!")
        print("이제 카프카 컨슈머에서 이 종목을 테스트해보세요.")
    else:
        print("❌ 테스트 데이터 준비 실패")
        sys.exit(1)
