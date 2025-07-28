#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append('/home/grey1/stock-kafka3/common')

from database import DuckDBManager
import time

def add_sample_watchlist():
    """메인 DB에 샘플 watchlist 데이터 추가"""
    print("🎯 Watchlist 샘플 데이터 추가 시작")
    
    # 잠시 후 시도 (Airflow 태스크 완료 대기)
    print("⏳ Airflow 태스크 완료 대기 중...")
    time.sleep(30)
    
    try:
        # DuckDBManager를 사용하여 연결
        db = DuckDBManager("/home/grey1/stock-kafka3/data/duckdb/stock_data_replica.db")
        
        # watchlist 테이블 생성 (존재하지 않는 경우)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS watchlist (
            symbol VARCHAR PRIMARY KEY,
            signal VARCHAR,
            price DOUBLE,
            volume BIGINT,
            bb_upper DOUBLE,
            bb_middle DOUBLE,
            bb_lower DOUBLE,
            bb_width DOUBLE,
            bb_position DOUBLE,
            rsi DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        db.execute_query(create_table_sql)
        print("✅ Watchlist 테이블 생성/확인 완료")
        
        # 기존 데이터 확인
        existing = db.execute_query("SELECT COUNT(*) as count FROM watchlist")
        existing_count = existing.iloc[0]['count'] if not existing.empty else 0
        print(f"📊 기존 watchlist 데이터: {existing_count}개")
        
        # 샘플 데이터 준비
        sample_data = [
            {
                'symbol': 'AAPL',
                'signal': 'BUY',
                'price': 180.50,
                'volume': 50000000,
                'bb_upper': 185.00,
                'bb_middle': 180.00,
                'bb_lower': 175.00,
                'bb_width': 10.00,
                'bb_position': 0.55,
                'rsi': 65.5
            },
            {
                'symbol': 'MSFT',
                'signal': 'BUY',
                'price': 340.25,
                'volume': 30000000,
                'bb_upper': 345.00,
                'bb_middle': 340.00,
                'bb_lower': 335.00,
                'bb_width': 10.00,
                'bb_position': 0.52,
                'rsi': 62.3
            },
            {
                'symbol': 'GOOGL',
                'signal': 'SELL',
                'price': 2800.75,
                'volume': 15000000,
                'bb_upper': 2850.00,
                'bb_middle': 2800.00,
                'bb_lower': 2750.00,
                'bb_width': 100.00,
                'bb_position': 0.51,
                'rsi': 35.2
            },
            {
                'symbol': 'TSLA',
                'signal': 'BUY',
                'price': 250.30,
                'volume': 45000000,
                'bb_upper': 255.00,
                'bb_middle': 248.00,
                'bb_lower': 241.00,
                'bb_width': 14.00,
                'bb_position': 0.67,
                'rsi': 70.8
            },
            {
                'symbol': 'NVDA',
                'signal': 'BUY',
                'price': 450.60,
                'volume': 35000000,
                'bb_upper': 460.00,
                'bb_middle': 448.00,
                'bb_lower': 436.00,
                'bb_width': 24.00,
                'bb_position': 0.61,
                'rsi': 68.4
            }
        ]
        
        # 데이터 삽입 (중복 제거)
        insert_sql = """
        INSERT OR REPLACE INTO watchlist 
        (symbol, signal, price, volume, bb_upper, bb_middle, bb_lower, bb_width, bb_position, rsi, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        
        success_count = 0
        for data in sample_data:
            try:
                db.execute_query(insert_sql, (
                    data['symbol'],
                    data['signal'],
                    data['price'],
                    data['volume'],
                    data['bb_upper'],
                    data['bb_middle'],
                    data['bb_lower'],
                    data['bb_width'],
                    data['bb_position'],
                    data['rsi']
                ))
                success_count += 1
                print(f"✅ {data['symbol']}: {data['signal']} 신호 추가")
            except Exception as e:
                print(f"❌ {data['symbol']} 추가 실패: {e}")
        
        # 결과 확인
        result = db.execute_query("SELECT * FROM watchlist ORDER BY created_at DESC")
        print(f"\n🎉 Watchlist 데이터 추가 완료!")
        print(f"📊 총 {len(result)}개 종목이 watchlist에 등록됨")
        
        # 상세 내용 출력
        print("\n📋 현재 Watchlist:")
        for _, row in result.iterrows():
            print(f"  {row['symbol']}: {row['signal']} - ${row['price']:.2f} (RSI: {row['rsi']:.1f})")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Watchlist 데이터 추가 실패: {e}")
        return False

if __name__ == "__main__":
    add_sample_watchlist()
