#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
샘플 데이터 생성 스크립트
기술적 지표 및 관심종목 테스트용 데이터 생성
"""

import sys
import os
import random
from datetime import datetime, date, timedelta
import pandas as pd

# 프로젝트 경로 추가
sys.path.append('/opt/airflow/common')
sys.path.append('/app/common')

try:
    from database import PostgreSQLManager
    from technical_scanner_postgres import TechnicalScannerPostgreSQL
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)

def generate_sample_nasdaq_symbols():
    """샘플 나스닥 종목 데이터 생성"""
    
    sample_symbols = [
        {'symbol': 'AAPL', 'name': 'Apple Inc.', 'marketCap': '$3.0T', 'sector': 'Technology'},
        {'symbol': 'MSFT', 'name': 'Microsoft Corporation', 'marketCap': '$2.8T', 'sector': 'Technology'},
        {'symbol': 'GOOGL', 'name': 'Alphabet Inc.', 'marketCap': '$1.7T', 'sector': 'Technology'},
        {'symbol': 'AMZN', 'name': 'Amazon.com Inc.', 'marketCap': '$1.5T', 'sector': 'Consumer Discretionary'},
        {'symbol': 'TSLA', 'name': 'Tesla Inc.', 'marketCap': '$800B', 'sector': 'Automotive'},
        {'symbol': 'META', 'name': 'Meta Platforms Inc.', 'marketCap': '$750B', 'sector': 'Technology'},
        {'symbol': 'NVDA', 'name': 'NVIDIA Corporation', 'marketCap': '$1.8T', 'sector': 'Technology'},
        {'symbol': 'NFLX', 'name': 'Netflix Inc.', 'marketCap': '$200B', 'sector': 'Entertainment'},
        {'symbol': 'AMD', 'name': 'Advanced Micro Devices', 'marketCap': '$180B', 'sector': 'Technology'},
        {'symbol': 'INTC', 'name': 'Intel Corporation', 'marketCap': '$150B', 'sector': 'Technology'},
        {'symbol': 'CRM', 'name': 'Salesforce Inc.', 'marketCap': '$250B', 'sector': 'Technology'},
        {'symbol': 'ORCL', 'name': 'Oracle Corporation', 'marketCap': '$320B', 'sector': 'Technology'},
        {'symbol': 'ADBE', 'name': 'Adobe Inc.', 'marketCap': '$280B', 'sector': 'Technology'},
        {'symbol': 'PYPL', 'name': 'PayPal Holdings Inc.', 'marketCap': '$80B', 'sector': 'Financial Services'},
        {'symbol': 'CSCO', 'name': 'Cisco Systems Inc.', 'marketCap': '$200B', 'sector': 'Technology'},
        {'symbol': 'CMCSA', 'name': 'Comcast Corporation', 'marketCap': '$180B', 'sector': 'Telecommunications'},
        {'symbol': 'PEP', 'name': 'PepsiCo Inc.', 'marketCap': '$240B', 'sector': 'Consumer Staples'},
        {'symbol': 'COST', 'name': 'Costco Wholesale Corp', 'marketCap': '$350B', 'sector': 'Consumer Staples'},
        {'symbol': 'AVGO', 'name': 'Broadcom Inc.', 'marketCap': '$600B', 'sector': 'Technology'},
        {'symbol': 'TXN', 'name': 'Texas Instruments Inc.', 'marketCap': '$170B', 'sector': 'Technology'}
    ]
    
    return sample_symbols

def generate_sample_stock_data(symbols: list, days: int = 60):
    """샘플 주가 데이터 생성"""
    
    stock_data = []
    end_date = date.today()
    
    for symbol_info in symbols:
        symbol = symbol_info['symbol']
        
        # 초기 가격 설정
        base_price = random.uniform(50, 500)
        
        for i in range(days):
            current_date = end_date - timedelta(days=days-1-i)
            
            # 주가 변동 시뮬레이션 (랜덤 워크)
            daily_change = random.uniform(-0.05, 0.05)  # -5% ~ +5%
            base_price *= (1 + daily_change)
            
            # OHLCV 데이터 생성
            open_price = base_price * random.uniform(0.98, 1.02)
            close_price = base_price * random.uniform(0.98, 1.02)
            high_price = max(open_price, close_price) * random.uniform(1.0, 1.03)
            low_price = min(open_price, close_price) * random.uniform(0.97, 1.0)
            volume = random.randint(1000000, 50000000)
            
            stock_data.append({
                'symbol': symbol,
                'date': current_date,
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume
            })
    
    return stock_data

def main():
    """메인 실행 함수"""
    
    print("📊 샘플 데이터 생성 시작...")
    
    try:
        # 데이터베이스 매니저 초기화
        db = PostgreSQLManager()
        
        # 1. 나스닥 종목 데이터 생성 및 저장
        print("📈 나스닥 종목 데이터 생성 중...")
        symbols = generate_sample_nasdaq_symbols()
        db.save_nasdaq_symbols(symbols)
        print(f"✅ 나스닥 종목 {len(symbols)}개 저장 완료")
        
        # 2. 주가 데이터 생성 및 저장
        print("💹 주가 데이터 생성 중...")
        stock_data = generate_sample_stock_data(symbols, days=60)
        
        for data in stock_data:
            db.save_stock_data(data)
        
        db.conn.commit()
        print(f"✅ 주가 데이터 {len(stock_data)}개 저장 완료")
        
        # 3. 기술적 스캐너로 관심종목 생성
        print("🔍 관심종목 스캔 중...")
        scanner = TechnicalScannerPostgreSQL()
        
        # 최근 5일간 스캔 실행
        for i in range(5):
            scan_date = date.today() - timedelta(days=i)
            try:
                signals = scanner.update_daily_watchlist(scan_date)
                print(f"📅 {scan_date}: {len(signals)}개 관심종목 발견")
            except Exception as e:
                print(f"⚠️ {scan_date} 스캔 실패: {str(e)}")
        
        # 4. 결과 요약
        print("\n📋 데이터 생성 완료!")
        
        # 저장된 데이터 확인
        symbol_count = db.conn.execute("SELECT COUNT(*) FROM nasdaq_symbols").fetchone()[0]
        stock_count = db.conn.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
        watchlist_count = db.conn.execute("SELECT COUNT(*) FROM daily_watchlist").fetchone()[0]
        
        print(f"  - 나스닥 종목: {symbol_count}개")
        print(f"  - 주가 데이터: {stock_count}개")
        print(f"  - 관심종목: {watchlist_count}개")
        
        # 최신 관심종목 표시
        latest_watchlist = db.conn.execute("""
            SELECT symbol, date, condition_type, condition_value 
            FROM daily_watchlist 
            ORDER BY date DESC, condition_value DESC 
            LIMIT 10
        """).fetchall()
        
        if latest_watchlist:
            print("\n🎯 최신 관심종목 (상위 10개):")
            for w in latest_watchlist:
                print(f"  - {w[0]}: {w[2]} ({w[3]:.3f}) [{w[1]}]")
        
        db.close()
        print("\n🎉 샘플 데이터 생성이 완료되었습니다!")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
