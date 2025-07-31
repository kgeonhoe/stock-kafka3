#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
샘플 주식 데이터 생성기
stock_data_replica.db에 샘플 데이터를 생성하여 기술적 지표 계산을 위한 기본 데이터를 제공합니다.
"""

import duckdb
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

class SampleStockDataCreator:
    def __init__(self, db_path="data/stock_data_replica.db"):
        """샘플 데이터 생성기 초기화"""
        self.db_path = db_path
        
        # 데이터 디렉토리 생성
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # DuckDB 연결
        self.conn = duckdb.connect(db_path)
        print(f"✅ DuckDB 연결: {db_path}")
        
        # 샘플 종목 리스트 (에러 로그에서 확인된 종목들)
        self.sample_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
            'META', 'NVDA', 'NFLX', 'ADBE', 'ORCL',
            'CRM', 'INTC', 'AMD', 'QCOM', 'PYPL'
        ]
        
    def create_tables(self):
        """필요한 테이블들 생성"""
        try:
            # stock_data 테이블 생성
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # nasdaq_symbols 테이블 생성
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS nasdaq_symbols (
                    symbol VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    market_cap BIGINT,
                    sector VARCHAR,
                    industry VARCHAR,
                    market_cap_tier INTEGER,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # technical_indicators 테이블 생성
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS technical_indicators (
                    symbol VARCHAR,
                    date DATE,
                    rsi_14 DOUBLE,
                    macd DOUBLE,
                    macd_signal DOUBLE,
                    macd_histogram DOUBLE,
                    bb_upper DOUBLE,
                    bb_middle DOUBLE,
                    bb_lower DOUBLE,
                    sma_20 DOUBLE,
                    ema_12 DOUBLE,
                    ema_26 DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            print("✅ 모든 테이블 생성 완료")
            
        except Exception as e:
            print(f"❌ 테이블 생성 실패: {e}")
            raise
    
    def fetch_sample_data(self, days=30):
        """Yahoo Finance에서 샘플 데이터 다운로드"""
        print(f"📊 {len(self.sample_symbols)}개 종목의 {days}일 데이터 다운로드 시작...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 10)  # 여유분 추가
        
        all_data = []
        nasdaq_data = []
        
        for i, symbol in enumerate(self.sample_symbols, 1):
            try:
                print(f"  📈 {i}/{len(self.sample_symbols)}: {symbol} 다운로드 중...")
                
                # Yahoo Finance에서 데이터 다운로드
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=end_date)
                
                if hist.empty:
                    print(f"  ⚠️ {symbol}: 데이터 없음")
                    continue
                
                # 종목 정보 가져오기
                try:
                    info = ticker.info
                    company_name = info.get('longName', f'{symbol} Inc.')
                    market_cap = info.get('marketCap', 1000000000)  # 기본값 10억
                    sector = info.get('sector', 'Technology')
                    industry = info.get('industry', 'Software')
                    
                    # 시가총액 티어 계산
                    if market_cap >= 200000000000:  # 2000억 이상
                        tier = 1
                    elif market_cap >= 10000000000:  # 100억 이상
                        tier = 2
                    elif market_cap >= 2000000000:   # 20억 이상
                        tier = 3
                    else:
                        tier = 4
                    
                    nasdaq_data.append({
                        'symbol': symbol,
                        'name': company_name,
                        'market_cap': market_cap,
                        'sector': sector,
                        'industry': industry,
                        'market_cap_tier': tier,
                        'is_active': True,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })
                    
                except Exception as info_error:
                    print(f"  ⚠️ {symbol} 종목정보 가져오기 실패: {info_error}")
                    nasdaq_data.append({
                        'symbol': symbol,
                        'name': f'{symbol} Inc.',
                        'market_cap': 1000000000,
                        'sector': 'Technology',
                        'industry': 'Software',
                        'market_cap_tier': 2,
                        'is_active': True,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })
                
                # 주식 데이터 변환
                for date, row in hist.iterrows():
                    all_data.append({
                        'symbol': symbol,
                        'date': date.date(),
                        'open': float(row['Open']),
                        'high': float(row['High']),
                        'low': float(row['Low']),
                        'close': float(row['Close']),
                        'volume': int(row['Volume']),
                        'created_at': datetime.now()
                    })
                
                print(f"  ✅ {symbol}: {len(hist)} 일 데이터 수집")
                
            except Exception as e:
                print(f"  ❌ {symbol} 데이터 수집 실패: {e}")
                continue
        
        print(f"✅ 총 {len(all_data)} 건의 주식 데이터 수집 완료")
        print(f"✅ 총 {len(nasdaq_data)} 개 종목 정보 수집 완료")
        
        return all_data, nasdaq_data
    
    def insert_data(self, stock_data, nasdaq_data):
        """데이터베이스에 데이터 삽입"""
        try:
            # nasdaq_symbols 데이터 삽입
            if nasdaq_data:
                nasdaq_df = pd.DataFrame(nasdaq_data)
                self.conn.execute("DELETE FROM nasdaq_symbols")  # 기존 데이터 삭제
                self.conn.execute("INSERT INTO nasdaq_symbols SELECT * FROM nasdaq_df")
                print(f"✅ NASDAQ 종목 정보 {len(nasdaq_data)}건 삽입 완료")
            
            # stock_data 삽입
            if stock_data:
                stock_df = pd.DataFrame(stock_data)
                self.conn.execute("DELETE FROM stock_data")  # 기존 데이터 삭제
                self.conn.execute("INSERT INTO stock_data SELECT * FROM stock_df")
                print(f"✅ 주식 데이터 {len(stock_data)}건 삽입 완료")
            
            # 데이터 확인
            result = self.conn.execute("SELECT COUNT(*) as total FROM stock_data").fetchone()
            print(f"📊 stock_data 총 레코드 수: {result[0]:,}")
            
            symbol_count = self.conn.execute("SELECT COUNT(DISTINCT symbol) as count FROM stock_data").fetchone()
            print(f"📊 고유 종목 수: {symbol_count[0]}")
            
            # 샘플 데이터 확인
            samples = self.conn.execute("""
                SELECT symbol, COUNT(*) as records, MIN(date) as start_date, MAX(date) as end_date
                FROM stock_data 
                GROUP BY symbol 
                ORDER BY symbol 
                LIMIT 5
            """).fetchall()
            
            print("\n📋 샘플 데이터:")
            for symbol, records, start_date, end_date in samples:
                print(f"  {symbol}: {records}일 ({start_date} ~ {end_date})")
            
        except Exception as e:
            print(f"❌ 데이터 삽입 실패: {e}")
            raise
    
    def verify_data(self):
        """데이터 검증"""
        try:
            print("\n🔍 데이터 검증 시작...")
            
            # 테이블 존재 확인
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            print(f"✅ 생성된 테이블: {table_names}")
            
            # 각 테이블 레코드 수 확인
            for table_name in ['stock_data', 'nasdaq_symbols']:
                if table_name in table_names:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"  📊 {table_name}: {count:,} 레코드")
            
            # 최근 데이터 확인
            recent_data = self.conn.execute("""
                SELECT symbol, date, close 
                FROM stock_data 
                WHERE date >= CURRENT_DATE - INTERVAL '3 days'
                ORDER BY date DESC, symbol
                LIMIT 10
            """).fetchall()
            
            if recent_data:
                print("\n📅 최근 3일 데이터 샘플:")
                for symbol, date, close in recent_data:
                    print(f"  {symbol}: {date} = ${close:.2f}")
            else:
                print("⚠️ 최근 데이터 없음")
            
            return True
            
        except Exception as e:
            print(f"❌ 데이터 검증 실패: {e}")
            return False
    
    def close(self):
        """연결 종료"""
        if self.conn:
            self.conn.close()
            print("✅ 데이터베이스 연결 종료")

def main():
    """메인 실행 함수"""
    try:
        print("🚀 샘플 주식 데이터 생성 시작")
        print("=" * 50)
        
        # 샘플 데이터 생성기 초기화
        creator = SampleStockDataCreator()
        
        # 1. 테이블 생성
        creator.create_tables()
        
        # 2. 샘플 데이터 다운로드
        stock_data, nasdaq_data = creator.fetch_sample_data(days=30)
        
        # 3. 데이터 삽입
        creator.insert_data(stock_data, nasdaq_data)
        
        # 4. 데이터 검증
        success = creator.verify_data()
        
        # 5. 연결 종료
        creator.close()
        
        if success:
            print("\n🎉 샘플 데이터 생성 완료!")
            print(f"📂 데이터베이스: {creator.db_path}")
            print("📋 생성된 테이블: stock_data, nasdaq_symbols, technical_indicators")
            print("\n이제 컨슈머에서 기술적 지표 계산이 가능합니다! 🚀")
        else:
            print("\n❌ 샘플 데이터 생성 중 오류 발생")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ 전체 프로세스 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
