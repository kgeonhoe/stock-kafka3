#!/usr/bin/env python3
"""
Redis 관심종목 샘플 데이터 로딩 스크립트

이 스크립트는:
1. stock_data_replica.db에 샘플 데이터 생성
2. Redis에 관심종목 데이터 로딩
3. 실시간 모니터링을 위한 데이터 준비
"""

import sys
import os
sys.path.insert(0, '/home/grey1/stock-kafka3/airflow/plugins')
sys.path.insert(0, '/home/grey1/stock-kafka3/airflow/common')

import redis
import json
import duckdb
from datetime import datetime, timedelta, date
import pandas as pd
from typing import Dict, List, Any


class SampleDataLoader:
    """샘플 데이터 로더"""
    
    def __init__(self, db_path: str = "/home/grey1/stock-kafka3/data/duckdb/stock_data_replica.db"):
        self.db_path = db_path
        self.redis_client = None
        self.db_conn = None
        
    def connect_redis(self):
        """Redis 연결"""
        try:
            self.redis_client = redis.Redis(
                host='localhost',
                port=6379,
                decode_responses=True,
                socket_timeout=5
            )
            # 연결 테스트
            self.redis_client.ping()
            print("✅ Redis 연결 성공")
            return True
        except Exception as e:
            print(f"❌ Redis 연결 실패: {e}")
            return False
    
    def connect_duckdb(self):
        """DuckDB 연결"""
        try:
            # 디렉토리 생성
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            self.db_conn = duckdb.connect(self.db_path)
            print(f"✅ DuckDB 연결 성공: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ DuckDB 연결 실패: {e}")
            return False
    
    def create_sample_tables(self):
        """샘플 테이블 생성"""
        try:
            # 나스닥 심볼 테이블
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS nasdaq_symbols (
                    symbol VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    market_cap BIGINT,
                    sector VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 주가 데이터 테이블
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            # 기술적 지표 테이블
            self.db_conn.execute("""
                CREATE TABLE IF NOT EXISTS technical_indicators (
                    symbol VARCHAR,
                    date DATE,
                    sma_20 DOUBLE,
                    ema_20 DOUBLE,
                    bb_upper DOUBLE,
                    bb_middle DOUBLE,
                    bb_lower DOUBLE,
                    rsi_14 DOUBLE,
                    macd DOUBLE,
                    macd_signal DOUBLE,
                    volume_sma_20 BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (symbol, date)
                )
            """)
            
            print("✅ 샘플 테이블 생성 완료")
            return True
        except Exception as e:
            print(f"❌ 테이블 생성 실패: {e}")
            return False
    
    def insert_sample_data(self):
        """샘플 데이터 삽입"""
        try:
            # 샘플 종목들
            sample_symbols = [
                ('AAPL', 'Apple Inc.', 3500000000000, 'Technology'),
                ('MSFT', 'Microsoft Corporation', 3000000000000, 'Technology'),
                ('GOOGL', 'Alphabet Inc.', 2000000000000, 'Technology'),
                ('AMZN', 'Amazon.com Inc.', 1800000000000, 'Consumer Discretionary'),
                ('TSLA', 'Tesla Inc.', 800000000000, 'Consumer Discretionary'),
                ('META', 'Meta Platforms Inc.', 900000000000, 'Technology'),
                ('NVDA', 'NVIDIA Corporation', 1200000000000, 'Technology'),
                ('NFLX', 'Netflix Inc.', 200000000000, 'Communication Services'),
                ('AMD', 'Advanced Micro Devices', 250000000000, 'Technology'),
                ('INTC', 'Intel Corporation', 180000000000, 'Technology')
            ]
            
            # 종목 정보 삽입
            for symbol, name, market_cap, sector in sample_symbols:
                self.db_conn.execute("""
                    INSERT OR REPLACE INTO nasdaq_symbols (symbol, name, market_cap, sector)
                    VALUES (?, ?, ?, ?)
                """, (symbol, name, market_cap, sector))
            
            print(f"✅ {len(sample_symbols)}개 종목 정보 삽입 완료")
            
            # 샘플 주가 데이터 생성 (최근 30일)
            import random
            base_date = date.today() - timedelta(days=30)
            
            for symbol, _, _, _ in sample_symbols:
                base_price = random.uniform(100, 300)  # 기준 가격
                
                for i in range(30):
                    current_date = base_date + timedelta(days=i)
                    
                    # 가격 변동 시뮬레이션
                    change = random.uniform(-0.05, 0.05)  # ±5% 변동
                    open_price = base_price * (1 + change)
                    high_price = open_price * (1 + random.uniform(0, 0.03))
                    low_price = open_price * (1 - random.uniform(0, 0.03))
                    close_price = open_price * (1 + random.uniform(-0.02, 0.02))
                    volume = random.randint(1000000, 10000000)
                    
                    self.db_conn.execute("""
                        INSERT OR REPLACE INTO stock_data 
                        (symbol, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (symbol, current_date, open_price, high_price, low_price, close_price, volume))
                    
                    # 기술적 지표 샘플 데이터
                    sma_20 = close_price * random.uniform(0.98, 1.02)
                    ema_20 = close_price * random.uniform(0.99, 1.01)
                    bb_middle = sma_20
                    bb_upper = bb_middle * 1.02
                    bb_lower = bb_middle * 0.98
                    rsi_14 = random.uniform(30, 70)
                    macd = random.uniform(-2, 2)
                    macd_signal = macd * random.uniform(0.8, 1.2)
                    
                    self.db_conn.execute("""
                        INSERT OR REPLACE INTO technical_indicators 
                        (symbol, date, sma_20, ema_20, bb_upper, bb_middle, bb_lower, 
                         rsi_14, macd, macd_signal, volume_sma_20)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (symbol, current_date, sma_20, ema_20, bb_upper, bb_middle, bb_lower,
                          rsi_14, macd, macd_signal, volume))
                    
                    base_price = close_price  # 다음날 기준가격으로 사용
            
            print("✅ 30일 주가 및 기술적 지표 데이터 생성 완료")
            return True
            
        except Exception as e:
            print(f"❌ 샘플 데이터 삽입 실패: {e}")
            return False
    
    def load_watchlist_to_redis(self):
        """관심종목 데이터를 Redis에 로딩"""
        try:
            # 1. 기본 종목 리스트 로딩
            symbols_result = self.db_conn.execute("""
                SELECT symbol, name, market_cap, sector 
                FROM nasdaq_symbols 
                ORDER BY market_cap DESC
            """).fetchall()
            
            symbols_data = []
            for row in symbols_result:
                symbols_data.append({
                    'symbol': row[0],
                    'name': row[1],
                    'market_cap': row[2],
                    'sector': row[3]
                })
            
            # Redis에 저장
            self.redis_client.set('watchlist:symbols', json.dumps(symbols_data))
            self.redis_client.expire('watchlist:symbols', 86400)  # 24시간 TTL
            
            print(f"✅ {len(symbols_data)}개 종목 정보를 Redis에 저장")
            
            # 2. 최신 주가 데이터 로딩
            latest_prices = self.db_conn.execute("""
                SELECT s.symbol, s.name, sd.close, sd.volume, sd.date
                FROM nasdaq_symbols s
                JOIN stock_data sd ON s.symbol = sd.symbol
                WHERE sd.date = (
                    SELECT MAX(date) FROM stock_data WHERE symbol = s.symbol
                )
                ORDER BY s.market_cap DESC
            """).fetchall()
            
            price_data = {}
            for row in latest_prices:
                symbol = row[0]
                price_data[symbol] = {
                    'symbol': row[0],
                    'name': row[1],
                    'price': round(row[2], 2),
                    'volume': row[3],
                    'date': str(row[4]),
                    'last_update': datetime.now().isoformat()
                }
            
            # 개별 종목별로 Redis에 저장
            for symbol, data in price_data.items():
                self.redis_client.set(f'price:{symbol}', json.dumps(data))
                self.redis_client.expire(f'price:{symbol}', 3600)  # 1시간 TTL
            
            # 전체 가격 리스트도 저장
            self.redis_client.set('watchlist:prices', json.dumps(price_data))
            self.redis_client.expire('watchlist:prices', 3600)
            
            print(f"✅ {len(price_data)}개 종목 가격 정보를 Redis에 저장")
            
            # 3. 기술적 지표 데이터 로딩
            indicators = self.db_conn.execute("""
                SELECT symbol, sma_20, ema_20, bb_upper, bb_middle, bb_lower, 
                       rsi_14, macd, macd_signal, date
                FROM technical_indicators
                WHERE date = (
                    SELECT MAX(date) FROM technical_indicators WHERE symbol = technical_indicators.symbol
                )
                ORDER BY symbol
            """).fetchall()
            
            indicator_data = {}
            for row in indicators:
                symbol = row[0]
                indicator_data[symbol] = {
                    'symbol': symbol,
                    'sma_20': round(row[1], 2) if row[1] else None,
                    'ema_20': round(row[2], 2) if row[2] else None,
                    'bb_upper': round(row[3], 2) if row[3] else None,
                    'bb_middle': round(row[4], 2) if row[4] else None,
                    'bb_lower': round(row[5], 2) if row[5] else None,
                    'rsi_14': round(row[6], 2) if row[6] else None,
                    'macd': round(row[7], 4) if row[7] else None,
                    'macd_signal': round(row[8], 4) if row[8] else None,
                    'date': str(row[9])
                }
            
            # 기술적 지표 Redis 저장
            for symbol, data in indicator_data.items():
                self.redis_client.set(f'indicators:{symbol}', json.dumps(data))
                self.redis_client.expire(f'indicators:{symbol}', 3600)
            
            self.redis_client.set('watchlist:indicators', json.dumps(indicator_data))
            self.redis_client.expire('watchlist:indicators', 3600)
            
            print(f"✅ {len(indicator_data)}개 종목 기술적 지표를 Redis에 저장")
            
            # 4. 관심종목 메타데이터 저장
            metadata = {
                'total_symbols': len(symbols_data),
                'last_update': datetime.now().isoformat(),
                'data_date': str(date.today()),
                'status': 'active',
                'source': 'sample_data_loader'
            }
            
            self.redis_client.set('watchlist:metadata', json.dumps(metadata))
            self.redis_client.expire('watchlist:metadata', 86400)
            
            print("✅ 관심종목 메타데이터 저장 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ Redis 로딩 실패: {e}")
            return False
    
    def verify_redis_data(self):
        """Redis 데이터 검증"""
        try:
            print("\n📊 Redis 데이터 검증:")
            
            # 1. 키 목록 확인
            keys = self.redis_client.keys('*')
            print(f"✅ 총 {len(keys)}개 키 존재")
            
            # 2. 주요 키 확인
            watchlist_keys = [
                'watchlist:symbols',
                'watchlist:prices', 
                'watchlist:indicators',
                'watchlist:metadata'
            ]
            
            for key in watchlist_keys:
                if self.redis_client.exists(key):
                    ttl = self.redis_client.ttl(key)
                    print(f"✅ {key}: 존재 (TTL: {ttl}초)")
                else:
                    print(f"❌ {key}: 없음")
            
            # 3. 샘플 데이터 확인
            symbols_data = json.loads(self.redis_client.get('watchlist:symbols') or '[]')
            print(f"📈 종목 수: {len(symbols_data)}개")
            
            if symbols_data:
                print(f"🔍 첫 번째 종목: {symbols_data[0]}")
            
            # 4. 개별 종목 데이터 확인
            price_keys = self.redis_client.keys('price:*')
            indicator_keys = self.redis_client.keys('indicators:*')
            print(f"💰 가격 데이터: {len(price_keys)}개")
            print(f"📊 지표 데이터: {len(indicator_keys)}개")
            
            # 5. 샘플 종목 상세 확인
            if price_keys:
                sample_symbol = price_keys[0].split(':')[1]
                price_data = json.loads(self.redis_client.get(f'price:{sample_symbol}'))
                indicator_data = json.loads(self.redis_client.get(f'indicators:{sample_symbol}'))
                
                print(f"\n🔍 {sample_symbol} 상세 데이터:")
                print(f"   가격: ${price_data['price']}")
                print(f"   RSI: {indicator_data['rsi_14']}")
                print(f"   MACD: {indicator_data['macd']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Redis 데이터 검증 실패: {e}")
            return False
    
    def run_sample_loading(self):
        """전체 샘플 로딩 프로세스 실행"""
        print("🚀 Redis 관심종목 샘플 데이터 로딩 시작")
        print("=" * 50)
        
        # 1. 연결
        if not self.connect_redis():
            return False
        if not self.connect_duckdb():
            return False
        
        # 2. 테이블 생성
        if not self.create_sample_tables():
            return False
        
        # 3. 샘플 데이터 생성
        if not self.insert_sample_data():
            return False
        
        # 4. Redis 로딩
        if not self.load_watchlist_to_redis():
            return False
        
        # 5. 검증
        if not self.verify_redis_data():
            return False
        
        print("\n🎉 Redis 관심종목 샘플 데이터 로딩 완료!")
        print("📌 이제 Streamlit 모니터링에서 데이터를 확인할 수 있습니다.")
        
        return True
    
    def close(self):
        """리소스 정리"""
        if self.db_conn:
            self.db_conn.close()
        if self.redis_client:
            self.redis_client.close()


def main():
    """메인 실행 함수"""
    loader = SampleDataLoader()
    
    try:
        success = loader.run_sample_loading()
        if success:
            print("\n✅ 모든 작업이 완료되었습니다!")
            print("📱 이제 Streamlit 앱에서 실시간 모니터링을 확인해보세요.")
        else:
            print("\n❌ 작업 중 오류가 발생했습니다.")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다.")
        return 1
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        return 1
    finally:
        loader.close()
    
    return 0


if __name__ == "__main__":
    exit(main())
