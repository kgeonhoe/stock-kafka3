#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Yahoo Finance API를 통해 종목 데이터를 가져와서 replica DB에 저장하는 스크립트
"""

import yfinance as yf
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

class StockDataLoader:
    def __init__(self, db_path=None, days=30, read_only=False):
        """초기화 및 DB 연결 설정"""
        if db_path is None:
            db_path = "/data/duckdb/stock_data_replica.db"
        
        self.db_path = db_path
        self.days = days
        self.read_only = read_only
        
        # 테스트할 종목 리스트 (에러 발생 종목들)
        self.symbols_to_load = [
            'AAPL', 'GOOGL', 'NVDA', 'TSLA', 'MSFT', 
            'META', 'NFLX', 'INTC', 'ADBE', 'ORCL', 'CRM'
        ]
        
        if read_only:
            print(f"📂 연결할 DB 경로 (읽기 전용 모드): {db_path}")
        else:
            print(f"📂 연결할 DB 경로: {db_path}")
        
        # DuckDB 연결 (먼저 읽기 전용으로 시도)
        try:
            # 먼저 읽기 전용 모드로 연결 시도
            try:
                self.conn = duckdb.connect(db_path, read_only=True)
                print(f"✅ DuckDB 연결 성공 (읽기 전용 모드): {db_path}")
                print("⚠️ 읽기 전용 모드로 연결되어 데이터 쓰기가 불가능합니다.")
                print("⚠️ 데이터 쓰기를 위해서는 카프카 컨슈머를 재시작해야 합니다.")
                self.read_only = True
                
                # 테이블 정보만 확인
                self.check_tables()
                return
            except Exception as ro_error:
                print(f"⚠️ 읽기 전용 모드 연결 실패: {ro_error}")
                # 읽기 전용 모드로 연결이 실패했고, 사용자가 read_only=False를 설정했을 경우에만 쓰기 모드 시도
                if not read_only:
                    try:
                        self.conn = duckdb.connect(db_path)
                        print(f"✅ DuckDB 연결 성공 (쓰기 모드): {db_path}")
                        self.read_only = False
                        self.check_tables()
                    except Exception as rw_error:
                        print(f"❌ 쓰기 모드 연결 실패: {rw_error}")
                        raise
                else:
                    # 사용자가 read_only=True를 요청했는데 실패한 경우
                    print("❌ 읽기 전용 모드로 연결할 수 없습니다.")
                    raise ro_error
        except Exception as e:
            print(f"❌ DuckDB 연결 실패: {e}")
            raise
    
    def check_tables(self):
        """테이블 존재 여부 확인 및 필요시 생성"""
        try:
            # 테이블 리스트 확인
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0].lower() for table in tables]
            
            print(f"📋 DB에 존재하는 테이블: {table_names}")
            
            # 읽기 전용 모드인 경우 테이블 생성 생략
            if hasattr(self, 'read_only') and self.read_only:
                print("⚠️ 읽기 전용 모드: 테이블 생성 작업 생략")
                
                # 필요한 테이블이 있는지 확인
                if 'stock_data' not in table_names:
                    print("❌ stock_data 테이블이 없습니다. 읽기 전용 모드에서는 생성할 수 없습니다.")
                    return False
                
                if 'nasdaq_symbols' not in table_names:
                    print("❌ nasdaq_symbols 테이블이 없습니다. 읽기 전용 모드에서는 생성할 수 없습니다.")
                    return False
                
                return True
            
            # stock_data 테이블이 없으면 생성 (쓰기 모드에서만)
            if 'stock_data' not in table_names:
                print("🏗️ stock_data 테이블 생성")
                self.conn.execute("""
                    CREATE TABLE stock_data (
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
            
            # nasdaq_symbols 테이블이 없으면 생성 (쓰기 모드에서만)
            if 'nasdaq_symbols' not in table_names:
                print("🏗️ nasdaq_symbols 테이블 생성")
                self.conn.execute("""
                    CREATE TABLE nasdaq_symbols (
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
            
            return True
        
        except Exception as e:
            print(f"❌ 테이블 확인/생성 실패: {e}")
            return False
    
    def load_data_from_yfinance(self):
        """Yahoo Finance API로 데이터 로드"""
        print(f"🚀 Yahoo Finance에서 {len(self.symbols_to_load)} 종목의 {self.days}일 데이터 로드 시작")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days + 10)  # 여유분 추가
        
        all_stock_data = []
        all_symbol_info = []
        
        for i, symbol in enumerate(self.symbols_to_load, 1):
            try:
                print(f"📈 [{i}/{len(self.symbols_to_load)}] {symbol} 데이터 다운로드 중...")
                
                # 1. 종목 데이터 다운로드
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=end_date)
                
                if hist.empty:
                    print(f"⚠️ {symbol}: 데이터 없음")
                    continue
                
                # 2. 주가 데이터 변환
                for date, row in hist.iterrows():
                    all_stock_data.append({
                        'symbol': symbol,
                        'date': date.date(),
                        'open': float(row['Open']),
                        'high': float(row['High']),
                        'low': float(row['Low']),
                        'close': float(row['Close']),
                        'volume': int(row['Volume']),
                        'created_at': datetime.now()
                    })
                
                # 3. 종목 정보 가져오기
                try:
                    info = ticker.info
                    name = info.get('longName', f'{symbol} Inc.')
                    market_cap = info.get('marketCap', 1000000000)
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
                    
                    all_symbol_info.append({
                        'symbol': symbol,
                        'name': name,
                        'market_cap': market_cap,
                        'sector': sector,
                        'industry': industry,
                        'market_cap_tier': tier,
                        'is_active': True,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })
                    
                except Exception as info_error:
                    print(f"⚠️ {symbol} 종목 정보 가져오기 실패: {info_error}")
                    
                    # 기본 정보로 추가
                    all_symbol_info.append({
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
                
                print(f"✅ {symbol}: {len(hist)} 일치 데이터 수집 완료")
                
            except Exception as e:
                print(f"❌ {symbol} 데이터 로드 실패: {e}")
                continue
        
        # 결과 반환
        return all_stock_data, all_symbol_info
    
    def save_to_database(self, stock_data, symbol_info):
        """데이터베이스에 저장"""
        try:
            # 1. 주가 데이터 삭제 및 저장
            if stock_data:
                # 기존 데이터 삭제
                symbols_str = ", ".join([f"'{s}'" for s in self.symbols_to_load])
                self.conn.execute(f"DELETE FROM stock_data WHERE symbol IN ({symbols_str})")
                
                # 새 데이터 저장
                stock_df = pd.DataFrame(stock_data)
                self.conn.execute("INSERT INTO stock_data SELECT * FROM stock_df")
                print(f"✅ {len(stock_data)} 개의 주가 데이터 저장 완료")
            
            # 2. 종목 정보 저장 (UPSERT)
            if symbol_info:
                symbol_df = pd.DataFrame(symbol_info)
                
                # UPSERT 실행
                for index, row in symbol_df.iterrows():
                    try:
                        self.conn.execute(f"""
                            INSERT INTO nasdaq_symbols (
                                symbol, name, market_cap, sector, industry, market_cap_tier, 
                                is_active, created_at, updated_at
                            ) VALUES (
                                '{row['symbol']}', '{row['name'].replace("'", "''")}', {row['market_cap']}, 
                                '{row['sector'].replace("'", "''")}', '{row['industry'].replace("'", "''")}', 
                                {row['market_cap_tier']}, {str(row['is_active']).lower()},
                                '{row['created_at']}', '{row['updated_at']}'
                            )
                            ON CONFLICT (symbol) DO UPDATE SET
                                name = EXCLUDED.name,
                                market_cap = EXCLUDED.market_cap,
                                sector = EXCLUDED.sector,
                                industry = EXCLUDED.industry,
                                market_cap_tier = EXCLUDED.market_cap_tier,
                                is_active = EXCLUDED.is_active,
                                updated_at = EXCLUDED.updated_at
                        """)
                    except Exception as upsert_error:
                        print(f"⚠️ {row['symbol']} 종목 정보 저장 실패: {upsert_error}")
                
                print(f"✅ {len(symbol_info)} 개의 종목 정보 저장 완료")
            
            return True
            
        except Exception as e:
            print(f"❌ 데이터 저장 실패: {e}")
            return False
    
    def verify_data(self):
        """저장된 데이터 검증"""
        try:
            print("\n🔍 저장된 데이터 검증...")
            
            # 1. 테이블별 레코드 수 확인
            stock_count = self.conn.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
            print(f"📊 stock_data 레코드 수: {stock_count:,}")
            
            symbol_count = self.conn.execute("SELECT COUNT(*) FROM nasdaq_symbols").fetchone()[0]
            print(f"📊 nasdaq_symbols 레코드 수: {symbol_count:,}")
            
            # 2. 종목별 데이터 수 확인
            symbol_data = self.conn.execute("""
                SELECT symbol, COUNT(*) as data_count 
                FROM stock_data 
                GROUP BY symbol
                ORDER BY data_count DESC
            """).fetchall()
            
            print("\n📋 종목별 데이터 수:")
            for symbol, count in symbol_data:
                print(f"  {symbol}: {count}일")
            
            # 3. 추가한 종목들 확인
            loaded_symbols = [s[0] for s in symbol_data]
            print(f"\n✅ 로드된 종목: {loaded_symbols}")
            
            missing_symbols = [s for s in self.symbols_to_load if s not in loaded_symbols]
            if missing_symbols:
                print(f"⚠️ 누락된 종목: {missing_symbols}")
            
            return True
            
        except Exception as e:
            print(f"❌ 데이터 검증 실패: {e}")
            return False
    
    def run(self):
        """전체 프로세스 실행"""
        try:
            # 1. 테이블 확인 및 생성
            if not self.check_tables():
                return False
            
            # 2. 데이터 로드
            stock_data, symbol_info = self.load_data_from_yfinance()
            
            if not stock_data:
                print("❌ 데이터 로드 실패")
                return False
            
            # 3. 데이터 저장
            if not self.save_to_database(stock_data, symbol_info):
                return False
            
            # 4. 데이터 검증
            self.verify_data()
            
            # 5. 연결 종료
            self.conn.close()
            print("✅ DuckDB 연결 종료")
            
            return True
            
        except Exception as e:
            print(f"❌ 실행 중 오류 발생: {e}")
            if hasattr(self, 'conn'):
                self.conn.close()
            return False

if __name__ == "__main__":
    print("🚀 Yahoo Finance API를 통한 종목 데이터 로드 시작")
    print("=" * 70)
    
    # 명령행 인수 처리
    db_path = "/data/duckdb/stock_data_replica.db"  # 기본값
    days = 30  # 기본값
    
    # 인수 처리 (db_path days)
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            days = int(sys.argv[2])
        except ValueError:
            print(f"⚠️ 유효하지 않은 일수: {sys.argv[2]}, 기본값 30일 사용")
    
    print(f"📂 DB 경로: {db_path}")
    print(f"📅 데이터 기간: {days}일")
    
    # 디렉토리 존재 확인
    dir_path = os.path.dirname(db_path)
    if not os.path.exists(dir_path) and dir_path:
        try:
            os.makedirs(dir_path, exist_ok=True)
            print(f"📁 디렉토리 생성: {dir_path}")
        except Exception as e:
            print(f"❌ 디렉토리 생성 실패: {e}")
    
    # 로더 실행
    try:
        # 먼저 읽기 전용 모드로 시도
        try:
            print("🔍 읽기 전용 모드로 DB에 접근 시도...")
            loader = StockDataLoader(db_path, days, read_only=True)
            
            # 파일이 있고 접근 가능하지만 다른 프로세스가 잠금 중인 경우
            print("\n🔁 데이터 읽기만 가능합니다. 현재 DB 상태를 확인합니다.")
            loader.verify_data()
            print("\n⚠️ DB가 다른 프로세스에 의해 잠겨 있습니다. 쓰기 작업이 불가능합니다.")
            print("✅ DB 데이터 확인 완료. 쓰기가 필요하면 카프카 컨슈머를 재시작하세요.")
            
        except Exception as ro_error:
            # 읽기 전용 모드로 실패한 경우 쓰기 모드 시도
            print(f"\n🔄 읽기 전용 모드 실패: {ro_error}")
            print("🔄 쓰기 모드로 재시도 중...")
            
            loader = StockDataLoader(db_path, days, read_only=False)
            success = loader.run()
            
            if success:
                print("\n🎉 데이터 로드 완료!")
                print("이제 카프카 컨슈머에서 이 종목들을 테스트할 수 있습니다.")
            else:
                print("\n❌ 데이터 로드 실패")
                sys.exit(1)
    
    except Exception as e:
        print(f"\n❌ DB 접근 실패: {e}")
        print("⚠️ 데이터베이스에 접근할 수 없습니다. 다음을 확인하세요:")
        print("  - DB 파일 경로가 올바른지 확인")
        print("  - 카프카 컨슈머가 DB를 잠그고 있는지 확인")
        print("  - 파일 권한이 올바른지 확인")
        sys.exit(1)
