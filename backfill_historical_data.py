#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yfinance as yf
import sys
import time
import gc
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import concurrent.futures
import random

# 프로젝트 경로 추가
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
from database import DuckDBManager

class HistoricalDataBackfiller:
    """과거 데이터 백필 클래스"""
    
    def __init__(self, db_path: str = "/home/grey1/stock-kafka3/data/duckdb/stock_data.db"):
        """
        백필 클래스 초기화
        
        Args:
            db_path: DuckDB 파일 경로
        """
        self.db = DuckDBManager(db_path)
        
        # 백필 연도 설정 (최근 1년 제외하고 4년)
        current_year = date.today().year
        self.backfill_years = [
            current_year - 5,  # 2020년
            current_year - 4,  # 2021년  
            current_year - 3,  # 2022년
            current_year - 2,  # 2023년
        ]
        
        print(f"🎯 백필 대상 연도: {self.backfill_years}")
    
    def analyze_missing_data(self, symbols: List[str]) -> Dict[str, List[int]]:
        """
        각 종목별로 누락된 연도 분석
        
        Args:
            symbols: 분석할 종목 리스트
            
        Returns:
            종목별 누락 연도 딕셔너리
        """
        print("🔍 종목별 누락 데이터 분석 중...")
        
        missing_data = {}
        
        for i, symbol in enumerate(symbols[:10]):  # 샘플 10개만 먼저 분석
            if i % 5 == 0:
                print(f"  📊 분석 진행: {i}/{min(10, len(symbols))} 종목")
            
            missing_years = []
            
            for year in self.backfill_years:
                # 해당 연도 데이터가 있는지 확인
                start_date = date(year, 1, 1)
                end_date = date(year, 12, 31)
                
                result = self.db.conn.execute("""
                    SELECT COUNT(*) as count 
                    FROM stock_data 
                    WHERE symbol = ? AND date >= ? AND date <= ?
                """, (symbol, start_date, end_date)).fetchone()
                
                count = result[0] if result else 0
                
                if count < 50:  # 1년에 50일 미만이면 누락으로 판단
                    missing_years.append(year)
            
            if missing_years:
                missing_data[symbol] = missing_years
                print(f"  🔍 {symbol}: {missing_years}년 누락")
            else:
                print(f"  ✅ {symbol}: 모든 데이터 보유")
        
        return missing_data
    
    def backfill_symbol_year(self, symbol: str, year: int) -> bool:
        """
        특정 종목의 특정 연도 데이터 백필
        
        Args:
            symbol: 종목 심볼
            year: 백필할 연도
            
        Returns:
            성공 여부
        """
        try:
            # API 제한 방지 지연
            time.sleep(random.uniform(1.0, 2.5))
            
            # 연도별 날짜 범위 설정
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            
            print(f"📊 {symbol} {year}년 백필 시작...")
            
            # yfinance로 데이터 수집
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date)
            
            if hist.empty:
                print(f"⚠️ {symbol} {year}년: 데이터 없음")
                return False
            
            # 데이터 정제
            hist = hist.reset_index()
            hist['symbol'] = symbol
            hist.columns = [col.lower().replace(' ', '_') for col in hist.columns]
            
            if 'date' in hist.columns:
                hist['date'] = hist['date'].dt.date
            
            required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
            hist = hist[required_columns].dropna()
            
            if len(hist) == 0:
                print(f"⚠️ {symbol} {year}년: 정제 후 데이터 없음")
                return False
            
            # 배치 저장
            batch_data = []
            for _, row in hist.iterrows():
                try:
                    stock_data = {
                        'symbol': row['symbol'],
                        'date': row['date'],
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': int(row['volume'])
                    }
                    batch_data.append(stock_data)
                except Exception as data_error:
                    continue
            
            if batch_data:
                save_count = self.db.save_stock_data_batch(batch_data)
                print(f"✅ {symbol} {year}년: {save_count}개 데이터 저장 완료")
                return True
            else:
                print(f"❌ {symbol} {year}년: 저장할 데이터 없음")
                return False
                
        except Exception as e:
            error_msg = str(e)
            if "rate limit" in error_msg.lower() or "429" in error_msg:
                print(f"🚫 {symbol} {year}년: API 제한 - 대기 후 스킵")
                time.sleep(random.uniform(15, 25))
                return False
            else:
                print(f"💥 {symbol} {year}년: 백필 실패 - {error_msg}")
                return False
    
    def backfill_by_year(self, symbols: List[str], target_year: int, max_workers: int = 1, batch_size: int = 5):
        """
        특정 연도의 모든 종목 백필
        
        Args:
            symbols: 백필할 종목 리스트
            target_year: 백필할 연도
            max_workers: 병렬 워커 수
            batch_size: 배치 크기
        """
        print(f"\n🚀 {target_year}년 백필 시작 ({len(symbols)}개 종목)")
        print(f"⚙️ 설정: 워커 {max_workers}개, 배치 {batch_size}개")
        
        start_time = time.time()
        total_success = 0
        total_fail = 0
        
        # 배치별 처리
        for batch_start in range(0, len(symbols), batch_size):
            batch_end = min(batch_start + batch_size, len(symbols))
            batch_symbols = symbols[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (len(symbols) + batch_size - 1) // batch_size
            
            print(f"\n📦 배치 {batch_num}/{total_batches}: {len(batch_symbols)}개 종목")
            batch_start_time = time.time()
            
            success_count = 0
            fail_count = 0
            
            # 순차 처리 (API 안정성 위해)
            if max_workers == 1:
                for symbol in batch_symbols:
                    success = self.backfill_symbol_year(symbol, target_year)
                    if success:
                        success_count += 1
                    else:
                        fail_count += 1
            else:
                # 병렬 처리
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_symbol = {
                        executor.submit(self.backfill_symbol_year, symbol, target_year): symbol 
                        for symbol in batch_symbols
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            success = future.result()
                            if success:
                                success_count += 1
                            else:
                                fail_count += 1
                        except Exception as e:
                            fail_count += 1
                            print(f"💥 {symbol}: 예외 - {e}")
            
            batch_duration = time.time() - batch_start_time
            total_success += success_count
            total_fail += fail_count
            
            print(f"✅ 배치 {batch_num} 완료: {success_count}/{len(batch_symbols)} 성공 ({batch_duration:.1f}초)")
            print(f"📊 {target_year}년 누적: {batch_end}/{len(symbols)} 종목 ({total_success}개 성공, {total_fail}개 실패)")
            
            # 메모리 정리
            gc.collect()
            
            # 배치 간 휴식
            if batch_end < len(symbols):
                print(f"⏸ 배치 간 휴식 (15초)...")
                time.sleep(15)
        
        elapsed_time = time.time() - start_time
        print(f"\n🎉 {target_year}년 백필 완료!")
        print(f"📊 결과: {total_success}/{len(symbols)} 성공 ({elapsed_time:.1f}초)")
        print(f"⚡ 속도: {len(symbols)/elapsed_time:.2f} 종목/초")
    
    def full_backfill(self, symbols: List[str] = None):
        """
        전체 4년 백필 실행
        
        Args:
            symbols: 백필할 종목 리스트 (None이면 DB에서 조회)
        """
        if symbols is None:
            symbols = self.db.get_active_symbols()
            if not symbols:
                print("❌ 활성 심볼이 없습니다.")
                return
        
        print("🚀 4년 과거 데이터 백필 시작!")
        print(f"📋 대상: {len(symbols)}개 종목")
        print(f"📅 기간: {self.backfill_years}")
        
        overall_start = time.time()
        
        for year in self.backfill_years:
            print(f"\n{'='*60}")
            print(f"📅 {year}년 백필 시작")
            print(f"{'='*60}")
            
            self.backfill_by_year(
                symbols=symbols,
                target_year=year,
                max_workers=1,  # 안정성 위해 순차 처리
                batch_size=5    # 작은 배치로 안정성 확보
            )
            
            # 연도 간 휴식
            if year != self.backfill_years[-1]:
                print(f"\n⏸ {year}년 완료 - 다음 연도 전 휴식 (30초)...")
                time.sleep(30)
        
        overall_elapsed = time.time() - overall_start
        
        print(f"\n🎉 전체 4년 백필 완료!")
        print(f"⏱️ 총 소요 시간: {overall_elapsed/3600:.1f}시간")
        print(f"📊 처리된 종목-연도: {len(symbols) * len(self.backfill_years)}개")
        print(f"💾 예상 데이터: ~{len(symbols) * len(self.backfill_years) * 250:,}개 레코드")
    
    def close(self):
        """리소스 정리"""
        self.db.close()

def main():
    """백필 실행 메인 함수"""
    
    print("🚀 과거 4년 데이터 백필 도구")
    print("현재 상황: 최근 1년 데이터 보유")
    print("목표: 2020-2023년 4년 데이터 백필")
    print("=" * 60)
    
    # 백필러 초기화
    backfiller = HistoricalDataBackfiller()
    
    try:
        # 활성 심볼 조회
        symbols = backfiller.db.get_active_symbols()
        if not symbols:
            print("❌ 활성 심볼이 없습니다. 먼저 NASDAQ 심볼을 수집하세요.")
            return
        
        print(f"📋 백필 대상: {len(symbols)}개 종목")
        
        # 누락 데이터 분석 (샘플)
        print("\n🔍 누락 데이터 분석 (샘플 10개)...")
        missing_data = backfiller.analyze_missing_data(symbols[:10])
        
        if not missing_data:
            print("✅ 샘플 종목들은 모든 데이터가 있습니다.")
            return
        
        # 사용자 확인
        print(f"\n📊 분석 결과: {len(missing_data)}개 종목에 누락 데이터 발견")
        print("💡 전체 백필 실행하시겠습니까? (y/n): ", end="")
        
        # 자동 실행을 위해 'y'로 설정 (실제 사용시 input() 사용)
        confirmation = "y"  # input().lower()
        print("y (자동 실행)")
        
        if confirmation == 'y':
            # 전체 백필 실행
            backfiller.full_backfill(symbols)
        else:
            print("❌ 백필이 취소되었습니다.")
    
    except Exception as e:
        print(f"💥 백필 중 오류 발생: {e}")
    
    finally:
        backfiller.close()
        print("✅ 백필 도구 종료")

if __name__ == "__main__":
    main()
