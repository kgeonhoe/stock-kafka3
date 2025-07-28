#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yfinance as yf
import pandas as pd
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List
import concurrent.futures
import time

# 프로젝트 경로 추가
sys.path.insert(0, '/opt/airflow/common')
from database import DuckDBManager

class YFinanceCollector:
    """yfinance 기반 주식 데이터 수집 클래스"""
    
    def __init__(self, db_path: str = "/data/duckdb/stock_data.db"):
        """
        yfinance 주식 데이터 수집 클래스 초기화
        
        Args:
            db_path: DuckDB 파일 경로
        """
        self.db = DuckDBManager(db_path)
    
    def collect_stock_data(self, symbol: str, period: str = "5y") -> bool:
        """
        개별 종목 주가 데이터 수집 (중복 날짜 스킵)
        
        Args:
            symbol: 종목 심볼 (예: AAPL)
            period: 수집 기간 (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            
        Returns:
            수집 성공 여부
        """
        import time
        import random
        from datetime import date, timedelta
        
        try:
            # 1. 기존 데이터 확인
            existing_dates = self.db.get_existing_dates(symbol, days_back=1825)  # 5년간 데이터 확인 (365*5)
            latest_date = self.db.get_latest_date(symbol)
            
            print(f"🔍 {symbol}: 기존 데이터 {len(existing_dates)}일, 최신 날짜: {latest_date}")
            
            # 2. API 호출 제한 방지를 위한 지연 (5년 데이터용 - 배치 처리로 단축)
            delay = random.uniform(1.0, 2.0)  # 1-2초 랜덤 지연 (배치 처리로 단축)
            time.sleep(delay)
            
            # 3. yfinance로 데이터 수집 (curl_cffi 세션 제거로 API 오류 해결)
            # Yahoo API가 curl_cffi를 요구하므로 세션 설정 제거
            ticker = yf.Ticker(symbol)
            hist = ticker.history(
                period=period, 
                auto_adjust=True,      # 배당/분할 자동 조정
                prepost=False,         # 시간외 거래 제외
                actions=False,         # 배당/분할 이벤트 제외 (속도 향상)
                repair=True           # 데이터 오류 자동 수정
            )
            
            if hist.empty:
                print(f"⚠️ {symbol}: 히스토리 데이터가 비어있음")
                return False
            
            # 4. 데이터 정리 (벡터화 연산으로 최적화)
            hist = hist.reset_index()
            hist['symbol'] = symbol
            
            # 컬럼명 일괄 변경
            hist.columns = [col.lower().replace(' ', '_') for col in hist.columns]
            
            # 날짜 처리 (pandas 벡터화)
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date']).dt.date
            
            # 필요한 컬럼만 선택하고 NaN 제거
            required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
            hist = hist[required_columns].dropna()
            
            if len(hist) == 0:
                print(f"⚠️ {symbol}: 정제 후 데이터가 비어있음")
                return False
            
            # 5. 중복 날짜 필터링 (새로운 로직)
            total_records = len(hist)
            hist_filtered = hist[~hist['date'].isin(existing_dates)]  # 기존 날짜 제외
            new_records = len(hist_filtered)
            
            if new_records == 0:
                print(f"✅ {symbol}: 모든 데이터가 이미 존재함 ({total_records}개 중 신규 0개)")
                return True
            
            # 6. DuckDB에 신규 데이터 배치 저장 (성능 최적화)
            if new_records == 0:
                print(f"✅ {symbol}: 모든 데이터가 이미 존재함 ({total_records}개 중 신규 0개)")
                return True
            
            print(f"📊 {symbol}: {total_records}개 중 신규 {new_records}개 데이터 배치 저장")
            
            # 배치 저장을 위한 데이터 준비
            batch_data = []
            for _, row in hist_filtered.iterrows():
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
                    print(f"⚠️ {symbol}: 데이터 변환 오류 - {data_error}")
                    continue
            
            # 배치 저장 (한 번에 모든 데이터 저장)
            if batch_data:
                try:
                    print(f"💾 {symbol}: {len(batch_data)}개 레코드 배치 저장 시작...")
                    import sys
                    sys.stdout.flush()  # 로그 즉시 출력
                    
                    save_count = self.db.save_stock_data_batch(batch_data)
                    print(f"✅ {symbol}: {save_count}개 신규 레코드 배치 저장 성공")
                    sys.stdout.flush()  # 로그 즉시 출력
                    return True
                except Exception as batch_error:
                    print(f"❌ {symbol}: 배치 저장 실패 - {batch_error}")
                    import sys
                    sys.stdout.flush()  # 로그 즉시 출력
                    
                    # 배치 저장 실패시 개별 저장으로 폴백
                    print(f"🔄 {symbol}: 개별 저장으로 폴백...")
                    sys.stdout.flush()  # 로그 즉시 출력
                    save_count = 0
                    for stock_data in batch_data:
                        try:
                            self.db.save_stock_data(stock_data)
                            save_count += 1
                        except Exception as save_error:
                            print(f"⚠️ {symbol}: 개별 저장 오류 - {save_error}")
                            continue
                    
                    if save_count > 0:
                        print(f"✅ {symbol}: {save_count}개 개별 저장 완료")
                        return True
                    else:
                        print(f"❌ {symbol}: 저장된 레코드 없음")
                        return False
            else:
                print(f"❌ {symbol}: 변환된 데이터 없음")
                return False
            
        except Exception as e:
            error_msg = str(e)
            if "delisted" in error_msg or "No data found" in error_msg:
                print(f"⚠️ {symbol}: 상장폐지 또는 데이터 없음")
                return False
            elif "rate limit" in error_msg.lower() or "429" in error_msg or "Too Many Requests" in error_msg:
                print(f"🚫 {symbol}: API 호출 제한 감지 - 장시간 대기 중...")
                time.sleep(random.uniform(10, 20))  # 10-20초 대기 (기존 2-5초에서 증가)
                print(f"🔄 {symbol}: 재시도 중...")
                # 재시도 시에는 더 긴 지연
                time.sleep(random.uniform(5, 10))
                return False  # 재시도 대신 실패로 처리하여 무한루프 방지
            else:
                print(f"💥 {symbol}: 수집 실패 - {error_msg}")
                return False
    
    def collect_all_symbols(self, symbols: List[str] = None, period: str = "2y", max_workers: int = 5, batch_size: int = 50) -> Dict[str, Any]:
        """
        전체 종목 배치별 병렬 수집 (메모리 최적화)
        
        Args:
            symbols: 수집할 종목 리스트 (None이면 DB에서 조회)
            period: 수집 기간 
            max_workers: 병렬 처리 수 (메모리 절약을 위해 기본값 2)
            batch_size: 배치 크기 (메모리 절약을 위해 기본값 50)
            
        Returns:
            수집 결과 통계
        """
        import concurrent.futures
        import time
        import gc  # 가비지 컬렉션
        
        # 심볼 목록 준비
        if symbols is None:
            symbols = self.db.get_active_symbols()
            if not symbols:
                print("❌ 활성 심볼이 없습니다.")
                return {'error': 'No active symbols found'}
        
        total_symbols = len(symbols)
        print(f"🚀 {total_symbols}개 종목 배치별 병렬 수집 시작 (메모리 최적화)")
        print(f"⚙️ 설정: 최대 {max_workers}개 워커, 배치 크기: {batch_size}, 수집 기간: {period}")
        
        start_time = time.time()
        total_success = 0
        total_fail = 0
        
        # 배치별로 처리
        for batch_start in range(0, total_symbols, batch_size):
            batch_end = min(batch_start + batch_size, total_symbols)
            batch_symbols = symbols[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total_symbols + batch_size - 1) // batch_size
            
            print(f"\n📦 배치 {batch_num}/{total_batches}: {len(batch_symbols)}개 종목 처리 중...")
            batch_start_time = time.time()
            
            success_count = 0
            fail_count = 0
            
            # 병렬 처리
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 배치 내 심볼들에 대해 future 생성
                future_to_symbol = {
                    executor.submit(self.collect_stock_data, symbol, period): symbol 
                    for symbol in batch_symbols
                }
                
                # 완료된 작업들 처리
                for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol), 1):
                    symbol = future_to_symbol[future]
                    
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                        else:
                            fail_count += 1
                        
                        # 진행 상황 표시 (5개마다)
                        if i % 5 == 0:
                            print(f"  📈 배치 진행: {i}/{len(batch_symbols)} 완료 ({success_count}개 성공)")
                            
                    except Exception as e:
                        fail_count += 1
                        print(f"💥 {symbol} 예외 발생: {e}")
            
            batch_duration = time.time() - batch_start_time
            total_success += success_count
            total_fail += fail_count
            
            print(f"✅ 배치 {batch_num} 완료: {success_count}/{len(batch_symbols)} 성공 ({batch_duration:.1f}초)")
            print(f"📊 누적 진행: {batch_end}/{total_symbols} 종목 ({total_success}개 성공, {total_fail}개 실패)")
            
            # 메모리 정리 (배치 간)
            gc.collect()
            
            # 배치 간 휴식 (API 제한 방지 및 메모리 안정화)
            if batch_end < total_symbols:
                print(f"� 배치 간 휴식 (5초)...")
                time.sleep(5)
        
        elapsed_time = time.time() - start_time
        
        result = {
            'total': total_symbols,
            'success': total_success,
            'fail': total_fail,
            'elapsed_time': round(elapsed_time, 2),
            'avg_time_per_symbol': round(elapsed_time / total_symbols, 2),
            'batch_size': batch_size,
            'max_workers': max_workers,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"\n🎉 배치별 병렬 데이터 수집 완료!")
        print(f"📊 최종 결과: 총 {result['total']}개, 성공 {result['success']}개, 실패 {result['fail']}개")
        print(f"⚡ 처리 시간: {result['elapsed_time']}초 (평균 {result['avg_time_per_symbol']}초/종목)")
        print(f"� 배치 설정: {batch_size}개씩 {max_workers}병렬 처리")
        print(f"💡 메모리 최적화로 안정성 향상!")
        
        return result
    
    def close(self):
        """리소스 정리"""
        self.db.close()

# Airflow 태스크 함수
def collect_stock_data_yfinance_task(**context):
    """
    고성능 병렬 주가 데이터 수집 태스크 (yfinance API 사용)
    """
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    print("🚀 yfinance 고속 병렬 주가 데이터 수집 시작! (5년 데이터)")
    start_time = time.time()
    
        # DuckDB에서 NASDAQ 심볼 목록 조회 (테스트용 제한)
    db = DuckDBManager()
    
    try:
        symbols_query = "SELECT DISTINCT symbol FROM nasdaq_symbols"  # 더 작은 테스트 세트
        symbols_df = db.execute_query(symbols_query)
        
        if symbols_df.empty:
            # 백업 심볼 사용 (메이저 종목들)
            backup_symbols = [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
                'NVDA', 'META', 'NFLX', 'ADBE', 'CRM',
                'ORCL', 'INTC', 'AMD', 'PYPL', 'AVGO',
                'TXN', 'QCOM', 'MU', 'AMAT', 'LRCX'
            ]
            symbols = backup_symbols
            print(f"📋 백업 심볼 사용: {len(symbols)}개")
        else:
            # 결과 타입에 따라 처리
            # if hasattr(symbols_df, 'tolist'):
            #     # pandas DataFrame 또는 QueryResult 클래스
            #     symbols = symbols_df['symbol'].tolist()
            # else:
                # 일반 리스트
            symbols = symbols_df['symbol'].tolist()
            
            print(f"📋 NASDAQ 심볼 사용: {len(symbols)}개")
            
    except Exception as db_error:
        print(f"❌ 데이터베이스 조회 오류: {db_error}")
        # 데이터베이스 오류 시 백업 심볼 사용
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        print(f"📋 백업 심볼 사용 (DB 오류): {len(symbols)}개")
    finally:
        if db:
            db.close()
    
    # YFinanceCollector 인스턴스 생성
    collector = YFinanceCollector()
    
    # 병렬 수집 실행 - 메모리 최적화로 배치 처리 (5년 데이터, 안정성 우선)
    result = collector.collect_all_symbols(symbols=symbols, max_workers=2, period="5y", batch_size=50)  # 배치 크기 50, 2개 워커로 안정성 확보
    success_count = result['success']
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"✅ yfinance 배치별 병렬 수집 완료! (5년 데이터)")
    print(f"📊 처리 결과: {success_count}/{len(symbols)}개 성공")
    print(f"⏱️  총 소요시간: {duration:.2f}초 (평균 {duration/len(symbols):.2f}초/종목)")
    print(f"🚄 성능: {len(symbols)/duration:.2f} 종목/초")
    print(f"🎯 전체 NASDAQ 종목 {len(symbols)}개 처리 완료! (5년 히스토리)")
    print(f"🔧 메모리 최적화: 50개씩 배치 처리로 안정성 향상")
    
    return {
        'total_symbols': len(symbols),
        'success_count': success_count,
        'duration': duration,
        'throughput': len(symbols)/duration,
        'batch_size': 50,
        'max_workers': 2,
        'result_details': result
    }
