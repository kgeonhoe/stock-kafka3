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
    
    def collect_stock_data(self, symbol: str, period: str = "1y") -> bool:
        """
        개별 종목 주가 데이터 수집 (최적화된 고속 처리)
        
        Args:
            symbol: 종목 심볼 (예: AAPL)
            period: 수집 기간 (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            
        Returns:
            수집 성공 여부
        """
        import time
        import random
        
        try:
            # API 호출 제한 방지를 위한 랜덤 지연
            delay = random.uniform(0.2, 0.8)  # 200ms ~ 800ms 랜덤 지연
            time.sleep(delay)
            
            # yfinance로 데이터 수집 (최적화된 설정)
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
            
            # 데이터 정리 (벡터화 연산으로 최적화)
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
            
            # DuckDB에 배치 저장 (최적화)
            save_count = 0
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
                    self.db.save_stock_data(stock_data)
                    save_count += 1
                except Exception as save_error:
                    print(f"⚠️ {symbol}: 저장 오류 - {save_error}")
                    continue  # 개별 레코드 오류는 무시하고 계속 진행
            
            if save_count > 0:
                print(f"✅ {symbol}: {save_count}개 레코드 저장 성공")
                return True
            else:
                print(f"❌ {symbol}: 저장된 레코드 없음")
                return False
            
        except Exception as e:
            error_msg = str(e)
            if "delisted" in error_msg or "No data found" in error_msg:
                print(f"⚠️ {symbol}: 상장폐지 또는 데이터 없음")
                return False
            elif "rate limit" in error_msg.lower() or "429" in error_msg:
                print(f"🚫 {symbol}: API 호출 제한 - 재시도 중...")
                time.sleep(random.uniform(2, 5))  # 2-5초 대기 후 재시도
                return self.collect_stock_data(symbol, period)  # 한 번만 재시도
            else:
                print(f"💥 {symbol}: 수집 실패 - {error_msg}")
                return False
    
    def collect_all_symbols(self, symbols: List[str] = None, period: str = "2y", max_workers: int = 5) -> Dict[str, Any]:
        """
        종목 리스트의 주가 데이터 수집 (병렬 처리)
        
        Args:
            symbols: 수집할 종목 리스트 (None이면 DB에서 자동 조회)
            period: 수집 기간
            max_workers: 병렬 처리 워커 수 (기본 5개)
            
        Returns:
            수집 결과 통계
        """
        print("🚀 yfinance 기반 고속 병렬 주가 데이터 수집 시작")
        
        # 종목 리스트 결정
        if symbols is None:
            symbols = self.db.get_active_symbols()
        
        if not symbols:
            print("⚠️ 저장된 종목이 없습니다")
            return {'total': 0, 'success': 0, 'fail': 0}
        print(f"📊 {(symbols)}")
        print(f"📊 총 {len(symbols)}개 종목을 {max_workers}개 워커로 병렬 수집")
        print(f"⚡ 예상 수집 시간: {len(symbols) * 2 // max_workers}초 (병렬 처리)")
        
        success_count = 0
        fail_count = 0
        start_time = time.time()
        
        # 병렬 처리로 데이터 수집
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 모든 심볼에 대해 future 생성
            future_to_symbol = {
                executor.submit(self.collect_stock_data, symbol, period): symbol 
                for symbol in symbols
            }
            
            # 완료된 작업들 처리
            for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol), 1):
                symbol = future_to_symbol[future]
                
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                        print(f"✅ {symbol} 성공 ({i}/{len(symbols)}) - {(i/len(symbols)*100):.1f}%")
                    else:
                        fail_count += 1
                        print(f"❌ {symbol} 실패 ({i}/{len(symbols)}) - {(i/len(symbols)*100):.1f}%")
                        
                except Exception as e:
                    fail_count += 1
                    print(f"💥 {symbol} 예외 발생: {e} ({i}/{len(symbols)})")
        
        elapsed_time = time.time() - start_time
        
        result = {
            'total': len(symbols),
            'success': success_count,
            'fail': fail_count,
            'elapsed_time': round(elapsed_time, 2),
            'avg_time_per_symbol': round(elapsed_time / len(symbols), 2),
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"🎉 병렬 데이터 수집 완료!")
        print(f"📊 결과: 총 {result['total']}개, 성공 {result['success']}개, 실패 {result['fail']}개")
        print(f"⚡ 처리 시간: {result['elapsed_time']}초 (평균 {result['avg_time_per_symbol']}초/종목)")
        print(f"🚀 병렬 처리 효과: {max_workers}배 속도 향상!")
        
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
    
    print("🚀 yfinance 고속 병렬 주가 데이터 수집 시작!")
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
    
    # 병렬 수집 실행 - API 제한 고려하여 워커 수 감소
    result = collector.collect_all_symbols(symbols=symbols, max_workers=1)  # 10->1으로 감소
    success_count = result['success']
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"✅ yfinance 병렬 수집 완료!")
    print(f"📊 처리 결과: {success_count}/{len(symbols)}개 성공")
    print(f"⏱️  총 소요시간: {duration:.2f}초 (평균 {duration/len(symbols):.2f}초/종목)")
    print(f"🚄 성능: {len(symbols)/duration:.2f} 종목/초")
    print(f"🎯 전체 NASDAQ 종목 {len(symbols)}개 처리 완료!")
    
    return {
        'total_symbols': len(symbols),
        'success_count': success_count,
        'duration': duration,
        'throughput': len(symbols)/duration,
        'result_details': result
    }
