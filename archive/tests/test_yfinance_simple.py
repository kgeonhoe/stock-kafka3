#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
yfinance 간단 테스트 (DuckDB 없이)
"""

import yfinance as yf
import pandas as pd
from datetime import datetime
import time

def test_yfinance_basic():
    """기본 yfinance 기능 테스트"""
    print("🧪 yfinance 기본 기능 테스트 시작...")
    
    # 테스트 종목
    symbol = "AAPL"
    
    try:
        print(f"📊 {symbol} 데이터 수집 테스트...")
        
        # yfinance로 데이터 수집
        ticker = yf.Ticker(symbol)
        hist = ticker.history(
            period="1mo",  # 1개월
            auto_adjust=True,
            prepost=False,
            actions=False,
            repair=True
        )
        
        if hist.empty:
            print(f"❌ {symbol}: 데이터가 비어있음")
            return False
        
        print(f"✅ {symbol}: {len(hist)}개 레코드 수집 성공")
        print(f"📈 날짜 범위: {hist.index[0].date()} ~ {hist.index[-1].date()}")
        print(f"💰 최근 종가: ${hist['Close'][-1]:.2f}")
        
        # 데이터 구조 확인
        print(f"📋 컬럼들: {list(hist.columns)}")
        print(f"🔍 샘플 데이터:")
        print(hist.head(3))
        
        return True
        
    except Exception as e:
        print(f"💥 {symbol} 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_multiple_symbols():
    """여러 종목 순차 테스트"""
    print("\n🧪 여러 종목 순차 테스트 시작...")
    
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    results = {}
    
    start_time = time.time()
    
    for symbol in symbols:
        print(f"\n📊 {symbol} 테스트 중...")
        
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1mo")
            
            if not hist.empty:
                results[symbol] = {
                    'success': True,
                    'records': len(hist),
                    'latest_price': hist['Close'][-1]
                }
                print(f"✅ {symbol}: {len(hist)}개 레코드, 최근가 ${hist['Close'][-1]:.2f}")
            else:
                results[symbol] = {'success': False, 'error': 'Empty data'}
                print(f"❌ {symbol}: 데이터 없음")
                
        except Exception as e:
            results[symbol] = {'success': False, 'error': str(e)}
            print(f"💥 {symbol}: 오류 - {e}")
    
    end_time = time.time()
    
    # 결과 요약
    print(f"\n📊 테스트 결과 요약:")
    success_count = sum(1 for r in results.values() if r['success'])
    print(f"   총 종목: {len(symbols)}")
    print(f"   성공: {success_count}")
    print(f"   실패: {len(symbols) - success_count}")
    print(f"   소요시간: {end_time - start_time:.2f}초")
    
    for symbol, result in results.items():
        if result['success']:
            print(f"   ✅ {symbol}: {result['records']}개 레코드")
        else:
            print(f"   ❌ {symbol}: {result['error']}")

def test_data_processing():
    """데이터 처리 테스트"""
    print("\n🧪 데이터 처리 테스트 시작...")
    
    symbol = "AAPL"
    
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="3mo")  # 3개월
        
        if hist.empty:
            print("❌ 데이터가 비어있음")
            return
        
        # 데이터 정리
        hist = hist.reset_index()
        hist['symbol'] = symbol
        
        # 컬럼명 소문자 변경
        hist.columns = [col.lower().replace(' ', '_') for col in hist.columns]
        
        # 날짜 처리
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.date
        
        # 필요한 컬럼만 선택
        required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        processed_hist = hist[required_columns].dropna()
        
        print(f"✅ 데이터 처리 성공:")
        print(f"   원본 레코드: {len(hist)}")
        print(f"   처리 후 레코드: {len(processed_hist)}")
        print(f"   컬럼들: {list(processed_hist.columns)}")
        
        # 샘플 출력
        print(f"🔍 처리된 데이터 샘플:")
        print(processed_hist.head(3))
        
        # 기본 통계
        print(f"\n📈 기본 통계:")
        print(f"   평균 종가: ${processed_hist['close'].mean():.2f}")
        print(f"   최고가: ${processed_hist['high'].max():.2f}")
        print(f"   최저가: ${processed_hist['low'].min():.2f}")
        print(f"   평균 거래량: {processed_hist['volume'].mean():,.0f}")
        
    except Exception as e:
        print(f"💥 데이터 처리 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 yfinance 간단 테스트 시작!")
    print("=" * 50)
    
    # 1. 기본 기능 테스트
    success = test_yfinance_basic()
    
    if success:
        # 2. 여러 종목 테스트
        test_multiple_symbols()
        
        # 3. 데이터 처리 테스트
        test_data_processing()
    else:
        print("❌ 기본 테스트 실패로 추가 테스트를 건너뜁니다.")
    
    print("\n🎉 테스트 완료!")
    print("=" * 50)
