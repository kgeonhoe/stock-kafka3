#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time

def test_yfinance_5y_data():
    """5년 데이터 수집 테스트"""
    print("🧪 yfinance 5년 데이터 수집 테스트 시작...")
    
    # 테스트할 심볼들 (대형주)
    test_symbols = ['AAPL', 'MSFT', 'GOOGL']
    
    for symbol in test_symbols:
        print(f"\n📊 {symbol} 테스트 중...")
        
        try:
            # yfinance로 5년 데이터 수집
            start_time = time.time()
            ticker = yf.Ticker(symbol)
            hist = ticker.history(
                period="5y",
                auto_adjust=True,
                prepost=False,
                actions=False,
                repair=True
            )
            end_time = time.time()
            
            if hist.empty:
                print(f"❌ {symbol}: 데이터가 비어있음")
                continue
            
            # 데이터 정보 출력
            print(f"✅ {symbol}: 수집 성공!")
            print(f"📈 데이터 기간: {hist.index[0].date()} ~ {hist.index[-1].date()}")
            print(f"📊 총 데이터 수: {len(hist)}개 레코드")
            print(f"⏱️  수집 시간: {end_time - start_time:.2f}초")
            
            # 최근 5일 데이터 샘플 출력
            print(f"📋 최근 5일 데이터:")
            recent_data = hist.tail(5)[['Open', 'High', 'Low', 'Close', 'Volume']]
            for date, row in recent_data.iterrows():
                print(f"  {date.date()}: ${row['Close']:.2f} (거래량: {row['Volume']:,})")
            
            # 데이터 품질 확인
            null_count = hist.isnull().sum().sum()
            if null_count > 0:
                print(f"⚠️ Null 값 발견: {null_count}개")
            else:
                print("✅ 데이터 품질: 양호 (Null 값 없음)")
                
        except Exception as e:
            print(f"❌ {symbol}: 오류 발생 - {e}")
        
        # API 제한 방지를 위한 지연
        time.sleep(2)
    
    print("\n🎉 5년 데이터 수집 테스트 완료!")

def test_data_range():
    """날짜 범위 확인 테스트"""
    print("\n📅 날짜 범위 확인 테스트...")
    
    symbol = "AAPL"
    ticker = yf.Ticker(symbol)
    
    # 다양한 기간 테스트
    periods = ["1y", "2y", "5y", "max"]
    
    for period in periods:
        try:
            hist = ticker.history(period=period)
            if not hist.empty:
                start_date = hist.index[0].date()
                end_date = hist.index[-1].date()
                record_count = len(hist)
                
                print(f"📊 {period}: {start_date} ~ {end_date} ({record_count}개)")
            else:
                print(f"❌ {period}: 데이터 없음")
        except Exception as e:
            print(f"❌ {period}: 오류 - {e}")
        
        time.sleep(1)

if __name__ == "__main__":
    test_yfinance_5y_data()
    test_data_range()
