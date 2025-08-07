#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
yfinance 지연 테스트 (Rate Limit 회피)
"""

import yfinance as yf
import time
from datetime import datetime

def test_with_delay():
    """지연을 두고 테스트"""
    print("🧪 지연 테스트 시작 (Rate Limit 회피)...")
    
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    
    for i, symbol in enumerate(symbols):
        print(f"\n📊 {symbol} 테스트 중... ({i+1}/{len(symbols)})")
        
        try:
            # 각 요청 사이에 2초 지연
            if i > 0:
                print("⏳ 2초 대기...")
                time.sleep(2)
            
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d", interval="1d")  # 하루만 요청
            
            if not hist.empty:
                latest_price = hist['Close'][-1]
                print(f"✅ {symbol}: 최근가 ${latest_price:.2f}")
            else:
                print(f"❌ {symbol}: 데이터 없음")
                
        except Exception as e:
            print(f"💥 {symbol}: 오류 - {e}")
    
    print("\n🎉 지연 테스트 완료!")

def check_yfinance_status():
    """yfinance 상태 확인"""
    print("🔍 yfinance 상태 확인...")
    
    try:
        # 가장 간단한 요청
        ticker = yf.Ticker("AAPL")
        info = ticker.basic_info
        
        if info:
            print("✅ yfinance 연결 정상")
            print(f"   회사명: {info.get('shortName', 'N/A')}")
            print(f"   심볼: {info.get('symbol', 'N/A')}")
        else:
            print("⚠️ 기본 정보 없음")
            
    except Exception as e:
        print(f"❌ yfinance 연결 오류: {e}")

if __name__ == "__main__":
    print("🚀 yfinance 지연 테스트 시작!")
    print("=" * 50)
    
    check_yfinance_status()
    test_with_delay()
    
    print("=" * 50)
