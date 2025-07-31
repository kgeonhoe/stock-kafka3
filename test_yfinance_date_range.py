#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yfinance as yf
from datetime import datetime, date, timedelta
import pandas as pd

def test_yfinance_date_options():
    """yfinance 날짜 옵션 테스트"""
    
    symbol = "AAPL"
    ticker = yf.Ticker(symbol)
    
    print(f"🧪 {symbol} yfinance 날짜 옵션 테스트\n")
    
    # 1. 특정 연도만 수집 (2023년)
    print("1️⃣ 2023년 데이터만 수집:")
    hist_2023 = ticker.history(start="2023-01-01", end="2023-12-31")
    print(f"   📊 2023년 데이터: {len(hist_2023)}일")
    print(f"   📅 기간: {hist_2023.index[0].date()} ~ {hist_2023.index[-1].date()}")
    
    # 2. 특정 월만 수집 (2024년 7월)
    print("\n2️⃣ 2024년 7월 데이터만 수집:")
    hist_202407 = ticker.history(start="2024-07-01", end="2024-07-31")
    print(f"   📊 2024년 7월 데이터: {len(hist_202407)}일")
    if len(hist_202407) > 0:
        print(f"   📅 기간: {hist_202407.index[0].date()} ~ {hist_202407.index[-1].date()}")
    
    # 3. 최근 30일만 수집
    print("\n3️⃣ 최근 30일 데이터:")
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    hist_30days = ticker.history(start=start_date, end=end_date)
    print(f"   📊 최근 30일 데이터: {len(hist_30days)}일")
    if len(hist_30days) > 0:
        print(f"   📅 기간: {hist_30days.index[0].date()} ~ {hist_30days.index[-1].date()}")
    
    # 4. 비어있는 날짜 찾기 예시
    print("\n4️⃣ 비어있는 날짜 찾기 예시:")
    
    # 전체 5년 데이터 수집
    hist_5y = ticker.history(period="5y")
    print(f"   📊 5년 전체 데이터: {len(hist_5y)}일")
    
    # 실제 존재하는 날짜들
    existing_dates = set(hist_5y.index.date)
    
    # 이론적으로 있어야 할 날짜들 (주말 제외)
    start_theoretical = hist_5y.index[0].date()
    end_theoretical = hist_5y.index[-1].date()
    
    all_dates = []
    current_date = start_theoretical
    while current_date <= end_theoretical:
        # 주말(토요일=5, 일요일=6) 제외
        if current_date.weekday() < 5:
            all_dates.append(current_date)
        current_date += timedelta(days=1)
    
    theoretical_dates = set(all_dates)
    missing_dates = theoretical_dates - existing_dates
    
    print(f"   📅 이론적 거래일: {len(theoretical_dates)}일")
    print(f"   📅 실제 데이터: {len(existing_dates)}일")
    print(f"   ❌ 누락된 날짜: {len(missing_dates)}일")
    
    if missing_dates:
        sorted_missing = sorted(list(missing_dates))
        print(f"   🔍 최근 누락 날짜 샘플: {sorted_missing[-5:]}")
    
    # 5. 누락된 기간만 타겟 수집 예시
    if missing_dates:
        print("\n5️⃣ 누락된 기간 타겟 수집 예시:")
        
        # 누락된 날짜를 연속된 구간으로 그룹화
        sorted_missing = sorted(list(missing_dates))
        
        # 간단한 연속 구간 찾기
        if len(sorted_missing) > 0:
            sample_missing_date = sorted_missing[0]
            
            # 해당 날짜 전후로 일주일 범위 수집해보기
            range_start = sample_missing_date - timedelta(days=3)
            range_end = sample_missing_date + timedelta(days=3)
            
            hist_targeted = ticker.history(start=range_start, end=range_end)
            print(f"   🎯 타겟 수집 ({range_start} ~ {range_end}): {len(hist_targeted)}일")
            
            if len(hist_targeted) > 0:
                print(f"   📋 수집된 날짜들:")
                for target_date in hist_targeted.index.date:
                    status = "✅ 복구됨" if target_date in missing_dates else "✅ 기존"
                    print(f"     - {target_date}: {status}")

def test_smart_incremental_collection():
    """스마트 증분 수집 전략 예시"""
    
    print("\n🚀 스마트 증분 수집 전략:")
    
    symbol = "AAPL"
    ticker = yf.Ticker(symbol)
    
    # 1. 기존 데이터에서 마지막 날짜 확인 (시뮬레이션)
    existing_last_date = date(2024, 7, 26)  # 예시
    today = date.today()
    
    print(f"   📅 기존 마지막 날짜: {existing_last_date}")
    print(f"   📅 오늘 날짜: {today}")
    
    # 2. 필요한 기간만 수집
    if existing_last_date < today:
        collect_start = existing_last_date + timedelta(days=1)
        collect_end = today
        
        print(f"   🎯 수집 필요 기간: {collect_start} ~ {collect_end}")
        
        # 실제 API 호출
        hist_incremental = ticker.history(start=collect_start, end=collect_end + timedelta(days=1))
        print(f"   📊 증분 수집 결과: {len(hist_incremental)}일")
        
        if len(hist_incremental) > 0:
            print(f"   📋 수집된 날짜:")
            for inc_date in hist_incremental.index.date:
                print(f"     - {inc_date}")
    else:
        print("   ✅ 이미 최신 데이터 (수집 불필요)")

if __name__ == "__main__":
    print("🧪 yfinance 날짜 범위 수집 테스트")
    print("=" * 50)
    
    test_yfinance_date_options()
    test_smart_incremental_collection()
    
    print("\n" + "=" * 50)
    print("✅ 테스트 완료")
    print("\n💡 결론:")
    print("  - yfinance는 start/end 날짜로 정확한 범위 지정 가능")
    print("  - 특정 연도, 월, 기간만 선택적 수집 가능")
    print("  - 비어있는 날짜만 타겟팅해서 효율적 수집 가능")
    print("  - API 호출 최소화로 속도/안정성 향상 가능")
