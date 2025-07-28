#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import random
from datetime import datetime

# 경로 추가
sys.path.append('/home/grey1/stock-kafka3')
sys.path.append('/home/grey1/stock-kafka3/common')
sys.path.append('/home/grey1/stock-kafka3/airflow/plugins')

from collect_stock_data_yfinance import YFinanceCollector

def test_sharding_performance():
    """실제 yfinance 데이터로 샤딩 성능 테스트"""
    print("🧪 실제 데이터 샤딩 성능 테스트")
    print("=" * 60)
    
    # 테스트용 종목들 (실제 존재하는 종목)
    test_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
        'NVDA', 'META', 'NFLX', 'ADBE', 'CRM',
        'ORCL', 'CSCO', 'INTC', 'IBM', 'DELL',
        'VMW', 'NOW', 'WDAY', 'OKTA', 'ZM'
    ]
    
    # 1. 단일 DB 테스트
    print("🔍 단일 DB 성능 테스트")
    single_db_path = "/tmp/single_test.db"
    
    collector_single = YFinanceCollector(
        db_path=single_db_path,
        use_sharding=False
    )
    
    start_time = time.time()
    success_count = 0
    
    for i, symbol in enumerate(test_symbols):
        symbol_start = time.time()
        
        success = collector_single.collect_stock_data(symbol, period="1y")  # 1년 데이터
        if success:
            success_count += 1
        
        symbol_end = time.time()
        symbol_duration = symbol_end - symbol_start
        
        print(f"  📊 {i+1:2}/{len(test_symbols)} {symbol}: {symbol_duration:.2f}초 {'✅' if success else '❌'}")
        
        # API 호출 제한 방지
        time.sleep(random.uniform(0.5, 1.5))
    
    single_total_time = time.time() - start_time
    print(f"✅ 단일 DB 완료: {success_count}/{len(test_symbols)} 성공, {single_total_time:.2f}초")
    print(f"  - 평균 종목당: {single_total_time/len(test_symbols):.2f}초")
    print("=" * 60)
    
    # 2. 샤딩 DB 테스트
    print("🔍 샤딩 DB 성능 테스트")
    
    collector_sharded = YFinanceCollector(
        db_path="",  # 샤딩 모드에서는 사용하지 않음
        use_sharding=True
    )
    
    start_time = time.time()
    success_count = 0
    
    for i, symbol in enumerate(test_symbols):
        symbol_start = time.time()
        
        success = collector_sharded.collect_stock_data(symbol, period="1y")  # 1년 데이터
        if success:
            success_count += 1
        
        symbol_end = time.time()
        symbol_duration = symbol_end - symbol_start
        
        # 샤드 번호 표시
        shard_num = collector_sharded.shard_manager.get_shard_number(symbol)
        print(f"  📊 {i+1:2}/{len(test_symbols)} {symbol} (샤드 {shard_num}): {symbol_duration:.2f}초 {'✅' if success else '❌'}")
        
        # API 호출 제한 방지
        time.sleep(random.uniform(0.5, 1.5))
    
    sharded_total_time = time.time() - start_time
    print(f"✅ 샤딩 DB 완료: {success_count}/{len(test_symbols)} 성공, {sharded_total_time:.2f}초")
    print(f"  - 평균 종목당: {sharded_total_time/len(test_symbols):.2f}초")
    
    # 샤드 통계 출력
    print("\n📈 샤드별 통계:")
    stats = collector_sharded.shard_manager.get_shard_statistics()
    for shard_name, stat in stats.items():
        if 'error' not in stat:
            print(f"  {shard_name}: {stat['symbol_count']}개 종목, {stat['record_count']}개 레코드")
        else:
            print(f"  {shard_name}: 오류 - {stat['error']}")
    
    print("=" * 60)
    
    # 3. 결과 비교
    print("📊 성능 비교 결과:")
    print(f"  단일 DB: {single_total_time:.2f}초")
    print(f"  샤딩 DB: {sharded_total_time:.2f}초")
    
    if sharded_total_time < single_total_time:
        improvement = ((single_total_time - sharded_total_time) / single_total_time) * 100
        print(f"  🎉 샤딩이 {improvement:.1f}% 더 빠름!")
    else:
        degradation = ((sharded_total_time - single_total_time) / single_total_time) * 100
        print(f"  ⚠️ 샤딩이 {degradation:.1f}% 더 느림")
    
    # 정리
    collector_sharded.shard_manager.close_all()
    try:
        import os
        os.remove(single_db_path)
    except:
        pass
    
    print("=" * 60)
    print("✅ 실제 데이터 성능 테스트 완료")

def test_individual_timing():
    """개별 종목의 데이터 적재 시간 측정"""
    print("⏱️ 개별 종목 성능 분석")
    print("=" * 60)
    
    # 다양한 규모의 종목들로 테스트
    test_cases = [
        ('AAPL', '대형주'),
        ('TSLA', '중형주'),
        ('ZOOM', '소형주'),
        ('NVDA', '대형주'),
        ('PLTR', '신주')
    ]
    
    collector = YFinanceCollector(
        db_path="/tmp/timing_test.db",
        use_sharding=False
    )
    
    for symbol, category in test_cases:
        print(f"\n📊 {symbol} ({category}) 분석:")
        
        # 1년 데이터 수집 시간 측정
        start_time = time.time()
        success = collector.collect_stock_data(symbol, period="1y")
        end_time = time.time()
        
        duration = end_time - start_time
        
        if success:
            print(f"  ✅ 수집 완료: {duration:.2f}초")
            
            # 데이터 건수 확인
            result = collector.db.execute_query(
                "SELECT COUNT(*) as count FROM stock_data WHERE symbol = ?",
                (symbol,)
            )
            
            if not result.empty:
                record_count = result.iloc[0]['count']
                print(f"  📈 저장된 레코드: {record_count}개")
                print(f"  ⚡ 레코드당 평균: {duration/record_count:.6f}초")
            
        else:
            print(f"  ❌ 수집 실패: {duration:.2f}초")
        
        # API 제한 방지
        time.sleep(2)
    
    print("=" * 60)
    print("✅ 개별 성능 분석 완료")

if __name__ == "__main__":
    print("🚀 yfinance 데이터 수집 성능 테스트 시작")
    print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. 개별 종목 성능 분석
    test_individual_timing()
    
    # 2. 샤딩 vs 단일 DB 성능 비교
    test_sharding_performance()
    
    print(f"⏰ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 테스트 완료")
