#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import random
from datetime import datetime, timedelta
import sys
import os

# 현재 디렉토리를 Python 경로에 추가
sys.path.append('/home/grey1/stock-kafka3')
sys.path.append('/home/grey1/stock-kafka3/common')

from common.db_sharding import DBShardManager
from common.database import DuckDBManager

def generate_test_data(symbol: str, days: int = 100) -> list:
    """테스트용 주식 데이터 생성"""
    data = []
    base_date = datetime.now() - timedelta(days=days)
    base_price = random.uniform(50, 300)
    
    for i in range(days):
        # 랜덤 가격 변동 (-5% ~ +5%)
        change = random.uniform(-0.05, 0.05)
        base_price *= (1 + change)
        
        stock_data = {
            'symbol': symbol,
            'date': (base_date + timedelta(days=i)).date(),
            'open': round(base_price * random.uniform(0.98, 1.02), 2),
            'high': round(base_price * random.uniform(1.00, 1.05), 2),
            'low': round(base_price * random.uniform(0.95, 1.00), 2),
            'close': round(base_price, 2),
            'volume': random.randint(100000, 10000000)
        }
        data.append(stock_data)
    
    return data

def test_single_db_performance(symbols: list, days_per_symbol: int = 100):
    """단일 DB 성능 테스트"""
    print(f"🔍 단일 DB 성능 테스트 ({len(symbols)}개 종목, {days_per_symbol}일)")
    
    # 단일 DB 매니저 생성
    db_manager = DuckDBManager("/tmp/single_test.db")
    
    start_time = time.time()
    total_records = 0
    
    for i, symbol in enumerate(symbols):
        symbol_start = time.time()
        
        # 테스트 데이터 생성
        test_data = generate_test_data(symbol, days_per_symbol)
        
        # 배치 저장
        saved_count = db_manager.save_stock_data_batch(test_data)
        total_records += saved_count
        
        symbol_end = time.time()
        symbol_duration = symbol_end - symbol_start
        
        if (i + 1) % 10 == 0:  # 10개마다 진행상황 출력
            print(f"  📊 {i+1}/{len(symbols)} 완료 - {symbol}: {saved_count}개 저장 ({symbol_duration:.2f}초)")
    
    total_time = time.time() - start_time
    
    print(f"✅ 단일 DB 완료: {total_records}개 레코드, {total_time:.2f}초")
    print(f"  - 평균 종목당: {total_time/len(symbols):.3f}초")
    print(f"  - 평균 레코드당: {total_time/total_records:.6f}초")
    
    # 정리
    try:
        db_manager.close()
        os.remove("/tmp/single_test.db")
    except:
        pass
    
    return total_time, total_records

def test_sharded_db_performance(symbols: list, days_per_symbol: int = 100):
    """샤딩 DB 성능 테스트"""
    print(f"🔍 샤딩 DB 성능 테스트 ({len(symbols)}개 종목, {days_per_symbol}일)")
    
    # 샤드 매니저 생성
    shard_manager = DBShardManager("/tmp/shard_test")
    
    start_time = time.time()
    total_records = 0
    
    for i, symbol in enumerate(symbols):
        symbol_start = time.time()
        
        # 테스트 데이터 생성
        test_data = generate_test_data(symbol, days_per_symbol)
        
        # 배치 저장 (샤딩)
        saved_count = shard_manager.save_stock_data_batch_sharded(test_data)
        total_records += saved_count
        
        symbol_end = time.time()
        symbol_duration = symbol_end - symbol_start
        
        if (i + 1) % 10 == 0:  # 10개마다 진행상황 출력
            shard_num = shard_manager.get_shard_number(symbol)
            print(f"  📊 {i+1}/{len(symbols)} 완료 - {symbol} (샤드 {shard_num}): {saved_count}개 저장 ({symbol_duration:.2f}초)")
    
    total_time = time.time() - start_time
    
    print(f"✅ 샤딩 DB 완료: {total_records}개 레코드, {total_time:.2f}초")
    print(f"  - 평균 종목당: {total_time/len(symbols):.3f}초")
    print(f"  - 평균 레코드당: {total_time/total_records:.6f}초")
    
    # 샤드 통계 출력
    print("\n📈 샤드별 통계:")
    stats = shard_manager.get_shard_statistics()
    for shard_name, stat in stats.items():
        if 'error' not in stat:
            print(f"  {shard_name}: {stat['symbol_count']}개 종목, {stat['record_count']}개 레코드")
    
    # 정리
    shard_manager.close_all()
    import shutil
    try:
        shutil.rmtree("/tmp/shard_test")
    except:
        pass
    
    return total_time, total_records

def test_hash_distribution_large():
    """대량 종목으로 해시 분산 테스트"""
    print("🔍 대량 종목 해시 분산 테스트")
    
    # 7000+ 종목 시뮬레이션 (실제 나스닥 비슷한 규모)
    test_symbols = []
    
    # 실제 형태의 종목 코드 생성
    for i in range(7000):
        if i < 1000:
            # A로 시작하는 종목들
            symbol = f"A{chr(65 + (i % 26))}{chr(65 + ((i // 26) % 26))}{i%10}"
        elif i < 2000:
            # B로 시작하는 종목들
            symbol = f"B{chr(65 + (i % 26))}{chr(65 + ((i // 26) % 26))}{i%10}"
        elif i < 3000:
            # C로 시작하는 종목들
            symbol = f"C{chr(65 + (i % 26))}{chr(65 + ((i // 26) % 26))}{i%10}"
        else:
            # 기타 랜덤 종목들
            symbol = f"{chr(65 + (i % 26))}{chr(65 + ((i // 26) % 26))}{chr(65 + ((i // 676) % 26))}{i%10}"
        
        test_symbols.append(symbol)
    
    # 샤드 매니저로 분산 테스트
    shard_manager = DBShardManager("/tmp/hash_test")
    distribution = shard_manager.test_hash_distribution(test_symbols)
    
    # 분산 품질 분석
    total_symbols = len(test_symbols)
    expected_per_shard = total_symbols / 5
    max_deviation = 0
    
    print(f"\n📊 분산 품질 분석:")
    print(f"  총 종목 수: {total_symbols}개")
    print(f"  샤드당 예상: {expected_per_shard:.1f}개")
    
    for shard_num, count in distribution.items():
        deviation = abs(count - expected_per_shard)
        deviation_percent = (deviation / expected_per_shard) * 100
        max_deviation = max(max_deviation, deviation_percent)
        
        print(f"  샤드 {shard_num}: {count}개 (편차: {deviation_percent:.1f}%)")
    
    print(f"  최대 편차: {max_deviation:.1f}%")
    
    if max_deviation < 5:
        print("✅ 우수한 분산 품질 (편차 < 5%)")
    elif max_deviation < 10:
        print("⚠️ 양호한 분산 품질 (편차 < 10%)")
    else:
        print("❌ 분산 품질 개선 필요 (편차 >= 10%)")
    
    return distribution

def run_performance_comparison():
    """성능 비교 테스트 실행"""
    print("🚀 DB 성능 비교 테스트 시작")
    print("=" * 60)
    
    # 테스트 설정
    test_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'ADBE', 'CRM',
        'ORCL', 'CSCO', 'INTC', 'IBM', 'HPQ', 'DELL', 'VMW', 'CRM', 'NOW', 'WDAY',
        'OKTA', 'ZM', 'TEAM', 'ATLR', 'MDB', 'SNOW', 'DDOG', 'CRWD', 'NET', 'FSLY'
    ]
    days_per_symbol = 200  # 약 8개월 데이터
    
    print(f"테스트 조건: {len(test_symbols)}개 종목, 종목당 {days_per_symbol}일 데이터")
    print("=" * 60)
    
    # 1. 해시 분산 테스트
    test_hash_distribution_large()
    print("=" * 60)
    
    # 2. 단일 DB 성능 테스트
    single_time, single_records = test_single_db_performance(test_symbols, days_per_symbol)
    print("=" * 60)
    
    # 3. 샤딩 DB 성능 테스트
    shard_time, shard_records = test_sharded_db_performance(test_symbols, days_per_symbol)
    print("=" * 60)
    
    # 4. 결과 비교
    print("📊 성능 비교 결과:")
    print(f"  단일 DB: {single_time:.2f}초 ({single_records}개 레코드)")
    print(f"  샤딩 DB: {shard_time:.2f}초 ({shard_records}개 레코드)")
    
    if shard_time < single_time:
        improvement = ((single_time - shard_time) / single_time) * 100
        print(f"  🎉 샤딩이 {improvement:.1f}% 더 빠름!")
    else:
        degradation = ((shard_time - single_time) / single_time) * 100
        print(f"  ⚠️ 샤딩이 {degradation:.1f}% 더 느림")
    
    print("=" * 60)
    print("✅ 성능 테스트 완료")

if __name__ == "__main__":
    run_performance_comparison()
