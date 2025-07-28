#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
데이터 적재 성능 테스트 및 샤딩 검증 스크립트
"""

import sys
import time
import os
from typing import List, Dict, Any

# 프로젝트 경로 추가
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
sys.path.insert(0, '/home/grey1/stock-kafka3/airflow/plugins')

from db_sharding import DBShardManager
from collect_stock_data_yfinance import YFinanceCollector

def test_performance_single_vs_sharded():
    """단일 DB vs 샤딩 성능 비교 테스트"""
    
    # 테스트용 심볼들 (다양한 특성)
    test_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',  # 대형주
        'AMD', 'NVDA', 'INTC', 'PYPL', 'NFLX',    # 기술주
        'JPM', 'BAC', 'WFC', 'GS', 'MS',          # 금융주
        'JNJ', 'PFE', 'MRK', 'ABT', 'TMO',        # 헬스케어
        'XOM', 'CVX', 'COP', 'SLB', 'HAL'         # 에너지
    ]
    
    print("🧪 데이터 적재 성능 테스트 시작")
    print(f"📊 테스트 대상: {len(test_symbols)}개 종목")
    print("=" * 60)
    
    results = {}
    
    # 1. 단일 DB 성능 테스트
    print("\n1️⃣ 단일 DB 모드 테스트")
    print("-" * 30)
    
    single_db_start = time.time()
    collector_single = YFinanceCollector(
        db_path="/tmp/test_single.db",
        use_sharding=False
    )
    
    single_result = collector_single.collect_all_symbols(
        symbols=test_symbols,
        max_workers=2,
        period="1y"  # 테스트용 1년 데이터
    )
    
    collector_single.close()
    single_db_end = time.time()
    single_duration = single_db_end - single_db_start
    
    results['single_db'] = {
        'duration': single_duration,
        'success_count': single_result['success'],
        'avg_per_symbol': single_duration / len(test_symbols),
        'result': single_result
    }
    
    print(f"✅ 단일 DB 완료: {single_duration:.2f}초 (평균 {single_duration/len(test_symbols):.2f}초/종목)")
    
    # 2. 샤딩 DB 성능 테스트
    print("\n2️⃣ 샤딩 DB 모드 테스트")
    print("-" * 30)
    
    sharded_db_start = time.time()
    collector_sharded = YFinanceCollector(use_sharding=True)
    
    # 샤딩 테스트를 위해 임시 경로 설정
    collector_sharded.shard_manager.base_path = "/tmp/test_shards"
    
    sharded_result = collector_sharded.collect_all_symbols(
        symbols=test_symbols,
        max_workers=2,
        period="1y"  # 테스트용 1년 데이터
    )
    
    # 샤딩 통계 수집
    shard_stats = collector_sharded.shard_manager.get_shard_statistics()
    
    collector_sharded.close()
    sharded_db_end = time.time()
    sharded_duration = sharded_db_end - sharded_db_start
    
    results['sharded_db'] = {
        'duration': sharded_duration,
        'success_count': sharded_result['success'],
        'avg_per_symbol': sharded_duration / len(test_symbols),
        'shard_stats': shard_stats,
        'result': sharded_result
    }
    
    print(f"✅ 샤딩 DB 완료: {sharded_duration:.2f}초 (평균 {sharded_duration/len(test_symbols):.2f}초/종목)")
    
    # 3. 성능 비교 결과
    print("\n📈 성능 비교 결과")
    print("=" * 60)
    
    speedup = single_duration / sharded_duration if sharded_duration > 0 else 0
    
    print(f"단일 DB  : {single_duration:.2f}초 ({results['single_db']['success_count']}/{len(test_symbols)}개 성공)")
    print(f"샤딩 DB  : {sharded_duration:.2f}초 ({results['sharded_db']['success_count']}/{len(test_symbols)}개 성공)")
    print(f"성능 향상: {speedup:.2f}배 {'🚀' if speedup > 1 else '⚠️'}")
    
    # 4. 샤딩 분산 품질 테스트
    print("\n🔍 해시 분산 품질 테스트")
    print("-" * 30)
    
    shard_manager = DBShardManager("/tmp/test_distribution")
    distribution = shard_manager.test_hash_distribution(test_symbols)
    
    # 분산 균등성 계산
    expected_per_shard = len(test_symbols) / 5
    max_deviation = max(abs(count - expected_per_shard) for count in distribution.values())
    balance_score = 100 - (max_deviation / expected_per_shard * 100)
    
    print(f"분산 균등성: {balance_score:.1f}% {'✅' if balance_score > 80 else '⚠️'}")
    
    shard_manager.close_all()
    
    # 5. 샤드별 통계 출력
    print("\n📊 샤드별 통계")
    print("-" * 30)
    
    for shard_name, stats in results['sharded_db']['shard_stats'].items():
        if 'error' not in stats:
            print(f"{shard_name}: {stats['symbol_count']}개 심볼, {stats['record_count']}개 레코드")
        else:
            print(f"{shard_name}: 오류 - {stats['error']}")
    
    return results

def test_hash_consistency():
    """해시 함수 일관성 테스트"""
    print("\n🔄 해시 함수 일관성 테스트")
    print("-" * 30)
    
    shard_manager = DBShardManager()
    test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    print("심볼별 샤드 배정 (5번 테스트):")
    for i in range(5):
        print(f"테스트 {i+1}:")
        for symbol in test_symbols:
            shard_num = shard_manager.get_shard_number(symbol)
            print(f"  {symbol} -> 샤드 {shard_num}")
        print()
    
    shard_manager.close_all()

def benchmark_individual_symbol_performance():
    """개별 종목 적재 시간 벤치마크"""
    print("\n⏱️ 개별 종목 적재 시간 벤치마크")
    print("-" * 30)
    
    test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    # 단일 DB 테스트
    collector = YFinanceCollector(
        db_path="/tmp/benchmark_single.db",
        use_sharding=False
    )
    
    print("단일 DB 모드:")
    for symbol in test_symbols:
        start_time = time.time()
        success = collector.collect_stock_data(symbol, period="1y")
        end_time = time.time()
        duration = end_time - start_time
        
        status = "✅" if success else "❌"
        print(f"  {symbol}: {duration:.2f}초 {status}")
    
    collector.close()
    
    # 샤딩 DB 테스트
    collector_sharded = YFinanceCollector(use_sharding=True)
    collector_sharded.shard_manager.base_path = "/tmp/benchmark_shards"
    
    print("\n샤딩 DB 모드:")
    for symbol in test_symbols:
        start_time = time.time()
        success = collector_sharded.collect_stock_data(symbol, period="1y")
        end_time = time.time()
        duration = end_time - start_time
        
        shard_num = collector_sharded.shard_manager.get_shard_number(symbol)
        status = "✅" if success else "❌"
        print(f"  {symbol}: {duration:.2f}초 (샤드 {shard_num}) {status}")
    
    collector_sharded.close()

if __name__ == "__main__":
    print("🚀 데이터 적재 성능 테스트 시작")
    print("=" * 60)
    
    # 1. 해시 일관성 테스트
    test_hash_consistency()
    
    # 2. 개별 종목 성능 벤치마크
    benchmark_individual_symbol_performance()
    
    # 3. 단일 vs 샤딩 성능 비교
    results = test_performance_single_vs_sharded()
    
    print("\n🎉 모든 테스트 완료!")
    print("=" * 60)
    
    # 결론 및 권장사항
    single_avg = results['single_db']['avg_per_symbol']
    sharded_avg = results['sharded_db']['avg_per_symbol']
    
    print(f"\n💡 성능 분석 결과:")
    print(f"• 종목당 평균 처리시간:")
    print(f"  - 단일 DB: {single_avg:.2f}초")
    print(f"  - 샤딩 DB: {sharded_avg:.2f}초")
    
    if sharded_avg < single_avg:
        improvement = ((single_avg - sharded_avg) / single_avg) * 100
        print(f"• 샤딩으로 {improvement:.1f}% 성능 향상 🚀")
        print("• 권장사항: 샤딩 사용 권장")
    else:
        degradation = ((sharded_avg - single_avg) / single_avg) * 100
        print(f"• 샤딩으로 {degradation:.1f}% 성능 저하 ⚠️")
        print("• 권장사항: 단일 DB 유지 또는 샤딩 최적화 필요")
    
    print(f"\n📊 7049개 종목 전체 처리 예상 시간:")
    print(f"• 단일 DB: {single_avg * 7049 / 3600:.1f}시간")
    print(f"• 샤딩 DB: {sharded_avg * 7049 / 3600:.1f}시간")
