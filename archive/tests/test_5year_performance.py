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

def test_5year_performance():
    """5년 데이터로 샤딩 vs 단일 DB 성능 비교"""
    print("🧪 5년 데이터 성능 비교 테스트")
    print("=" * 60)
    
    # 실제 존재하는 주요 종목들 (다양한 데이터 크기)
    test_symbols = [
        'AAPL',   # 애플 (대형주, 많은 데이터)
        'MSFT',   # 마이크로소프트
        'GOOGL',  # 구글
        'AMZN',   # 아마존
        'TSLA',   # 테슬라 (변동성 높음)
        'NVDA',   # 엔비디아
        'META',   # 메타
        'NFLX',   # 넷플릭스
        'ADBE',   # 어도비
        'CRM',    # 세일즈포스
        'ORCL',   # 오라클
        'INTC',   # 인텔
        'AMD',    # AMD
        'PYPL',   # 페이팔
        'AVGO'    # 브로드컴
    ]
    
    # 1. 단일 DB 테스트 (5년)
    print("🔍 단일 DB 성능 테스트 (5년 데이터)")
    single_times = []
    single_records = []
    
    try:
        from collect_stock_data_yfinance import YFinanceCollector
        
        collector_single = YFinanceCollector(
            db_path="/tmp/single_5year_test.db",
            use_sharding=False
        )
        
        single_start = time.time()
        
        for i, symbol in enumerate(test_symbols):
            symbol_start = time.time()
            
            print(f"  📊 {i+1:2}/{len(test_symbols)} {symbol} 수집 중...", end=" ")
            
            success = collector_single.collect_stock_data(symbol, period="5y")
            
            symbol_end = time.time()
            symbol_duration = symbol_end - symbol_start
            single_times.append(symbol_duration)
            
            if success:
                # 저장된 레코드 수 확인
                result = collector_single.db.execute_query(
                    "SELECT COUNT(*) as count FROM stock_data WHERE symbol = ?",
                    (symbol,)
                )
                if not result.empty:
                    record_count = result.iloc[0]['count']
                    single_records.append(record_count)
                    print(f"✅ {symbol_duration:.2f}초 ({record_count}개 레코드)")
                else:
                    single_records.append(0)
                    print(f"⚠️ {symbol_duration:.2f}초 (레코드 확인 실패)")
            else:
                single_records.append(0)
                print(f"❌ {symbol_duration:.2f}초 (수집 실패)")
            
            # API 제한 방지
            time.sleep(random.uniform(1.0, 2.0))
        
        single_total_time = time.time() - single_start
        single_total_records = sum(single_records)
        
        print(f"✅ 단일 DB 완료: {single_total_time:.2f}초, {single_total_records}개 레코드")
        print(f"  - 평균 종목당: {single_total_time/len(test_symbols):.2f}초")
        print(f"  - 평균 레코드당: {single_total_time/single_total_records:.6f}초" if single_total_records > 0 else "  - 레코드 없음")
        
        collector_single.close()
        
    except Exception as e:
        print(f"❌ 단일 DB 테스트 실패: {e}")
        single_total_time = float('inf')
        single_total_records = 0
    
    print("=" * 60)
    
    # 2. 샤딩 DB 테스트 (5년)
    print("🔍 샤딩 DB 성능 테스트 (5년 데이터)")
    shard_times = []
    shard_records = []
    
    try:
        collector_sharded = YFinanceCollector(
            db_path="",  # 샤딩 모드에서는 사용하지 않음
            use_sharding=True
        )
        
        shard_start = time.time()
        
        for i, symbol in enumerate(test_symbols):
            symbol_start = time.time()
            
            shard_num = collector_sharded.shard_manager.get_shard_number(symbol)
            print(f"  📊 {i+1:2}/{len(test_symbols)} {symbol} (샤드 {shard_num}) 수집 중...", end=" ")
            
            success = collector_sharded.collect_stock_data(symbol, period="5y")
            
            symbol_end = time.time()
            symbol_duration = symbol_end - symbol_start
            shard_times.append(symbol_duration)
            
            if success:
                # 해당 샤드에서 레코드 수 확인
                shard_manager = collector_sharded.shard_manager.get_shard_manager(shard_num)
                result = shard_manager.execute_query(
                    "SELECT COUNT(*) as count FROM stock_data WHERE symbol = ?",
                    (symbol,)
                )
                if not result.empty:
                    record_count = result.iloc[0]['count']
                    shard_records.append(record_count)
                    print(f"✅ {symbol_duration:.2f}초 ({record_count}개 레코드)")
                else:
                    shard_records.append(0)
                    print(f"⚠️ {symbol_duration:.2f}초 (레코드 확인 실패)")
            else:
                shard_records.append(0)
                print(f"❌ {symbol_duration:.2f}초 (수집 실패)")
            
            # API 제한 방지
            time.sleep(random.uniform(1.0, 2.0))
        
        shard_total_time = time.time() - shard_start
        shard_total_records = sum(shard_records)
        
        print(f"✅ 샤딩 DB 완료: {shard_total_time:.2f}초, {shard_total_records}개 레코드")
        print(f"  - 평균 종목당: {shard_total_time/len(test_symbols):.2f}초")
        print(f"  - 평균 레코드당: {shard_total_time/shard_total_records:.6f}초" if shard_total_records > 0 else "  - 레코드 없음")
        
        # 샤드별 통계
        print("\n📈 샤드별 분산 현황:")
        stats = collector_sharded.shard_manager.get_shard_statistics()
        for shard_name, stat in stats.items():
            if 'error' not in stat:
                print(f"  {shard_name}: {stat['symbol_count']}개 종목, {stat['record_count']}개 레코드")
            else:
                print(f"  {shard_name}: 오류 - {stat['error']}")
        
        collector_sharded.shard_manager.close_all()
        
    except Exception as e:
        print(f"❌ 샤딩 DB 테스트 실패: {e}")
        shard_total_time = float('inf')
        shard_total_records = 0
    
    print("=" * 60)
    
    # 3. 결과 비교 및 분석
    print("📊 5년 데이터 성능 비교 결과:")
    print(f"  단일 DB: {single_total_time:.2f}초 ({single_total_records}개 레코드)")
    print(f"  샤딩 DB: {shard_total_time:.2f}초 ({shard_total_records}개 레코드)")
    
    if shard_total_time < single_total_time:
        improvement = ((single_total_time - shard_total_time) / single_total_time) * 100
        print(f"  🎉 샤딩이 {improvement:.1f}% 더 빠름!")
    else:
        degradation = ((shard_total_time - single_total_time) / single_total_time) * 100
        print(f"  ⚠️ 샤딩이 {degradation:.1f}% 더 느림")
    
    # 상세 분석
    if len(single_times) > 0 and len(shard_times) > 0:
        print(f"\n🔍 상세 분석:")
        print(f"  단일 DB - 최고속: {min(single_times):.2f}초, 최저속: {max(single_times):.2f}초")
        print(f"  샤딩 DB - 최고속: {min(shard_times):.2f}초, 최저속: {max(shard_times):.2f}초")
        
        # 개별 종목별 비교
        print(f"\n📋 종목별 성능 비교:")
        for i, symbol in enumerate(test_symbols):
            if i < len(single_times) and i < len(shard_times):
                single_time = single_times[i]
                shard_time = shard_times[i]
                diff = shard_time - single_time
                symbol_result = "샤딩 승리" if shard_time < single_time else "단일 승리"
                print(f"    {symbol}: 단일 {single_time:.2f}초 vs 샤딩 {shard_time:.2f}초 (차이: {diff:+.2f}초) - {symbol_result}")
    
    print("=" * 60)
    
    # 4. 결론 및 권장사항
    print("🎯 테스트 결론:")
    
    if shard_total_time < single_total_time:
        print("  ✅ 샤딩이 더 빠름 - 대규모 데이터에서 샤딩 효과 확인!")
        print("  💡 권장사항: 샤딩 모드 사용 권장")
    else:
        overhead = ((shard_total_time - single_total_time) / single_total_time) * 100
        if overhead < 20:
            print("  ⚖️ 샤딩 오버헤드가 적음 (<20%) - 확장성을 위해 샤딩 고려")
            print("  💡 권장사항: 대규모 처리 시 샤딩 사용, 소규모는 단일 DB")
        else:
            print("  ❌ 샤딩 오버헤드가 큼 (>20%) - 현재 규모에서는 단일 DB 권장")
            print("  💡 권장사항: 단일 DB 사용, 나중에 데이터 증가시 샤딩 전환")
    
    print("=" * 60)
    print("✅ 5년 데이터 성능 테스트 완료")

def test_large_scale_timing():
    """대규모 종목으로 개별 타이밍 테스트"""
    print("⏱️ 대규모 5년 데이터 개별 타이밍 분석")
    print("=" * 60)
    
    # 다양한 크기의 종목들
    large_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',  # 대형주
        'NVDA', 'META', 'NFLX', 'ADBE', 'CRM',    # 대형주
        'ORCL', 'INTC', 'AMD', 'PYPL', 'AVGO',    # 중대형주
        'TXN', 'QCOM', 'MU', 'AMAT', 'LRCX',     # 중형주
        'KLAC', 'MRVL', 'FTNT', 'DDOG', 'SNOW'    # 성장주
    ]
    
    try:
        from collect_stock_data_yfinance import YFinanceCollector
        
        collector = YFinanceCollector(
            db_path="/tmp/timing_analysis.db",
            use_sharding=False
        )
        
        timing_results = []
        
        for i, symbol in enumerate(large_symbols):
            print(f"\n📊 {i+1:2}/{len(large_symbols)} {symbol} 분석:")
            
            start_time = time.time()
            success = collector.collect_stock_data(symbol, period="5y")
            end_time = time.time()
            
            duration = end_time - start_time
            
            if success:
                # 데이터 건수 확인
                result = collector.db.execute_query(
                    "SELECT COUNT(*) as count FROM stock_data WHERE symbol = ?",
                    (symbol,)
                )
                
                if not result.empty:
                    record_count = result.iloc[0]['count']
                    per_record_time = duration / record_count if record_count > 0 else 0
                    
                    timing_results.append({
                        'symbol': symbol,
                        'duration': duration,
                        'records': record_count,
                        'per_record': per_record_time
                    })
                    
                    print(f"  ✅ 수집 완료: {duration:.2f}초")
                    print(f"  📈 저장된 레코드: {record_count:,}개")
                    print(f"  ⚡ 레코드당 평균: {per_record_time:.6f}초")
                    
                    # 처리 속도 분류
                    if duration < 10:
                        speed_class = "🚀 매우 빠름"
                    elif duration < 20:
                        speed_class = "⚡ 빠름"
                    elif duration < 30:
                        speed_class = "🚶 보통"
                    else:
                        speed_class = "🐌 느림"
                    
                    print(f"  📊 속도 등급: {speed_class}")
                
            else:
                print(f"  ❌ 수집 실패: {duration:.2f}초")
            
            # API 제한 방지
            time.sleep(random.uniform(1.5, 2.5))
        
        # 통계 분석
        if timing_results:
            durations = [r['duration'] for r in timing_results]
            records = [r['records'] for r in timing_results]
            per_records = [r['per_record'] for r in timing_results]
            
            print("\n📈 종합 통계:")
            print(f"  성공한 종목: {len(timing_results)}개")
            print(f"  평균 수집 시간: {sum(durations)/len(durations):.2f}초")
            print(f"  최빠름: {min(durations):.2f}초")
            print(f"  최느림: {max(durations):.2f}초")
            print(f"  총 레코드: {sum(records):,}개")
            print(f"  평균 레코드수: {sum(records)/len(records):,.0f}개/종목")
            print(f"  전체 처리 시간: {sum(durations):.2f}초")
            print(f"  처리 속도: {len(timing_results)/sum(durations):.2f} 종목/초")
            
            # 7000개 종목 예상 시간
            estimated_time = (sum(durations)/len(durations)) * 7000
            estimated_hours = estimated_time / 3600
            print(f"\n🔮 7000개 종목 예상 시간:")
            print(f"  순차 처리: {estimated_time:,.0f}초 ({estimated_hours:.1f}시간)")
            print(f"  2병렬 처리: {estimated_time/2:,.0f}초 ({estimated_hours/2:.1f}시간)")
            print(f"  5병렬 처리: {estimated_time/5:,.0f}초 ({estimated_hours/5:.1f}시간)")
        
        collector.close()
        
    except Exception as e:
        print(f"❌ 타이밍 분석 실패: {e}")
    
    print("=" * 60)
    print("✅ 대규모 타이밍 분석 완료")

if __name__ == "__main__":
    print("🚀 5년 데이터 성능 테스트 시작")
    print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 1. 대규모 개별 타이밍 분석
    test_large_scale_timing()
    
    # 2. 샤딩 vs 단일 DB 성능 비교 (5년)
    test_5year_performance()
    
    print(f"⏰ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 5년 데이터 성능 테스트 완료")
