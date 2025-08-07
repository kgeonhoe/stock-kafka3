#!/usr/bin/env python3
"""
메모리 최적화된 YFinance 수집기

주요 개선사항:
1. 배치당 즉시 저장으로 메모리 사용량 최소화
2. 5개 워커로 API 제한 회피
3. 가비지 컬렉션 강화
4. 메모리 모니터링
"""

import sys
import os
sys.path.insert(0, '/home/grey1/stock-kafka3/airflow/plugins')
sys.path.insert(0, '/home/grey1/stock-kafka3/airflow/common')

import yfinance as yf
import time
import gc
import psutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
from duckdb_manager import DuckDBManager


class MemoryOptimizedCollector:
    """메모리 최적화된 주식 데이터 수집기"""
    
    def __init__(self, db_path: str = "/home/grey1/stock-kafka3/data/duckdb/stock_data.db"):
        self.db = DuckDBManager(db_path)
        self.process = psutil.Process()
        self.stats = {
            'total_processed': 0,
            'success_count': 0,
            'fail_count': 0,
            'api_limit_errors': 0,
            'start_time': None,
            'memory_peak': 0
        }
    
    def get_memory_usage(self) -> float:
        """현재 메모리 사용량 (MB)"""
        memory_mb = self.process.memory_info().rss / 1024 / 1024
        self.stats['memory_peak'] = max(self.stats['memory_peak'], memory_mb)
        return memory_mb
    
    def force_cleanup(self):
        """강제 메모리 정리"""
        gc.collect()
        time.sleep(0.1)  # GC 완료 대기
    
    def collect_single_symbol(self, symbol: str, period: str = "1y") -> Dict[str, Any]:
        """단일 종목 수집 및 즉시 저장"""
        start_time = time.time()
        
        try:
            # 메모리 체크
            memory_before = self.get_memory_usage()
            
            # API 제한 방지 지연 (랜덤 0.5-1.5초)
            import random
            delay = random.uniform(0.5, 1.5)
            time.sleep(delay)
            
            # 기존 데이터 확인 (증분 업데이트)
            last_date = self.db.get_latest_date(symbol)
            
            if last_date:
                # 증분 업데이트: 마지막 날짜부터 현재까지
                start_date = last_date + timedelta(days=1)
                end_date = datetime.now().date()
                
                if start_date >= end_date:
                    return {
                        'symbol': symbol,
                        'status': 'skipped',
                        'reason': 'Already up to date',
                        'duration': time.time() - start_time,
                        'memory_mb': self.get_memory_usage()
                    }
                
                # 날짜 범위로 수집
                ticker = yf.Ticker(symbol)
                hist = ticker.history(start=start_date, end=end_date, timeout=15)
                update_type = "증분"
            else:
                # 신규 종목: 전체 기간 수집
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period=period, timeout=15)
                update_type = "신규"
            
            if hist.empty:
                return {
                    'symbol': symbol,
                    'status': 'empty',
                    'reason': 'No data available',
                    'duration': time.time() - start_time,
                    'memory_mb': self.get_memory_usage()
                }
            
            # 즉시 저장 (메모리 절약)
            records_saved = 0
            hist_reset = hist.reset_index()
            
            for _, row in hist_reset.iterrows():
                success = self.db.save_stock_data({
                    'symbol': symbol,
                    'date': row['Date'].date(),
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume'])
                })
                if success:
                    records_saved += 1
            
            # 메모리 정리
            del hist, hist_reset, ticker
            self.force_cleanup()
            
            memory_after = self.get_memory_usage()
            
            return {
                'symbol': symbol,
                'status': 'success',
                'update_type': update_type,
                'records_saved': records_saved,
                'duration': time.time() - start_time,
                'memory_before_mb': memory_before,
                'memory_after_mb': memory_after,
                'memory_delta_mb': memory_after - memory_before
            }
            
        except Exception as e:
            error_msg = str(e).lower()
            is_api_limit = any(keyword in error_msg for keyword in 
                             ['429', 'rate limit', 'too many', 'quota', 'throttle', 'forbidden'])
            
            if is_api_limit:
                self.stats['api_limit_errors'] += 1
                print(f"🚫 {symbol}: API 제한 감지 - 긴 대기 후 스킵")
                time.sleep(5)  # API 제한시 추가 대기
            
            return {
                'symbol': symbol,
                'status': 'error',
                'error': str(e),
                'is_api_limit': is_api_limit,
                'duration': time.time() - start_time,
                'memory_mb': self.get_memory_usage()
            }
    
    def collect_batch(self, symbols: List[str], period: str = "1y", max_workers: int = 5) -> Dict[str, Any]:
        """배치 단위 수집 (메모리 최적화)"""
        batch_start_time = time.time()
        memory_start = self.get_memory_usage()
        
        print(f"\n📦 배치 시작: {len(symbols)}개 종목, {max_workers}개 워커")
        print(f"💾 시작 메모리: {memory_start:.1f} MB")
        
        batch_results = []
        success_count = 0
        fail_count = 0
        api_errors = 0
        
        # 배치 처리
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 작업 제출
            future_to_symbol = {
                executor.submit(self.collect_single_symbol, symbol, period): symbol 
                for symbol in symbols
            }
            
            # 결과 수집
            for i, future in enumerate(as_completed(future_to_symbol), 1):
                symbol = future_to_symbol[future]
                result = future.result()
                batch_results.append(result)
                
                # 통계 업데이트
                if result['status'] == 'success':
                    success_count += 1
                    print(f"✅ {symbol} ({result['update_type']}): {result['records_saved']}개 저장 "
                          f"({result['duration']:.1f}초, {result['memory_after_mb']:.1f}MB)")
                elif result['status'] == 'skipped':
                    print(f"⏭️ {symbol}: {result['reason']}")
                elif result['status'] == 'empty':
                    print(f"⚠️ {symbol}: 데이터 없음")
                else:
                    fail_count += 1
                    if result.get('is_api_limit', False):
                        api_errors += 1
                        print(f"🚫 {symbol}: API 제한")
                    else:
                        print(f"❌ {symbol}: {result.get('error', 'Unknown error')[:50]}...")
                
                # 진행률 표시
                if i % 5 == 0 or i == len(symbols):
                    current_memory = self.get_memory_usage()
                    print(f"   📊 진행률: {i}/{len(symbols)} ({i/len(symbols)*100:.1f}%) "
                          f"메모리: {current_memory:.1f}MB")
        
        # 배치 완료 후 강제 정리
        self.force_cleanup()
        
        batch_time = time.time() - batch_start_time
        memory_end = self.get_memory_usage()
        
        batch_summary = {
            'total_symbols': len(symbols),
            'success_count': success_count,
            'fail_count': fail_count,
            'api_errors': api_errors,
            'batch_time': batch_time,
            'memory_start_mb': memory_start,
            'memory_end_mb': memory_end,
            'memory_delta_mb': memory_end - memory_start,
            'throughput': success_count / batch_time if batch_time > 0 else 0
        }
        
        print(f"\n📋 배치 완료:")
        print(f"   ✅ 성공: {success_count}/{len(symbols)} ({success_count/len(symbols)*100:.1f}%)")
        print(f"   🚫 API 제한: {api_errors}개")
        print(f"   ⏱️ 소요시간: {batch_time:.1f}초")
        print(f"   💾 메모리: {memory_start:.1f}MB → {memory_end:.1f}MB (델타: {memory_end-memory_start:+.1f}MB)")
        print(f"   🚀 처리율: {batch_summary['throughput']:.2f} symbols/sec")
        
        return batch_summary
    
    def collect_all_symbols(self, symbols: List[str] = None, period: str = "1y", 
                          max_workers: int = 5, batch_size: int = 20) -> Dict[str, Any]:
        """전체 종목 수집 (메모리 최적화)"""
        self.stats['start_time'] = time.time()
        
        # 심볼 목록 준비
        if symbols is None:
            symbols = self.db.get_active_symbols()
            if not symbols:
                return {'error': 'No active symbols found'}
        
        total_symbols = len(symbols)
        print(f"🚀 메모리 최적화 수집 시작")
        print(f"📊 설정: {total_symbols}개 종목, {max_workers}개 워커, {batch_size}개 배치")
        print(f"📅 기간: {period}")
        print(f"💾 초기 메모리: {self.get_memory_usage():.1f} MB")
        
        # 배치별 처리
        batch_summaries = []
        total_batches = (total_symbols + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, total_symbols)
            batch_symbols = symbols[batch_start:batch_end]
            
            print(f"\n🔄 배치 {batch_num + 1}/{total_batches}")
            
            # 배치 처리
            batch_summary = self.collect_batch(batch_symbols, period, max_workers)
            batch_summaries.append(batch_summary)
            
            # 통계 업데이트
            self.stats['total_processed'] += batch_summary['total_symbols']
            self.stats['success_count'] += batch_summary['success_count']
            self.stats['fail_count'] += batch_summary['fail_count']
            self.stats['api_limit_errors'] += batch_summary['api_errors']
            
            # 배치 간 쿨다운 (API 제한 방지)
            if batch_num < total_batches - 1:
                cooldown = min(10, max(3, batch_summary['api_errors'] * 2))
                print(f"⏰ 배치 간 쿨다운: {cooldown}초...")
                time.sleep(cooldown)
        
        # 최종 결과
        total_time = time.time() - self.stats['start_time']
        final_memory = self.get_memory_usage()
        
        final_result = {
            'total_symbols': total_symbols,
            'total_processed': self.stats['total_processed'],
            'success_count': self.stats['success_count'],
            'fail_count': self.stats['fail_count'],
            'api_limit_errors': self.stats['api_limit_errors'],
            'success_rate': self.stats['success_count'] / total_symbols * 100,
            'total_time': total_time,
            'average_throughput': self.stats['success_count'] / total_time if total_time > 0 else 0,
            'memory_peak_mb': self.stats['memory_peak'],
            'memory_final_mb': final_memory,
            'batch_summaries': batch_summaries
        }
        
        print(f"\n🎉 전체 수집 완료!")
        print(f"✅ 성공: {self.stats['success_count']}/{total_symbols} ({final_result['success_rate']:.1f}%)")
        print(f"🚫 API 제한: {self.stats['api_limit_errors']}개")
        print(f"⏱️ 총 시간: {total_time/60:.1f}분")
        print(f"🚀 평균 처리율: {final_result['average_throughput']:.2f} symbols/sec")
        print(f"💾 메모리 피크: {self.stats['memory_peak']:.1f} MB")
        
        return final_result
    
    def close(self):
        """리소스 정리"""
        if hasattr(self, 'db'):
            self.db.close()


def main():
    """메인 실행 함수"""
    print("🚀 메모리 최적화 주식 데이터 수집기")
    print("=" * 50)
    
    collector = MemoryOptimizedCollector()
    
    try:
        # 설정
        MAX_WORKERS = 5      # API 제한 고려
        BATCH_SIZE = 20      # 메모리 절약
        PERIOD = "1y"        # 1년 데이터
        
        # 수집 실행
        result = collector.collect_all_symbols(
            max_workers=MAX_WORKERS,
            batch_size=BATCH_SIZE,
            period=PERIOD
        )
        
        if 'error' in result:
            print(f"❌ 오류: {result['error']}")
        else:
            print(f"\n📈 최종 성과:")
            print(f"   성공률: {result['success_rate']:.1f}%")
            print(f"   처리 속도: {result['average_throughput']:.2f} symbols/sec")
            print(f"   메모리 효율: 피크 {result['memory_peak_mb']:.1f}MB")
            
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
    finally:
        collector.close()


if __name__ == "__main__":
    main()
