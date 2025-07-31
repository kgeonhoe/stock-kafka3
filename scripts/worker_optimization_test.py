#!/usr/bin/env python3
"""
Yahoo Finance API 워커 수 최적화 테스트 스크립트

이 스크립트는 Yahoo Finance API 호출 시 최적의 워커 수를 찾기 위한 테스트를 수행합니다.
API 제한을 피하면서 최대 성능을 얻을 수 있는 설정을 찾습니다.

사용법:
    python worker_optimization_test.py [--quick] [--workers 1,3,5,7,10] [--symbols 20]
"""

import yfinance as yf
import time
import random
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional


class WorkerOptimizationTester:
    """워커 수 최적화 테스트 클래스"""
    
    def __init__(self):
        self.test_symbols = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", 
            "META", "NVDA", "AMD", "INTC", "ORCL",
            "IBM", "CRM", "ADBE", "NFLX", "PYPL",
            "UBER", "LYFT", "ZOOM", "ZM", "DOCU",
            "SNOW", "PLTR", "RBLX", "ROKU", "SQ",
            "SHOP", "SPOT", "TWLO", "OKTA", "MDB"
        ]
        self.results = {}
    
    def test_single_symbol(self, symbol: str, delay_range: tuple = (0.1, 0.5)) -> Dict:
        """단일 심볼 테스트 (실제 API 호출)"""
        start_time = time.time()
        
        try:
            # API 호출 제한 방지 지연
            delay = random.uniform(*delay_range)
            time.sleep(delay)
            
            # yfinance 호출
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d", timeout=10)
            
            if hist.empty:
                return {
                    'symbol': symbol,
                    'success': False,
                    'duration': time.time() - start_time,
                    'error': 'Empty data'
                }
            
            return {
                'symbol': symbol,
                'success': True,
                'duration': time.time() - start_time,
                'records': len(hist),
                'error': None
            }
            
        except Exception as e:
            error_msg = str(e).lower()
            is_rate_limit = any(keyword in error_msg for keyword in 
                              ['429', 'rate limit', 'too many', 'quota', 'throttle'])
            
            return {
                'symbol': symbol,
                'success': False,
                'duration': time.time() - start_time,
                'error': str(e),
                'is_rate_limit': is_rate_limit
            }
    
    def test_worker_count(self, worker_count: int, symbol_batch_size: int = 10) -> Dict:
        """특정 워커 수로 테스트 실행"""
        print(f"\n🧪 워커 수 {worker_count}개 테스트 시작...")
        
        test_symbols = self.test_symbols[:symbol_batch_size]
        start_time = time.time()
        
        results = []
        failed_count = 0
        api_limit_errors = 0
        
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            # 작업 제출
            future_to_symbol = {
                executor.submit(self.test_single_symbol, symbol): symbol 
                for symbol in test_symbols
            }
            
            # 결과 수집
            for future in as_completed(future_to_symbol):
                result = future.result()
                results.append(result)
                
                if not result['success']:
                    failed_count += 1
                    if result.get('is_rate_limit', False):
                        api_limit_errors += 1
        
        total_time = time.time() - start_time
        success_count = len(results) - failed_count
        
        # 통계 계산
        successful_durations = [r['duration'] for r in results if r['success']]
        avg_duration = sum(successful_durations) / len(successful_durations) if successful_durations else 0
        
        test_result = {
            'worker_count': worker_count,
            'total_symbols': len(test_symbols),
            'success_count': success_count,
            'failed_count': failed_count,
            'api_limit_errors': api_limit_errors,
            'total_time': total_time,
            'avg_symbol_time': avg_duration,
            'symbols_per_second': success_count / total_time if total_time > 0 else 0,
            'success_rate': success_count / len(test_symbols) * 100,
            'efficiency_score': self._calculate_efficiency_score(success_count, len(test_symbols), 
                                                               total_time, api_limit_errors)
        }
        
        self.results[worker_count] = test_result
        
        print(f"✅ 워커 {worker_count}개 완료:")
        print(f"   성공: {success_count}/{len(test_symbols)} ({test_result['success_rate']:.1f}%)")
        print(f"   API 제한 오류: {api_limit_errors}개")
        print(f"   총 시간: {total_time:.1f}초")
        print(f"   처리율: {test_result['symbols_per_second']:.2f} symbols/sec")
        print(f"   효율성 점수: {test_result['efficiency_score']:.2f}")
        
        return test_result
    
    def _calculate_efficiency_score(self, success_count: int, total_count: int, 
                                  total_time: float, api_errors: int) -> float:
        """효율성 점수 계산"""
        if total_count == 0 or total_time == 0:
            return 0
        
        # 기본 점수: 성공률 * 처리 속도
        base_score = (success_count / total_count) * (success_count / total_time)
        
        # API 에러 페널티 (에러당 -10% 감점)
        error_penalty = api_errors * 0.1
        
        return max(0, base_score - error_penalty)
    
    def run_optimization_test(self, worker_counts: List[int] = None, 
                            symbol_batch_size: int = 15, cooldown_time: int = 10) -> Optional[int]:
        """전체 최적화 테스트 실행"""
        if worker_counts is None:
            worker_counts = [1, 3, 5, 7, 10, 15]
        
        print("🚀 워커 수 최적화 테스트 시작")
        print("=" * 60)
        print(f"테스트 설정:")
        print(f"  - 워커 수: {worker_counts}")
        print(f"  - 심볼 수: {symbol_batch_size}")
        print(f"  - 쿨다운: {cooldown_time}초")
        print("=" * 60)
        
        for i, worker_count in enumerate(worker_counts):
            try:
                self.test_worker_count(worker_count, symbol_batch_size)
                
                # 테스트 간 쿨다운 (API 제한 방지)
                if i < len(worker_counts) - 1:
                    print(f"⏰ 쿨다운 대기 중... ({cooldown_time}초)")
                    time.sleep(cooldown_time)
                    
            except Exception as e:
                print(f"❌ 워커 {worker_count}개 테스트 실패: {e}")
        
        return self.analyze_results()
    
    def quick_test(self, test_symbols: List[str] = None) -> Optional[int]:
        """빠른 테스트 (소규모)"""
        if test_symbols is None:
            test_symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
        
        print("⚡ 빠른 워커 테스트 시작")
        print(f"테스트 심볼: {', '.join(test_symbols)}")
        
        test_workers = [3, 5, 8]
        results = {}
        
        for workers in test_workers:
            print(f"\n🔸 워커 {workers}개 테스트...")
            start_time = time.time()
            
            success_count = 0
            api_errors = 0
            
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(self.test_single_symbol, symbol) 
                    for symbol in test_symbols
                ]
                
                for future in as_completed(futures):
                    result = future.result()
                    if result['success']:
                        success_count += 1
                    elif result.get('is_rate_limit', False):
                        api_errors += 1
            
            total_time = time.time() - start_time
            success_rate = success_count / len(test_symbols) * 100
            
            results[workers] = {
                'success_rate': success_rate,
                'api_errors': api_errors,
                'total_time': total_time,
                'throughput': success_count / total_time if total_time > 0 else 0,
                'efficiency_score': self._calculate_efficiency_score(
                    success_count, len(test_symbols), total_time, api_errors)
            }
            
            print(f"   ✅ 성공률: {success_rate:.1f}% | API오류: {api_errors}개 | 시간: {total_time:.1f}초")
            
            # 테스트 간 간격
            if workers != test_workers[-1]:
                time.sleep(3)
        
        # 결과 요약
        print(f"\n📋 빠른 테스트 결과:")
        best_workers = max(results.keys(), key=lambda w: results[w]['efficiency_score'])
        
        for workers, result in results.items():
            marker = "🎯" if workers == best_workers else "  "
            print(f"{marker} {workers}개 워커: {result['success_rate']:.1f}% 성공률, "
                  f"{result['throughput']:.2f} req/sec, 점수: {result['efficiency_score']:.2f}")
        
        print(f"\n💡 추천: {best_workers}개 워커 사용")
        return best_workers
    
    def analyze_results(self) -> Optional[int]:
        """결과 분석 및 최적 워커 수 추천"""
        if not self.results:
            print("❌ 분석할 결과가 없습니다.")
            return None
        
        print(f"\n📊 워커 수 최적화 테스트 결과 분석")
        print("=" * 70)
        
        # 결과 테이블 출력
        print(f"{'워커수':<6} {'성공률':<8} {'API제한':<8} {'처리율':<12} {'평균시간':<10} {'효율점수':<8}")
        print("-" * 70)
        
        best_worker_count = None
        best_score = 0
        
        for worker_count in sorted(self.results.keys()):
            result = self.results[worker_count]
            
            if result['efficiency_score'] > best_score:
                best_score = result['efficiency_score']
                best_worker_count = worker_count
            
            print(f"{worker_count:<6} {result['success_rate']:<7.1f}% {result['api_limit_errors']:<8} "
                  f"{result['symbols_per_second']:<11.2f} {result['avg_symbol_time']:<9.2f}s "
                  f"{result['efficiency_score']:<7.2f}")
        
        if best_worker_count:
            best_result = self.results[best_worker_count]
            print(f"\n🎯 최적 설정:")
            print(f"   워커 수: {best_worker_count}개")
            print(f"   성공률: {best_result['success_rate']:.1f}%")
            print(f"   처리율: {best_result['symbols_per_second']:.2f} symbols/sec")
            print(f"   효율성 점수: {best_result['efficiency_score']:.2f}")
            
            # 프로덕션 설정 제안
            self._suggest_production_config(best_worker_count, best_result)
        
        return best_worker_count
    
    def _suggest_production_config(self, worker_count: int, result: Dict):
        """프로덕션 설정 제안"""
        batch_size = max(50, 100 // worker_count)
        estimated_time_per_1000 = 1000 / (result['symbols_per_second'] * 60) if result['symbols_per_second'] > 0 else 60
        
        print(f"\n🔧 프로덕션 설정 제안:")
        print(f"   MAX_WORKERS = {worker_count}")
        print(f"   BATCH_SIZE = {batch_size}")
        print(f"   예상 처리 시간 (1000개 심볼): {estimated_time_per_1000:.1f}분")
        
        # 나스닥 전체 심볼 기준 예상 시간
        nasdaq_total = 3000
        total_time_hours = nasdaq_total / (result['symbols_per_second'] * 3600) if result['symbols_per_second'] > 0 else 2
        print(f"   예상 처리 시간 (나스닥 3000개): {total_time_hours:.1f}시간")
        
        # 권장 스케줄링
        if total_time_hours < 1:
            print(f"   권장 스케줄: 매시간 실행 가능")
        elif total_time_hours < 6:
            print(f"   권장 스케줄: 하루 4회 실행 (6시간 간격)")
        else:
            print(f"   권장 스케줄: 하루 1회 실행 (야간 배치)")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='Yahoo Finance API 워커 수 최적화 테스트')
    parser.add_argument('--quick', action='store_true', help='빠른 테스트 실행 (5개 심볼만)')
    parser.add_argument('--workers', type=str, help='테스트할 워커 수 (예: 1,3,5,7,10)')
    parser.add_argument('--symbols', type=int, default=15, help='테스트할 심볼 수')
    parser.add_argument('--cooldown', type=int, default=10, help='테스트 간 쿨다운 시간(초)')
    
    args = parser.parse_args()
    
    tester = WorkerOptimizationTester()
    
    print(f"🕒 테스트 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        if args.quick:
            # 빠른 테스트
            optimal_workers = tester.quick_test()
        else:
            # 전체 테스트
            worker_counts = None
            if args.workers:
                worker_counts = [int(w.strip()) for w in args.workers.split(',')]
            
            optimal_workers = tester.run_optimization_test(
                worker_counts=worker_counts,
                symbol_batch_size=args.symbols,
                cooldown_time=args.cooldown
            )
        
        if optimal_workers:
            print(f"\n🎉 테스트 완료! 최적 워커 수: {optimal_workers}개")
        else:
            print(f"\n❌ 테스트 실패 또는 결과 없음")
            
    except KeyboardInterrupt:
        print(f"\n⚠️ 사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")
    
    print(f"🕒 테스트 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
