#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
부하테스트 및 성능 모니터링 실행 스크립트
"""

import subprocess
import sys
import os
import time
import threading
import json
from datetime import datetime
import argparse

# 프로젝트 경로 추가
sys.path.insert(0, '/home/grey1/stock-kafka3/monitoring')
sys.path.insert(0, '/home/grey1/stock-kafka3/common')

from performance_monitor import monitor, CircuitBreaker

def run_kafka_stress_test(duration_minutes=5, threads=10, messages_per_thread=500):
    """Kafka 스트레스 테스트"""
    print(f"📤 Kafka 스트레스 테스트 시작!")
    print(f"⏱️ 실행 시간: {duration_minutes}분")
    print(f"🧵 스레드 수: {threads}")
    print(f"📨 스레드당 메시지: {messages_per_thread}")
    
    try:
        from kafka import KafkaProducer
        import random
        
        def producer_thread(thread_id, message_count):
            """Kafka 프로듀서 스레드"""
            try:
                producer = KafkaProducer(
                    bootstrap_servers=['localhost:9092'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    retries=3,
                    retry_backoff_ms=1000
                )
                
                symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
                success_count = 0
                error_count = 0
                
                start_time = time.time()
                
                for i in range(message_count):
                    try:
                        message = {
                            'symbol': random.choice(symbols),
                            'price': round(random.uniform(100, 500), 2),
                            'volume': random.randint(1000, 100000),
                            'timestamp': time.time(),
                            'thread_id': thread_id,
                            'message_id': i,
                            'test_type': 'stress_test'
                        }
                        
                        # 메시지 전송
                        future = producer.send('yfinance-stock-data', message)
                        future.get(timeout=10)  # 전송 확인
                        
                        success_count += 1
                        monitor.log_kafka_message('yfinance-stock-data', 'success')
                        
                        # 처리량 조절 (너무 빠르면 부하 조절)
                        if i % 50 == 0:
                            time.sleep(0.1)
                        
                    except Exception as e:
                        error_count += 1
                        monitor.log_kafka_message('yfinance-stock-data', 'error', error=str(e))
                        print(f"❌ Thread {thread_id} 메시지 {i} 전송 실패: {e}")
                
                end_time = time.time()
                duration = end_time - start_time
                
                producer.flush()
                producer.close()
                
                print(f"✅ Thread {thread_id} 완료:")
                print(f"   📊 성공: {success_count}, 실패: {error_count}")
                print(f"   ⏱️ 소요시간: {duration:.2f}초")
                print(f"   📈 처리율: {success_count/duration:.2f} msg/sec")
                
                return {
                    'thread_id': thread_id,
                    'success_count': success_count,
                    'error_count': error_count,
                    'duration': duration,
                    'throughput': success_count / duration
                }
                
            except Exception as e:
                print(f"❌ Thread {thread_id} 전체 실패: {e}")
                return {
                    'thread_id': thread_id,
                    'success_count': 0,
                    'error_count': message_count,
                    'duration': 0,
                    'throughput': 0,
                    'error': str(e)
                }
        
        # 스레드 실행
        thread_objects = []
        for i in range(threads):
            thread = threading.Thread(
                target=lambda tid=i: producer_thread(tid, messages_per_thread)
            )
            thread_objects.append(thread)
            thread.start()
        
        # 모든 스레드 완료 대기
        for thread in thread_objects:
            thread.join()
        
        print("🎉 Kafka 스트레스 테스트 완료!")
        
    except ImportError:
        print("❌ kafka-python이 설치되지 않았습니다.")
    except Exception as e:
        print(f"❌ Kafka 스트레스 테스트 실패: {e}")

def run_locust_test(users=10, spawn_rate=2, host="http://localhost:8080", duration="5m"):
    """Locust 부하테스트 실행"""
    
    print("🚀 Locust 부하테스트 시작!")
    print(f"👥 동시 사용자 수: {users}")
    print(f"📈 사용자 증가율: {spawn_rate}/초")
    print(f"🎯 대상 호스트: {host}")
    print(f"⏱️ 실행 시간: {duration}")
    
    # Locust 파일 경로 확인
    locust_file = "/home/grey1/stock-kafka3/load_tests/stock_api_load_test.py"
    if not os.path.exists(locust_file):
        print(f"❌ Locust 파일을 찾을 수 없습니다: {locust_file}")
        return False
    
    # 결과 파일 경로
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_html = f"load_test_report_{timestamp}.html"
    report_csv = f"load_test_results_{timestamp}"
    
    # Locust 명령어 구성
    cmd = [
        "locust",
        "-f", locust_file,
        "--users", str(users),
        "--spawn-rate", str(spawn_rate),
        "--host", host,
        "--run-time", duration,
        "--html", report_html,
        "--csv", report_csv,
        "--headless"  # 웹 UI 없이 실행
    ]
    
    try:
        print(f"🔧 실행 명령어: {' '.join(cmd)}")
        
        # 부하테스트 실행
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=600,  # 10분 타임아웃
            cwd="/home/grey1/stock-kafka3"
        )
        
        print("✅ Locust 부하테스트 완료!")
        print(f"📊 HTML 리포트: {report_html}")
        print(f"📈 CSV 데이터: {report_csv}_*.csv")
        
        if result.stdout:
            print("📋 Locust 출력:")
            print(result.stdout)
            
        if result.stderr:
            print("⚠️ Locust 경고/오류:")
            print(result.stderr)
            
        return True
        
    except subprocess.TimeoutExpired:
        print("⏰ 부하테스트 시간 초과 (10분)")
        return False
    except FileNotFoundError:
        print("❌ Locust가 설치되지 않았습니다. 'pip install locust' 실행 필요")
        return False
    except Exception as e:
        print(f"❌ 부하테스트 실행 오류: {e}")
        return False

def run_system_monitoring_test(duration_minutes=10):
    """시스템 모니터링 테스트"""
    print(f"🔍 시스템 모니터링 테스트 시작 ({duration_minutes}분)")
    
    # Circuit Breaker 테스트용
    api_breaker = CircuitBreaker(failure_threshold=3, timeout=30, name="API")
    db_breaker = CircuitBreaker(failure_threshold=5, timeout=60, name="Database")
    
    def simulate_api_calls():
        """API 호출 시뮬레이션"""
        import random
        
        for i in range(100):
            try:
                # 70% 성공률로 시뮬레이션
                def api_call():
                    if random.random() < 0.7:
                        time.sleep(random.uniform(0.1, 0.5))
                        return f"API 응답 {i}"
                    else:
                        raise Exception(f"API 오류 {i}")
                
                result = api_breaker.call(api_call)
                monitor.log_request("GET", f"/api/test/{i}", 200, 0.3)
                
            except Exception as e:
                monitor.log_request("GET", f"/api/test/{i}", 500, 0, error=str(e))
            
            time.sleep(2)
    
    def simulate_db_operations():
        """데이터베이스 작업 시뮬레이션"""
        import random
        
        for i in range(50):
            try:
                # 85% 성공률로 시뮬레이션
                def db_operation():
                    if random.random() < 0.85:
                        time.sleep(random.uniform(0.05, 0.2))
                        return f"DB 결과 {i}"
                    else:
                        raise Exception(f"DB 오류 {i}")
                
                start_time = time.time()
                result = db_breaker.call(db_operation)
                duration = time.time() - start_time
                
                monitor.log_database_operation("SELECT", "success", duration)
                
            except Exception as e:
                monitor.log_database_operation("SELECT", "error", error=str(e))
            
            time.sleep(4)
    
    # 병렬 시뮬레이션 실행
    api_thread = threading.Thread(target=simulate_api_calls)
    db_thread = threading.Thread(target=simulate_db_operations)
    
    api_thread.start()
    db_thread.start()
    
    # 모니터링 실행
    time.sleep(duration_minutes * 60)
    
    api_thread.join()
    db_thread.join()
    
    # Circuit Breaker 상태 출력
    print(f"🔌 API Circuit Breaker 상태: {api_breaker.get_state()}")
    print(f"🗃️ DB Circuit Breaker 상태: {db_breaker.get_state()}")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='Stock Kafka 부하테스트 도구')
    parser.add_argument('--test-type', choices=['kafka', 'locust', 'monitoring', 'all'], 
                       default='all', help='실행할 테스트 타입')
    parser.add_argument('--users', type=int, default=10, help='Locust 동시 사용자 수')
    parser.add_argument('--duration', default='5m', help='테스트 실행 시간')
    parser.add_argument('--kafka-threads', type=int, default=10, help='Kafka 스레드 수')
    parser.add_argument('--kafka-messages', type=int, default=500, help='스레드당 메시지 수')
    
    args = parser.parse_args()
    
    print("🚀 Stock Kafka 부하테스트 시작!")
    print(f"📅 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 성능 모니터링 시작
    monitor.start_prometheus_server()
    monitor.start_monitoring(interval=5)
    
    try:
        if args.test_type in ['kafka', 'all']:
            print("\n1️⃣ Kafka 스트레스 테스트")
            run_kafka_stress_test(
                duration_minutes=5,
                threads=args.kafka_threads,
                messages_per_thread=args.kafka_messages
            )
            time.sleep(5)
        
        if args.test_type in ['locust', 'all']:
            print("\n2️⃣ Locust API 부하테스트")
            run_locust_test(
                users=args.users,
                duration=args.duration
            )
            time.sleep(5)
        
        if args.test_type in ['monitoring', 'all']:
            print("\n3️⃣ 시스템 모니터링 테스트")
            run_system_monitoring_test(duration_minutes=3)
        
        # 최종 시스템 상태 체크
        print("\n📊 최종 시스템 상태:")
        health = monitor.get_health_status()
        print(json.dumps(health, indent=2, ensure_ascii=False))
        
    except KeyboardInterrupt:
        print("\n⏹️ 사용자가 테스트를 중단했습니다.")
    except Exception as e:
        print(f"\n❌ 테스트 실행 중 오류: {e}")
    finally:
        # 모니터링 중지
        monitor.stop_monitoring()
        print("\n🎉 부하테스트 완료!")
        print(f"📅 종료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
