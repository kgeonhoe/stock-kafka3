#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 부하테스트 모듈 (새로운 버전)
- 다중 토픽 지원
- 워커 수량 조절
- API 호출 테스트
- 실시간 모니터링
"""

import json
import time
import threading
import random
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging
import concurrent.futures
import psutil
import requests

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LoadTestConfig:
    """부하테스트 설정"""
    duration_minutes: int = 5
    kafka_producer_workers: int = 10  # Kafka Producer 워커 수
    kafka_consumer_workers: int = 5   # Kafka Consumer 워커 수
    api_call_workers: int = 4         # API 호출 워커 수 (BulkDataCollector의 max_workers)
    messages_per_worker: int = 1000
    topics: List[str] = None
    api_endpoints: List[str] = None
    signal_probability: float = 0.3
    
    def __post_init__(self):
        if self.topics is None:
            self.topics = ["realtime-stock", "yfinance-stock-data"]
        if self.api_endpoints is None:
            self.api_endpoints = [
                "http://localhost:8080/api/kafka/health",
                "http://localhost:8081/api/v1/health",  # Airflow API
                "http://localhost:6379/ping"            # Redis API (가상)
            ]

@dataclass
class WorkerStats:
    """워커 통계"""
    worker_id: str
    worker_type: str
    success_count: int = 0
    error_count: int = 0
    total_messages: int = 0
    duration: float = 0.0
    throughput: float = 0.0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class KafkaLoadTester:
    """Kafka 부하테스트 클래스"""
    
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.results = []
        self.is_running = False
        self.start_time = None
        
    def generate_stock_message(self, worker_id: str, message_id: int) -> Dict:
        """주식 데이터 메시지 생성"""
        symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
            'AMD', 'INTC', 'CRM', 'ORCL', 'IBM', 'CSCO', 'QCOM'
        ]
        
        symbol = random.choice(symbols)
        base_price = random.uniform(100, 500)
        
        message = {
            'symbol': symbol,
            'price': round(base_price + random.uniform(-10, 10), 2),
            'volume': random.randint(1000, 1000000),
            'timestamp': time.time(),
            'datetime': datetime.now().isoformat(),
            'worker_id': worker_id,
            'message_id': message_id,
            'test_type': 'load_test',
            'change': round(random.uniform(-5, 5), 2),
            'change_percent': round(random.uniform(-10, 10), 2),
            # 기술적 지표 (시뮬레이션)
            'rsi': random.uniform(20, 80),
            'macd': random.uniform(-2, 2),
            'bb_upper': round(base_price * 1.05, 2),
            'bb_lower': round(base_price * 0.95, 2),
            # 신호 감지 (확률 기반)
            'signal': self._generate_signal() if random.random() < self.config.signal_probability else None
        }
        
        return message
    
    def _generate_signal(self) -> Dict:
        """신호 생성"""
        signal_types = [
            'bollinger_upper_touch', 'bollinger_lower_touch',
            'rsi_overbought', 'rsi_oversold',
            'macd_bullish_crossover', 'macd_bearish_crossover',
            'volume_spike', 'price_breakout'
        ]
        
        return {
            'type': random.choice(signal_types),
            'strength': random.uniform(0.5, 1.0),
            'timestamp': time.time()
        }
    
    def kafka_producer_worker(self, worker_id: str, topic: str, message_count: int) -> WorkerStats:
        """Kafka Producer 워커"""
        stats = WorkerStats(
            worker_id=f"producer_{worker_id}",
            worker_type="kafka_producer"
        )
        
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=1000,
                compression_type='gzip',  # 압축 활성화
                batch_size=16384,        # 배치 크기 최적화
                linger_ms=10             # 배치 대기 시간
            )
            
            start_time = time.time()
            
            for i in range(message_count):
                if not self.is_running:
                    break
                    
                try:
                    message = self.generate_stock_message(worker_id, i)
                    
                    # 메시지 전송
                    future = producer.send(topic, message)
                    
                    # 논블로킹으로 결과 확인 (선택적)
                    if i % 100 == 0:  # 100개마다 확인
                        future.get(timeout=5)
                    
                    stats.success_count += 1
                    
                    # 처리량 조절 (CPU 부하 방지)
                    if i % 50 == 0:
                        time.sleep(0.01)
                        
                except KafkaError as e:
                    stats.error_count += 1
                    stats.errors.append(f"Kafka error: {str(e)}")
                    logger.error(f"Producer {worker_id} Kafka error: {e}")
                    
                except Exception as e:
                    stats.error_count += 1
                    stats.errors.append(f"General error: {str(e)}")
                    logger.error(f"Producer {worker_id} error: {e}")
            
            producer.flush(timeout=30)  # 모든 메시지 전송 완료 대기
            producer.close()
            
            end_time = time.time()
            stats.duration = end_time - start_time
            stats.total_messages = message_count
            stats.throughput = stats.success_count / stats.duration if stats.duration > 0 else 0
            
            logger.info(f"✅ Producer {worker_id} 완료: {stats.success_count}/{message_count}, {stats.throughput:.2f} msg/sec")
            
        except Exception as e:
            stats.error_count = message_count
            stats.errors.append(f"Worker initialization error: {str(e)}")
            logger.error(f"❌ Producer {worker_id} 초기화 실패: {e}")
        
        return stats
    
    def kafka_consumer_worker(self, worker_id: str, topic: str, timeout_seconds: int) -> WorkerStats:
        """Kafka Consumer 워커"""
        stats = WorkerStats(
            worker_id=f"consumer_{worker_id}",
            worker_type="kafka_consumer"
        )
        
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['localhost:9092'],
                group_id=f'load_test_consumer_group_{worker_id}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=1000  # 1초 타임아웃
            )
            
            start_time = time.time()
            end_time = start_time + timeout_seconds
            
            while time.time() < end_time and self.is_running:
                try:
                    message_batch = consumer.poll(timeout_ms=500)
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            stats.success_count += 1
                            
                            # 메시지 처리 시뮬레이션 (실제로는 신호 감지 등)
                            if 'signal' in message.value and message.value['signal']:
                                logger.debug(f"Consumer {worker_id} 신호 감지: {message.value['signal']}")
                            
                            # 처리 시간 시뮬레이션
                            time.sleep(0.001)  # 1ms 처리 시간
                    
                except Exception as e:
                    stats.error_count += 1
                    stats.errors.append(f"Consumption error: {str(e)}")
                    logger.warning(f"Consumer {worker_id} error: {e}")
            
            consumer.close()
            
            stats.duration = time.time() - start_time
            stats.total_messages = stats.success_count + stats.error_count
            stats.throughput = stats.success_count / stats.duration if stats.duration > 0 else 0
            
            logger.info(f"✅ Consumer {worker_id} 완료: {stats.success_count} consumed, {stats.throughput:.2f} msg/sec")
            
        except Exception as e:
            stats.errors.append(f"Consumer initialization error: {str(e)}")
            logger.error(f"❌ Consumer {worker_id} 초기화 실패: {e}")
        
        return stats
    
    def api_call_worker(self, worker_id: str, endpoints: List[str], calls_per_endpoint: int) -> WorkerStats:
        """API 호출 워커 (BulkDataCollector 스타일)"""
        stats = WorkerStats(
            worker_id=f"api_{worker_id}",
            worker_type="api_caller"
        )
        
        session = requests.Session()
        session.timeout = 5
        
        start_time = time.time()
        
        try:
            for endpoint in endpoints:
                for i in range(calls_per_endpoint):
                    if not self.is_running:
                        break
                        
                    try:
                        # API 호출 (실제로는 yfinance, KIS API 등)
                        response = session.get(
                            endpoint,
                            timeout=5,
                            params={
                                'test': True,
                                'worker_id': worker_id,
                                'call_id': i
                            }
                        )
                        
                        if response.status_code == 200:
                            stats.success_count += 1
                        else:
                            stats.error_count += 1
                            stats.errors.append(f"HTTP {response.status_code}: {endpoint}")
                            
                    except requests.RequestException as e:
                        stats.error_count += 1
                        stats.errors.append(f"Request error: {str(e)}")
                        logger.warning(f"API Worker {worker_id} error: {e}")
                    
                    # API 호출 간격 (실제로는 rate limiting)
                    time.sleep(0.1)
        
        except Exception as e:
            stats.errors.append(f"API worker error: {str(e)}")
            logger.error(f"❌ API Worker {worker_id} 전체 실패: {e}")
        
        finally:
            session.close()
        
        stats.duration = time.time() - start_time
        stats.total_messages = stats.success_count + stats.error_count
        stats.throughput = stats.success_count / stats.duration if stats.duration > 0 else 0
        
        logger.info(f"✅ API Worker {worker_id} 완료: {stats.success_count}/{stats.total_messages}, {stats.throughput:.2f} calls/sec")
        
        return stats
    
    def run_load_test(self) -> Dict:
        """전체 부하테스트 실행"""
        logger.info("🚀 Kafka + API 부하테스트 시작!")
        logger.info(f"⚙️ 설정: {self.config}")
        
        self.is_running = True
        self.start_time = time.time()
        
        # 시스템 리소스 모니터링 시작
        system_stats = self._start_system_monitoring()
        
        # 워커 스레드 실행
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = []
            
            # 1. Kafka Producer 워커들
            for i in range(self.config.kafka_producer_workers):
                topic = self.config.topics[i % len(self.config.topics)]  # 토픽 순환
                future = executor.submit(
                    self.kafka_producer_worker,
                    f"prod_{i}",
                    topic,
                    self.config.messages_per_worker
                )
                futures.append(future)
            
            # 2. Kafka Consumer 워커들
            test_duration = self.config.duration_minutes * 60
            for i in range(self.config.kafka_consumer_workers):
                topic = self.config.topics[i % len(self.config.topics)]
                future = executor.submit(
                    self.kafka_consumer_worker,
                    f"cons_{i}",
                    topic,
                    test_duration
                )
                futures.append(future)
            
            # 3. API 호출 워커들
            calls_per_worker = 100  # 워커당 API 호출 수
            for i in range(self.config.api_call_workers):
                future = executor.submit(
                    self.api_call_worker,
                    f"api_{i}",
                    self.config.api_endpoints,
                    calls_per_worker
                )
                futures.append(future)
            
            # 지정된 시간만큼 실행
            logger.info(f"⏱️ {self.config.duration_minutes}분간 실행 중...")
            time.sleep(self.config.duration_minutes * 60)
            
            # 테스트 종료
            self.is_running = False
            logger.info("🛑 테스트 종료 신호 전송...")
            
            # 모든 워커 완료 대기
            for future in concurrent.futures.as_completed(futures, timeout=120):
                try:
                    result = future.result()
                    self.results.append(result)
                except Exception as e:
                    logger.error(f"워커 실행 오류: {e}")
        
        # 시스템 모니터링 종료
        final_system_stats = self._stop_system_monitoring(system_stats)
        
        # 결과 집계
        summary = self._generate_test_summary(final_system_stats)
        
        logger.info("✅ 부하테스트 완료!")
        logger.info(f"📊 결과 요약: {summary['total_messages']} 메시지, 성공률 {summary['success_rate']:.1f}%")
        
        return summary
    
    def _start_system_monitoring(self) -> Dict:
        """시스템 리소스 모니터링 시작"""
        return {
            'start_cpu_percent': psutil.cpu_percent(),
            'start_memory': psutil.virtual_memory(),
            'start_time': time.time()
        }
    
    def _stop_system_monitoring(self, start_stats: Dict) -> Dict:
        """시스템 리소스 모니터링 종료"""
        end_stats = {
            'end_cpu_percent': psutil.cpu_percent(),
            'end_memory': psutil.virtual_memory(),
            'end_time': time.time()
        }
        
        return {**start_stats, **end_stats}
    
    def _generate_test_summary(self, system_stats: Dict) -> Dict:
        """테스트 결과 요약 생성"""
        total_success = sum(r.success_count for r in self.results)
        total_errors = sum(r.error_count for r in self.results)
        total_messages = total_success + total_errors
        
        # 워커 타입별 집계
        producer_results = [r for r in self.results if r.worker_type == 'kafka_producer']
        consumer_results = [r for r in self.results if r.worker_type == 'kafka_consumer']
        api_results = [r for r in self.results if r.worker_type == 'api_caller']
        
        summary = {
            'test_config': self.config,
            'total_duration': time.time() - self.start_time,
            'total_messages': total_messages,
            'total_success': total_success,
            'total_errors': total_errors,
            'success_rate': (total_success / total_messages * 100) if total_messages > 0 else 0,
            'overall_throughput': total_success / (time.time() - self.start_time),
            
            # 워커별 통계
            'producer_stats': {
                'worker_count': len(producer_results),
                'avg_throughput': sum(r.throughput for r in producer_results) / len(producer_results) if producer_results else 0,
                'total_sent': sum(r.success_count for r in producer_results)
            },
            'consumer_stats': {
                'worker_count': len(consumer_results),
                'avg_throughput': sum(r.throughput for r in consumer_results) / len(consumer_results) if consumer_results else 0,
                'total_consumed': sum(r.success_count for r in consumer_results)
            },
            'api_stats': {
                'worker_count': len(api_results),
                'avg_throughput': sum(r.throughput for r in api_results) / len(api_results) if api_results else 0,
                'total_calls': sum(r.success_count for r in api_results)
            },
            
            # 시스템 리소스
            'system_resources': {
                'avg_cpu_usage': (system_stats['start_cpu_percent'] + system_stats['end_cpu_percent']) / 2,
                'memory_used_gb': system_stats['end_memory'].used / (1024**3),
                'memory_percent': system_stats['end_memory'].percent
            },
            
            # 개별 워커 결과
            'worker_results': [
                {
                    'worker_id': r.worker_id,
                    'worker_type': r.worker_type,
                    'success_count': r.success_count,
                    'error_count': r.error_count,
                    'throughput': r.throughput,
                    'duration': r.duration,
                    'error_sample': r.errors[:3] if r.errors else []  # 첫 3개 오류만
                }
                for r in self.results
            ]
        }
        
        return summary

def main():
    """메인 함수 - CLI 테스트용"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka + API 부하테스트")
    parser.add_argument("--duration", type=int, default=5, help="테스트 지속시간 (분)")
    parser.add_argument("--kafka-producers", type=int, default=10, help="Kafka Producer 워커 수")
    parser.add_argument("--kafka-consumers", type=int, default=5, help="Kafka Consumer 워커 수")
    parser.add_argument("--api-workers", type=int, default=4, help="API 호출 워커 수")
    parser.add_argument("--messages-per-worker", type=int, default=1000, help="워커당 메시지 수")
    parser.add_argument("--topics", nargs='+', default=["realtime-stock", "yfinance-stock-data"], help="테스트 토픽 목록")
    
    args = parser.parse_args()
    
    # 설정 생성
    config = LoadTestConfig(
        duration_minutes=args.duration,
        kafka_producer_workers=args.kafka_producers,
        kafka_consumer_workers=args.kafka_consumers,
        api_call_workers=args.api_workers,
        messages_per_worker=args.messages_per_worker,
        topics=args.topics
    )
    
    # 테스트 실행
    tester = KafkaLoadTester(config)
    results = tester.run_load_test()
    
    # 결과 출력
    print("\n" + "="*60)
    print("📊 KAFKA + API 부하테스트 결과")
    print("="*60)
    print(f"전체 메시지: {results['total_messages']:,}")
    print(f"성공: {results['total_success']:,}")
    print(f"실패: {results['total_errors']:,}")
    print(f"성공률: {results['success_rate']:.1f}%")
    print(f"전체 처리량: {results['overall_throughput']:.2f} msg/sec")
    print(f"테스트 시간: {results['total_duration']:.2f}초")
    print(f"CPU 사용률: {results['system_resources']['avg_cpu_usage']:.1f}%")
    print(f"메모리 사용률: {results['system_resources']['memory_percent']:.1f}%")
    
    # 워커별 통계
    print(f"\n📤 Producer: {results['producer_stats']['total_sent']:,} sent, {results['producer_stats']['avg_throughput']:.2f} avg msg/sec")
    print(f"📥 Consumer: {results['consumer_stats']['total_consumed']:,} consumed, {results['consumer_stats']['avg_throughput']:.2f} avg msg/sec")
    print(f"🌐 API: {results['api_stats']['total_calls']:,} calls, {results['api_stats']['avg_throughput']:.2f} avg calls/sec")

if __name__ == "__main__":
    main()
