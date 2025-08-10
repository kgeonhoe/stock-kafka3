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
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging
import concurrent.futures
import psutil
import requests
from threading import Lock
from collections import defaultdict

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
    bootstrap_servers: List[str] = None  # Kafka 브로커 리스트 (없으면 자동 결정)
    
    def __post_init__(self):
        if self.topics is None:
            self.topics = ["realtime-stock", "yfinance-stock-data"]
        if self.api_endpoints is None:
            import os, socket
            # 환경변수로 커스텀 (쉼표 분리)
            env_api = os.getenv("LOAD_TEST_API_ENDPOINTS")
            if env_api:
                self.api_endpoints = [e.strip() for e in env_api.split(',') if e.strip()]
            else:
                # 컨테이너 내부 여부 감지 (/.dockerenv 존재 등)
                in_container = os.path.exists('/.dockerenv') or os.getenv('KUBERNETES_SERVICE_HOST') is not None
                # 기본: Airflow webserver 헬스체크 (내부 호스트명), 필요시 사용자 서비스 추가
                if in_container:
                    self.api_endpoints = [
                        "http://airflow-webserver:8080/health"  # Airflow 내부 health
                    ]
                else:
                    # 로컬 호스트에서 직접 실행 시 포트 매핑 기준(호스트 8081 -> 컨테이너 8080)
                    self.api_endpoints = [
                        "http://localhost:8081/health"
                    ]
        # 잘못된 Redis HTTP endpoint 자동 제거 (:6379 HTTP 시도)
        cleaned = []
        for ep in self.api_endpoints:
            if ep.startswith(('http://', 'https://')) and ':6379' in ep:
                try:
                    logger.warning(f"제거: Redis 포트 6379에 대한 HTTP endpoint는 유효하지 않음 -> {ep}")
                except Exception:
                    pass
                continue
            cleaned.append(ep)
        self.api_endpoints = cleaned
    # NOTE: 필요 시 redis:// 형태를 별도 Redis ping 워커로 지원 가능
        if self.bootstrap_servers is None:
            # 환경변수(KAFKA_BOOTSTRAP_SERVERS) 우선 사용 (쉼표 구분)
            import os
            env_val = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
            if env_val:
                self.bootstrap_servers = [s.strip() for s in env_val.split(',') if s.strip()]
            else:
                self.bootstrap_servers = ["localhost:9092"]

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

@dataclass
class SLOCriteria:
    """SLO 기준 (운영 영향 판단용)"""
    min_success_rate: float = 99.0          # 최소 성공률 (%)
    max_error_count: int = 0                # 허용 총 에러 수 (0이면 오류 없는지)
    max_avg_cpu_usage: float = 70.0         # 평균 CPU 사용률 상한 (%)
    max_consumer_producer_gap_pct: float = 5.0  # consumer/producer 누적 차이 허용 편차 (%)
    min_overall_throughput: float = 0.0     # 최소 전체 처리량 (msg/sec)


class KafkaLoadTester:
    """Kafka 부하테스트 클래스"""

    def __init__(self, config: LoadTestConfig, slo_criteria: Optional[SLOCriteria] = None):
        """초기화"""
        self.config = config
        self.slo_criteria = slo_criteria  # 없으면 평가 생략
        # 실행 상태
        self.results: List[WorkerStats] = []
        self.is_running: bool = False
        self.start_time: Optional[float] = None
        self.run_id: Optional[str] = None  # 각 테스트 고유 ID
        # 동시성 & 카운터
        self._lock = Lock()
        self._producer_sent = 0
        self._consumer_received = 0
        self._api_success = 0
        self._skipped_consumed = 0  # run_id 불일치로 무시된 소비
        # 스냅샷 (실시간 모니터링)
        self.metric_snapshots: List[Dict] = []
        self._last_snapshot_time = 0.0
        self.snapshot_interval_sec = 1.0
        self.recent_errors: List[str] = []
        # 엔드포인트별 성공/실패
        self.endpoint_success = defaultdict(int)
        self.endpoint_error = defaultdict(int)
        # Latency 기록 (consumer 매칭 메시지)
        self._latencies: List[float] = []
        self._latency_sample_limit = 200000  # 메모리 보호

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
            'run_id': self.run_id,
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
                bootstrap_servers=self.config.bootstrap_servers,
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
                    with self._lock:
                        self._producer_sent += 1
                    self._maybe_snapshot()
                    
                    # 처리량 조절 (CPU 부하 방지)
                    if i % 50 == 0:
                        time.sleep(0.01)
                        
                except KafkaError as e:
                    stats.error_count += 1
                    stats.errors.append(f"Kafka error: {str(e)}")
                    logger.error(f"Producer {worker_id} Kafka error: {e}")
                    with self._lock:
                        self.recent_errors.append(f"producer:{worker_id}:{e}")
                        self.recent_errors = self.recent_errors[-50:]
                    self._maybe_snapshot(force=True)
                    
                except Exception as e:
                    stats.error_count += 1
                    stats.errors.append(f"General error: {str(e)}")
                    logger.error(f"Producer {worker_id} error: {e}")
                    with self._lock:
                        self.recent_errors.append(f"producer:{worker_id}:{e}")
                        self.recent_errors = self.recent_errors[-50:]
                    self._maybe_snapshot(force=True)
            
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
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=f'load_test_consumer_group_{int(self.start_time or time.time())}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',  # run_id 기반 필터링 시 backlog 제외
                enable_auto_commit=True,
                consumer_timeout_ms=1000  # 1초 타임아웃
            )
            
            start_time = time.time()
            end_time = start_time + timeout_seconds
            
            empty_polls = 0
            while time.time() < end_time and self.is_running:
                try:
                    message_batch = consumer.poll(timeout_ms=300)
                    if not message_batch:
                        empty_polls += 1
                        if empty_polls % 10 == 0:
                            logger.debug(f"Consumer {worker_id} 아직 수신 없음 (empty polls={empty_polls})")
                        # 주기적으로 스냅샷
                        if empty_polls % 5 == 0:
                            self._maybe_snapshot()
                        continue
                    empty_polls = 0
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            val = message.value
                            # run_id 필터: 현재 테스트 메시지 아닌 경우 skip
                            if val.get('run_id') != self.run_id:
                                with self._lock:
                                    self._skipped_consumed += 1
                                continue
                            stats.success_count += 1
                            recv_time = time.time()
                            with self._lock:
                                self._consumer_received += 1
                                if 'timestamp' in val and len(self._latencies) < self._latency_sample_limit:
                                    self._latencies.append(recv_time - float(val['timestamp']))
                            if 'signal' in message.value and message.value['signal']:
                                logger.debug(f"Consumer {worker_id} 신호 감지: {message.value['signal']}")
                            # 처리 시간 시뮬레이션
                            # time.sleep(0.001)  # 필요시 지연
                            self._maybe_snapshot()
                except Exception as e:
                    stats.error_count += 1
                    stats.errors.append(f"Consumption error: {str(e)}")
                    logger.warning(f"Consumer {worker_id} error: {e}")
                    with self._lock:
                        self.recent_errors.append(f"consumer:{worker_id}:{e}")
                        self.recent_errors = self.recent_errors[-50:]
                    self._maybe_snapshot(force=True)
            
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
                        # Connection refused 발생 시:
                        # 1) 해당 endpoint 서비스가 실행되지 않았거나
                        # 2) 컨테이너 내부에서 'localhost' 로 접근했지만 실제 서비스는 다른 컨테이너(hostname)에서 열려있거나
                        # 3) 포트 매핑이 호스트->컨테이너 다르게 설정된 경우
                        # docker-compose 내부에서는 http://<service-name>:<port>/ 형태 사용 권장.
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
                            with self._lock:
                                self._api_success += 1
                                try:
                                    self.endpoint_success[endpoint] += 1
                                except Exception:
                                    pass
                            self._maybe_snapshot()
                        else:
                            stats.error_count += 1
                            stats.errors.append(f"HTTP {response.status_code}: {endpoint}")
                            with self._lock:
                                try:
                                    self.endpoint_error[endpoint] += 1
                                except Exception:
                                    pass
                            
                    except requests.RequestException as e:
                        stats.error_count += 1
                        stats.errors.append(f"Request error: {str(e)}")
                        logger.warning(f"API Worker {worker_id} error: {e}")
                        with self._lock:
                            self.recent_errors.append(f"api:{worker_id}:{e}")
                            self.recent_errors = self.recent_errors[-50:]
                            try:
                                self.endpoint_error[endpoint] += 1
                            except Exception:
                                pass
                        self._maybe_snapshot(force=True)
                    
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
        # run_id 발급
        self.run_id = f"{int(self.start_time)}_{uuid.uuid4().hex[:8]}"
        logger.info(f"🆔 run_id={self.run_id}")
        # 초기 스냅샷
        self._maybe_snapshot(force=True)
        # 시스템 리소스 모니터링 시작
        system_stats = self._start_system_monitoring()

        # 워커 스레드 실행
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = []

            # 1. Kafka Consumer 워커들 (먼저 시작)
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

            time.sleep(0.5)

            # 2. Kafka Producer 워커들
            for i in range(self.config.kafka_producer_workers):
                topic = self.config.topics[i % len(self.config.topics)]
                future = executor.submit(
                    self.kafka_producer_worker,
                    f"prod_{i}",
                    topic,
                    self.config.messages_per_worker
                )
                futures.append(future)

            # 3. API 호출 워커들
            calls_per_worker = 100
            for i in range(self.config.api_call_workers):
                future = executor.submit(
                    self.api_call_worker,
                    f"api_{i}",
                    self.config.api_endpoints,
                    calls_per_worker
                )
                futures.append(future)

            logger.info(f"⏱️ {self.config.duration_minutes}분간 실행 중 (중단 가능)...")
            end_time = time.time() + self.config.duration_minutes * 60
            while self.is_running and time.time() < end_time:
                time.sleep(0.5)
                self._maybe_snapshot()
            self.is_running = False
            logger.info("🛑 테스트 종료 신호 전송...")

            for future in concurrent.futures.as_completed(futures, timeout=120):
                try:
                    result = future.result()
                    self.results.append(result)
                except Exception as e:
                    logger.error(f"워커 실행 오류: {e}")

        final_system_stats = self._stop_system_monitoring(system_stats)
        summary = self._generate_test_summary(final_system_stats)
        logger.info("✅ 부하테스트 완료!")
        logger.info(f"📊 결과 요약: {summary['total_messages']} 메시지, 성공률 {summary['success_rate']:.1f}%")
        return summary

    # -------- 실시간 모니터링 관련 유틸 ---------
    def _maybe_snapshot(self, force: bool = False):
        """스냅샷 주기 조건 충족 시 기록 (force=True 시 즉시)"""
        now = time.time()
        if force or (now - self._last_snapshot_time >= self.snapshot_interval_sec):
            self._last_snapshot_time = now
            with self._lock:
                elapsed = now - (self.start_time or now)
                snapshot = {
                    'timestamp': now,
                    'elapsed_sec': elapsed,
                    'producer_sent': self._producer_sent,
                    'consumer_received': self._consumer_received,
                    'api_success': self._api_success
                }
                self.metric_snapshots.append(snapshot)

    def get_live_metrics(self) -> Dict:
        """현재까지 누적 및 최근 처리량(초당) 계산"""
        with self._lock:
            if len(self.metric_snapshots) >= 2:
                last = self.metric_snapshots[-1]
                prev = self.metric_snapshots[-2]
                interval = last['elapsed_sec'] - prev['elapsed_sec'] or 1
                return {
                    'producer_sent_total': self._producer_sent,
                    'consumer_received_total': self._consumer_received,
                    'api_success_total': self._api_success,
                    'producer_tps': (last['producer_sent'] - prev['producer_sent']) / interval,
                    'consumer_tps': (last['consumer_received'] - prev['consumer_received']) / interval,
                    'api_tps': (last['api_success'] - prev['api_success']) / interval,
                    'snapshots': list(self.metric_snapshots[-300:]),  # 최근 5분(초단위) 제한
                    'recent_errors': list(self.recent_errors[-10:])
                }
            else:
                return {
                    'producer_sent_total': self._producer_sent,
                    'consumer_received_total': self._consumer_received,
                    'api_success_total': self._api_success,
                    'producer_tps': 0.0,
                    'consumer_tps': 0.0,
                    'api_tps': 0.0,
                    'snapshots': list(self.metric_snapshots),
                    'recent_errors': list(self.recent_errors[-10:])
                }
    
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
            'overall_throughput': total_success / (time.time() - self.start_time) if self.start_time else 0,
            'api_endpoint_breakdown': {
                'success': dict(self.endpoint_success),
                'error': dict(self.endpoint_error)
            },
            'run_id': self.run_id,
            'skipped_messages': self._skipped_consumed,
            # 워커별 통계
            'producer_stats': {
                'worker_count': len(producer_results),
                'avg_throughput': (sum(r.throughput for r in producer_results) / len(producer_results)) if producer_results else 0,
                'total_sent': sum(r.success_count for r in producer_results)
            },
            'consumer_stats': {
                'worker_count': len(consumer_results),
                'avg_throughput': (sum(r.throughput for r in consumer_results) / len(consumer_results)) if consumer_results else 0,
                'total_consumed': sum(r.success_count for r in consumer_results)
            },
            'api_stats': {
                'worker_count': len(api_results),
                'avg_throughput': (sum(r.throughput for r in api_results) / len(api_results)) if api_results else 0,
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
                    'error_sample': r.errors[:3] if r.errors else []
                }
                for r in self.results
            ]
        }
        # SLO 평가
        if self.slo_criteria:
            checks = []
            sc = self.slo_criteria
            # 1. 성공률
            checks.append({
                'name': '성공률',
                'passed': summary['success_rate'] >= sc.min_success_rate,
                'actual': round(summary['success_rate'], 2),
                'expected': f">= {sc.min_success_rate}"
            })
            # 2. 오류 수
            checks.append({
                'name': '총 오류 수',
                'passed': summary['total_errors'] <= sc.max_error_count,
                'actual': summary['total_errors'],
                'expected': f"<= {sc.max_error_count}"
            })
            # 3. CPU 사용률
            cpu_actual = summary['system_resources']['avg_cpu_usage']
            checks.append({
                'name': '평균 CPU 사용률',
                'passed': cpu_actual <= sc.max_avg_cpu_usage,
                'actual': round(cpu_actual, 2),
                'expected': f"<= {sc.max_avg_cpu_usage}"
            })
            # 4. Consumer/Producer 편차
            prod_total = summary['producer_stats']['total_sent'] or 0
            cons_total = summary['consumer_stats']['total_consumed'] or 0
            if prod_total > 0:
                gap_pct = abs(cons_total - prod_total) / prod_total * 100
                checks.append({
                    'name': 'Consumer/Producer 누적 편차%',
                    'passed': gap_pct <= sc.max_consumer_producer_gap_pct,
                    'actual': round(gap_pct, 2),
                    'expected': f"<= {sc.max_consumer_producer_gap_pct}"
                })
            else:
                checks.append({
                    'name': 'Consumer/Producer 누적 편차%',
                    'passed': True,
                    'actual': 'N/A',
                    'expected': f"<= {sc.max_consumer_producer_gap_pct} (producer=0)"
                })
            # 5. 전체 처리량
            checks.append({
                'name': '전체 처리량(msg/s)',
                'passed': summary['overall_throughput'] >= sc.min_overall_throughput,
                'actual': round(summary['overall_throughput'], 2),
                'expected': f">= {sc.min_overall_throughput}"
            })
            summary['slo'] = {
                'criteria': sc,
                'checks': checks,
                'passed': all(c['passed'] for c in checks)
            }
        # Latency 통계 추가
        with self._lock:
            lats = list(self._latencies)
        if lats:
            lats_sorted = sorted(lats)
            def pct(p):
                if not lats_sorted:
                    return None
                k = (len(lats_sorted)-1) * p/100
                f = int(k)
                c = min(f+1, len(lats_sorted)-1)
                if f == c:
                    return lats_sorted[f]
                return lats_sorted[f] + (lats_sorted[c]-lats_sorted[f])*(k-f)
            summary['latency_stats'] = {
                'count': len(lats_sorted),
                'avg_ms': (sum(lats_sorted)/len(lats_sorted))*1000,
                'p50_ms': pct(50)*1000,
                'p95_ms': pct(95)*1000,
                'p99_ms': pct(99)*1000,
                'max_ms': max(lats_sorted)*1000
            }
        else:
            summary['latency_stats'] = {
                'count': 0,
                'avg_ms': None,
                'p50_ms': None,
                'p95_ms': None,
                'p99_ms': None,
                'max_ms': None
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
