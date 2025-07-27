#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
성능 모니터링 및 메트릭 수집 도구
"""

import psutil
import time
import logging
import json
import threading
from datetime import datetime
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import sys
import os

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Prometheus 메트릭 정의
REQUESTS_TOTAL = Counter('stock_requests_total', 'Total requests', ['method', 'endpoint', 'status'])
REQUEST_DURATION = Histogram('stock_request_duration_seconds', 'Request duration')
ACTIVE_CONNECTIONS = Gauge('stock_active_connections', 'Active connections')
MEMORY_USAGE = Gauge('stock_memory_usage_bytes', 'Memory usage')
CPU_USAGE = Gauge('stock_cpu_usage_percent', 'CPU usage')
DISK_USAGE = Gauge('stock_disk_usage_percent', 'Disk usage')
KAFKA_MESSAGES = Counter('kafka_messages_total', 'Total Kafka messages', ['topic', 'status'])
DATABASE_OPERATIONS = Counter('database_operations_total', 'Database operations', ['operation', 'status'])
API_ERRORS = Counter('api_errors_total', 'API errors', ['service', 'error_type'])

class PerformanceMonitor:
    """성능 모니터링 클래스"""
    
    def __init__(self, log_file='performance.log', metrics_port=8000):
        self.log_file = log_file
        self.metrics_port = metrics_port
        self.monitoring = False
        self.setup_logging()
        
        # 성능 통계
        self.stats = {
            'requests_count': 0,
            'errors_count': 0,
            'total_response_time': 0,
            'max_memory_usage': 0,
            'max_cpu_usage': 0,
            'start_time': None
        }
        
    def setup_logging(self):
        """로깅 설정"""
        self.logger = logging.getLogger(__name__)
        
        # 파일 핸들러
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        
        # 포맷터
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
    
    def start_prometheus_server(self):
        """Prometheus 메트릭 서버 시작"""
        try:
            start_http_server(self.metrics_port)
            self.logger.info(f"🚀 Prometheus 메트릭 서버 시작: http://localhost:{self.metrics_port}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Prometheus 서버 시작 실패: {e}")
            return False
    
    def start_monitoring(self, interval=5):
        """시스템 모니터링 시작"""
        self.monitoring = True
        self.stats['start_time'] = datetime.now()
        
        def monitor_loop():
            while self.monitoring:
                try:
                    # 시스템 메트릭 수집
                    cpu_percent = psutil.cpu_percent(interval=1)
                    memory = psutil.virtual_memory()
                    disk = psutil.disk_usage('/')
                    
                    # 네트워크 연결 수
                    connections = len(psutil.net_connections())
                    
                    # Prometheus 메트릭 업데이트
                    CPU_USAGE.set(cpu_percent)
                    MEMORY_USAGE.set(memory.used)
                    DISK_USAGE.set((disk.used / disk.total) * 100)
                    ACTIVE_CONNECTIONS.set(connections)
                    
                    # 최대값 추적
                    self.stats['max_cpu_usage'] = max(self.stats['max_cpu_usage'], cpu_percent)
                    self.stats['max_memory_usage'] = max(self.stats['max_memory_usage'], memory.used)
                    
                    # 상세 메트릭 로깅
                    metrics = {
                        'timestamp': datetime.now().isoformat(),
                        'system': {
                            'cpu_percent': round(cpu_percent, 2),
                            'memory_used_gb': round(memory.used / 1024 / 1024 / 1024, 2),
                            'memory_percent': round(memory.percent, 2),
                            'disk_used_gb': round(disk.used / 1024 / 1024 / 1024, 2),
                            'disk_percent': round((disk.used / disk.total) * 100, 2),
                            'active_connections': connections
                        },
                        'application': {
                            'total_requests': self.stats['requests_count'],
                            'total_errors': self.stats['errors_count'],
                            'error_rate': round((self.stats['errors_count'] / max(self.stats['requests_count'], 1)) * 100, 2),
                            'avg_response_time': round(self.stats['total_response_time'] / max(self.stats['requests_count'], 1), 3) if self.stats['requests_count'] > 0 else 0
                        }
                    }
                    
                    # 성능 임계값 경고
                    if cpu_percent > 80:
                        self.logger.warning(f"⚠️ 높은 CPU 사용률: {cpu_percent}%")
                    
                    if memory.percent > 85:
                        self.logger.warning(f"⚠️ 높은 메모리 사용률: {memory.percent}%")
                    
                    if (disk.used / disk.total) * 100 > 90:
                        self.logger.warning(f"⚠️ 높은 디스크 사용률: {(disk.used / disk.total) * 100:.1f}%")
                    
                    # 5분마다 상세 로그
                    if int(time.time()) % 300 == 0:
                        self.logger.info(f"📊 상세 시스템 메트릭:\n{json.dumps(metrics, indent=2, ensure_ascii=False)}")
                    
                    time.sleep(interval)
                    
                except Exception as e:
                    self.logger.error(f"❌ 모니터링 오류: {e}")
                    time.sleep(interval)
        
        # 백그라운드에서 모니터링 실행
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        self.logger.info("🔍 성능 모니터링 시작")
    
    def stop_monitoring(self):
        """모니터링 중지"""
        self.monitoring = False
        
        # 최종 통계 출력
        end_time = datetime.now()
        duration = (end_time - self.stats['start_time']).total_seconds()
        
        final_stats = {
            'monitoring_duration_seconds': round(duration, 2),
            'total_requests': self.stats['requests_count'],
            'total_errors': self.stats['errors_count'],
            'error_rate_percent': round((self.stats['errors_count'] / max(self.stats['requests_count'], 1)) * 100, 2),
            'requests_per_second': round(self.stats['requests_count'] / duration, 2) if duration > 0 else 0,
            'max_cpu_usage_percent': round(self.stats['max_cpu_usage'], 2),
            'max_memory_usage_gb': round(self.stats['max_memory_usage'] / 1024 / 1024 / 1024, 2)
        }
        
        self.logger.info(f"⏹️ 성능 모니터링 중지")
        self.logger.info(f"📊 최종 통계:\n{json.dumps(final_stats, indent=2, ensure_ascii=False)}")
    
    def log_request(self, method, endpoint, status_code, duration, error=None):
        """요청 메트릭 로깅"""
        self.stats['requests_count'] += 1
        self.stats['total_response_time'] += duration
        
        if status_code >= 400 or error:
            self.stats['errors_count'] += 1
            API_ERRORS.labels(service=endpoint, error_type=str(status_code)).inc()
        
        # Prometheus 메트릭 업데이트
        REQUESTS_TOTAL.labels(method=method, endpoint=endpoint, status=status_code).inc()
        REQUEST_DURATION.observe(duration)
        
        # 느린 요청 경고 (5초 이상)
        if duration > 5.0:
            self.logger.warning(f"🐌 느린 요청: {method} {endpoint} - {duration:.2f}초")
        
        self.logger.info(f"🌐 요청: {method} {endpoint} - {status_code} ({duration:.3f}초)")
    
    def log_kafka_message(self, topic, status='success', error=None):
        """Kafka 메시지 메트릭 로깅"""
        KAFKA_MESSAGES.labels(topic=topic, status=status).inc()
        
        if error:
            self.logger.error(f"❌ Kafka 오류 ({topic}): {error}")
        else:
            self.logger.debug(f"📤 Kafka 메시지: {topic} - {status}")
    
    def log_database_operation(self, operation, status='success', duration=None, error=None):
        """데이터베이스 작업 메트릭 로깅"""
        DATABASE_OPERATIONS.labels(operation=operation, status=status).inc()
        
        if error:
            self.logger.error(f"❌ DB 오류 ({operation}): {error}")
        else:
            duration_str = f" ({duration:.3f}초)" if duration else ""
            self.logger.info(f"🗃️ DB 작업: {operation} - {status}{duration_str}")
    
    def get_health_status(self):
        """시스템 상태 체크"""
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            health_status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'system': {
                    'cpu_usage': cpu_percent,
                    'memory_usage': memory.percent,
                    'disk_usage': (disk.used / disk.total) * 100,
                    'uptime_seconds': time.time() - psutil.boot_time()
                },
                'application': {
                    'total_requests': self.stats['requests_count'],
                    'error_rate': (self.stats['errors_count'] / max(self.stats['requests_count'], 1)) * 100
                }
            }
            
            # 상태 판정
            if cpu_percent > 90 or memory.percent > 95 or (disk.used / disk.total) * 100 > 95:
                health_status['status'] = 'critical'
            elif cpu_percent > 70 or memory.percent > 80 or (disk.used / disk.total) * 100 > 85:
                health_status['status'] = 'warning'
            
            return health_status
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

# 전역 모니터 인스턴스
monitor = PerformanceMonitor()

# Circuit Breaker 패턴 구현
class CircuitBreaker:
    """Circuit Breaker 패턴으로 장애 전파 방지"""
    
    def __init__(self, failure_threshold=5, timeout=60, name="CircuitBreaker"):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.name = name
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.logger = logging.getLogger(f"CircuitBreaker.{name}")
    
    def call(self, func, *args, **kwargs):
        """Circuit Breaker를 통한 함수 호출"""
        
        # OPEN 상태 - 실행 차단
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.timeout:
                self.state = 'HALF_OPEN'
                self.logger.info(f"🔄 {self.name} Circuit Breaker: OPEN → HALF_OPEN")
            else:
                raise Exception(f"Circuit breaker is OPEN for {self.name}")
        
        try:
            # 함수 실행
            result = func(*args, **kwargs)
            
            # 성공 시 상태 리셋
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.logger.info(f"✅ {self.name} Circuit Breaker: HALF_OPEN → CLOSED")
            
            self.failure_count = 0
            return result
            
        except Exception as e:
            # 실패 시 카운트 증가
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                self.logger.error(f"🚨 {self.name} Circuit Breaker: CLOSED → OPEN (실패 {self.failure_count}회)")
            
            monitor.log_request("CIRCUIT_BREAKER", self.name, 500, 0, error=str(e))
            raise e
    
    def get_state(self):
        """현재 상태 반환"""
        return {
            'state': self.state,
            'failure_count': self.failure_count,
            'last_failure_time': self.last_failure_time,
            'name': self.name
        }

# 재시도 데코레이터
def retry_with_backoff(max_retries=3, base_delay=1, max_delay=60, backoff_factor=2.0):
    """지수 백오프 재시도 데코레이터"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            delay = base_delay
            
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    
                    # 재시도 후 성공
                    if attempt > 0:
                        monitor.log_request("RETRY_SUCCESS", func.__name__, 200, 0)
                        logging.getLogger(func.__name__).info(f"✅ 재시도 성공 (시도 {attempt + 1}회)")
                    
                    return result
                    
                except Exception as e:
                    if attempt < max_retries:
                        logging.getLogger(func.__name__).warning(f"⚠️ 재시도 {attempt + 1}/{max_retries}: {str(e)}")
                        monitor.log_request("RETRY_ATTEMPT", func.__name__, 500, delay, error=str(e))
                        time.sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
                    else:
                        logging.getLogger(func.__name__).error(f"❌ 최대 재시도 초과: {str(e)}")
                        monitor.log_request("RETRY_FAILED", func.__name__, 500, 0, error=str(e))
                        raise e
        
        return wrapper
    return decorator

if __name__ == "__main__":
    # 테스트 코드
    print("🔧 성능 모니터링 시스템 테스트")
    
    # 모니터링 시작
    monitor.start_prometheus_server()
    monitor.start_monitoring(interval=2)
    
    # 테스트 요청들
    for i in range(10):
        monitor.log_request("GET", f"/api/test/{i}", 200, 0.1 + i * 0.01)
        monitor.log_kafka_message("test-topic", "success")
        monitor.log_database_operation("SELECT", "success", 0.05)
        time.sleep(1)
    
    # 상태 체크
    health = monitor.get_health_status()
    print(f"시스템 상태: {health}")
    
    # 모니터링 중지
    time.sleep(5)
    monitor.stop_monitoring()
