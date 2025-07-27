#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
주식 API 부하테스트 스크립트 (Locust 기반)
"""

from locust import HttpUser, task, between
import random
import json
import time
import psutil
import logging
from kafka import KafkaProducer
from datetime import datetime
import sys
import os

# 공통 모듈 경로 추가
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
sys.path.insert(0, '/opt/airflow/common')

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockAPILoadTest(HttpUser):
    """주식 API 부하테스트 클래스"""
    
    wait_time = between(1, 3)  # 1-3초 대기
    
    def on_start(self):
        """테스트 시작 시 초기화"""
        self.symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
            'META', 'NVDA', 'NFLX', 'ADBE', 'CRM'
        ]
        
        # Kafka Producer 초기화 (부하테스트용)
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                retry_backoff_ms=1000,
                request_timeout_ms=30000
            )
            logger.info("✅ Kafka Producer 연결 성공")
            self.kafka_available = True
        except Exception as e:
            logger.error(f"❌ Kafka Producer 연결 실패: {e}")
            self.kafka_available = False
    
    def on_stop(self):
        """테스트 종료 시 정리"""
        if hasattr(self, 'producer') and self.producer:
            self.producer.close()
            logger.info("🔚 Kafka Producer 연결 종료")
    
    @task(3)
    def test_yfinance_data_collection(self):
        """yfinance 데이터 수집 부하테스트"""
        symbol = random.choice(self.symbols)
        
        start_time = time.time()
        memory_before = psutil.virtual_memory().used
        
        try:
            # yfinance API 호출 시뮬레이션 (실제 API 엔드포인트가 있다면 사용)
            headers = {
                'User-Agent': 'Stock-Load-Test/1.0',
                'Content-Type': 'application/json'
            }
            
            # 실제 API 엔드포인트로 변경 필요
            # response = self.client.get(f"/api/stock/{symbol}/daily", headers=headers)
            
            # 시뮬레이션용 더미 응답
            processing_time = time.time() - start_time
            memory_after = psutil.virtual_memory().used
            memory_used = (memory_after - memory_before) / 1024 / 1024  # MB
            
            # 성공 시뮬레이션 (90% 성공률)
            if random.random() < 0.9:
                logger.info(f"📊 {symbol} 수집 성공: {processing_time:.2f}초, 메모리: {memory_used:.2f}MB")
                
                # 성공 메트릭 기록
                self.environment.events.request.fire(
                    request_type="GET",
                    name=f"/api/stock/{symbol}/daily",
                    response_time=processing_time * 1000,
                    response_length=1024,
                    exception=None
                )
            else:
                # 실패 시뮬레이션
                logger.error(f"❌ {symbol} 수집 실패: 시뮬레이션 오류")
                self.environment.events.request.fire(
                    request_type="GET",
                    name=f"/api/stock/{symbol}/daily",
                    response_time=processing_time * 1000,
                    response_length=0,
                    exception=Exception("Simulated API failure")
                )
                
        except Exception as e:
            logger.error(f"❌ API 호출 오류 ({symbol}): {e}")
            self.environment.events.request.fire(
                request_type="GET",
                name=f"/api/stock/{symbol}/daily",
                response_time=0,
                response_length=0,
                exception=e
            )
    
    @task(2)
    def test_kafka_message_production(self):
        """Kafka 메시지 전송 부하테스트"""
        if not self.kafka_available:
            return
            
        symbol = random.choice(self.symbols)
        
        test_data = {
            'symbol': symbol,
            'price': round(random.uniform(100, 500), 2),
            'volume': random.randint(1000, 100000),
            'timestamp': time.time(),
            'source': 'load_test',
            'test_id': f"test_{int(time.time())}"
        }
        
        try:
            start_time = time.time()
            
            # Kafka 메시지 전송
            future = self.producer.send('yfinance-stock-data', test_data)
            
            # 전송 완료 대기 (타임아웃 5초)
            record_metadata = future.get(timeout=5)
            
            processing_time = time.time() - start_time
            logger.info(f"📤 Kafka 메시지 전송 성공 ({symbol}): {processing_time:.3f}초, 파티션: {record_metadata.partition}")
            
            # 성공 메트릭 기록
            self.environment.events.request.fire(
                request_type="KAFKA",
                name=f"kafka_send_{symbol}",
                response_time=processing_time * 1000,
                response_length=len(json.dumps(test_data)),
                exception=None
            )
            
        except Exception as e:
            logger.error(f"❌ Kafka 전송 실패 ({symbol}): {e}")
            self.environment.events.request.fire(
                request_type="KAFKA",
                name=f"kafka_send_{symbol}",
                response_time=0,
                response_length=0,
                exception=e
            )
    
    @task(1)
    def test_database_heavy_operation(self):
        """데이터베이스 집약적 작업 부하테스트"""
        symbol = random.choice(self.symbols)
        
        try:
            start_time = time.time()
            
            # 무거운 데이터베이스 작업 시뮬레이션
            # 실제로는 DuckDB 복잡한 쿼리를 실행
            time.sleep(random.uniform(0.1, 0.5))  # 0.1-0.5초 시뮬레이션
            
            processing_time = time.time() - start_time
            
            # 성공 확률 85%
            if random.random() < 0.85:
                data_size = random.randint(1000, 50000)  # 1KB-50KB
                logger.info(f"🗃️ DB 집약적 작업 성공 ({symbol}): {processing_time:.2f}초, 데이터: {data_size}bytes")
                
                self.environment.events.request.fire(
                    request_type="DB",
                    name=f"heavy_query_{symbol}",
                    response_time=processing_time * 1000,
                    response_length=data_size,
                    exception=None
                )
            else:
                logger.error(f"❌ DB 작업 실패 ({symbol}): 시뮬레이션 타임아웃")
                self.environment.events.request.fire(
                    request_type="DB",
                    name=f"heavy_query_{symbol}",
                    response_time=processing_time * 1000,
                    response_length=0,
                    exception=Exception("Database timeout simulation")
                )
            
        except Exception as e:
            logger.error(f"❌ DB 작업 오류 ({symbol}): {e}")
            self.environment.events.request.fire(
                request_type="DB",
                name=f"heavy_query_{symbol}",
                response_time=0,
                response_length=0,
                exception=e
            )

class StockSystemLoadTest(HttpUser):
    """시스템 전체 부하테스트 (더 공격적)"""
    
    wait_time = between(0.1, 1)  # 0.1-1초 대기 (더 빠름)
    
    def on_start(self):
        """공격적 테스트 초기화"""
        self.symbols = ['AAPL'] * 100  # 같은 심볼 반복으로 부하 증가
        logger.info("🚀 공격적 부하테스트 시작")
    
    @task
    def stress_test_rapid_requests(self):
        """빠른 연속 요청으로 시스템 스트레스 테스트"""
        symbol = random.choice(self.symbols)
        
        try:
            start_time = time.time()
            
            # 매우 빠른 요청 시뮬레이션
            time.sleep(random.uniform(0.01, 0.05))  # 10-50ms
            
            processing_time = time.time() - start_time
            
            # 부하가 높을 때 성공률 감소 (70%)
            if random.random() < 0.7:
                self.environment.events.request.fire(
                    request_type="STRESS",
                    name=f"rapid_request_{symbol}",
                    response_time=processing_time * 1000,
                    response_length=512,
                    exception=None
                )
            else:
                self.environment.events.request.fire(
                    request_type="STRESS",
                    name=f"rapid_request_{symbol}",
                    response_time=processing_time * 1000,
                    response_length=0,
                    exception=Exception("High load failure simulation")
                )
                
        except Exception as e:
            logger.error(f"❌ 스트레스 테스트 오류: {e}")
