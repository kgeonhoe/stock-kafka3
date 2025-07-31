#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka Consumer Group 등록용 스크립트
Spark Structured Streaming이 UI에 나타나지 않는 문제를 해결하기 위해
일반 Kafka Consumer로 Consumer Group을 등록합니다.
"""

import sys
import time
import signal
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# 컨테이너 환경에서는 /app이 기본 경로이므로 Python 경로에 추가
sys.path.insert(0, '/app')

from config.kafka_config import KafkaConfig

class KafkaConsumerGroupRegister:
    def __init__(self):
        self.consumer = None
        self.running = False
        
    def create_consumer(self):
        """Kafka Consumer 생성"""
        try:
            self.consumer = KafkaConsumer(
                KafkaConfig.TOPIC_KIS_STOCK,
                KafkaConfig.TOPIC_YFINANCE_STOCK,
                bootstrap_servers='kafka:29092',  # 컨테이너 내부 주소
                group_id=KafkaConfig.CONSUMER_GROUP_REALTIME,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                value_deserializer=lambda m: m.decode('utf-8') if m else None,
                consumer_timeout_ms=30000  # 30초 타임아웃
            )
            print(f"✅ Consumer Group '{KafkaConfig.CONSUMER_GROUP_REALTIME}' 등록됨")
            print(f"📋 구독 토픽: {[KafkaConfig.TOPIC_KIS_STOCK, KafkaConfig.TOPIC_YFINANCE_STOCK]}")
            return True
        except Exception as e:
            print(f"❌ Consumer 생성 실패: {e}")
            return False
    
    def start_consuming(self):
        """메시지 소비 시작 (실제로는 그룹 등록만 목적)"""
        if not self.consumer:
            print("❌ Consumer가 생성되지 않았습니다")
            return
            
        self.running = True
        print("🚀 Consumer Group 등록 및 유지 시작...")
        
        try:
            while self.running:
                # 메시지를 실제로 처리하지는 않고, 그룹 등록만 유지
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if message_batch:
                    message_count = sum(len(messages) for messages in message_batch.values())
                    print(f"📨 {message_count}개 메시지 감지됨 (처리하지 않음)")
                
                # 주기적으로 커밋하여 그룹 활성 상태 유지
                self.consumer.commit()
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n📢 중단 신호 감지됨")
        except Exception as e:
            print(f"❌ Consumer 실행 오류: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Consumer 중지"""
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
                print("🔴 Consumer 연결 종료")
            except:
                pass

def signal_handler(signum, frame):
    """신호 처리기"""
    print(f"\n📢 신호 {signum} 받음, 종료 중...")
    global consumer_register
    if consumer_register:
        consumer_register.stop()
    sys.exit(0)

def main():
    global consumer_register
    
    print("🚀 Kafka Consumer Group 등록 서비스 시작")
    
    # 신호 처리기 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer_register = KafkaConsumerGroupRegister()
    
    # Consumer 생성
    if not consumer_register.create_consumer():
        print("❌ Consumer 생성 실패, 종료")
        return
    
    # 소비 시작
    consumer_register.start_consuming()

if __name__ == "__main__":
    consumer_register = None
    main()
