#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from kafka import KafkaConsumer
import os
import sys

def create_consumer_group():
    """명시적인 Consumer Group 생성"""
    try:
        print("🚀 Kafka Consumer Group 생성 시작")
        
        # Kafka Consumer 설정
        consumer = KafkaConsumer(
            'kis-stock', 'yfinance-stock',
            bootstrap_servers=['kafka:29092'],
            group_id='stock-data-consumer-group',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=30000  # 30초 후 종료
        )
        
        print("✅ Consumer Group 'stock-data-consumer-group' 생성됨")
        print("📋 구독 토픽: kis-stock, yfinance-stock")
        
        message_count = 0
        max_messages = 10  # 최대 10개 메시지만 처리
        
        print(f"📊 최대 {max_messages}개 메시지 처리 후 종료...")
        
        for message in consumer:
            try:
                message_count += 1
                
                # 메시지 정보 출력
                topic = message.topic
                partition = message.partition
                offset = message.offset
                value = message.value
                
                print(f"📦 메시지 {message_count}: 토픽={topic}, 파티션={partition}, 오프셋={offset}")
                
                if value:
                    try:
                        data = json.loads(value)
                        symbol = data.get('symbol', 'Unknown')
                        price = data.get('price', 'N/A')
                        print(f"   📊 데이터: {symbol} = ${price}")
                    except json.JSONDecodeError:
                        print(f"   ⚠️ JSON 파싱 실패: {value[:100]}...")
                
                # 최대 메시지 수에 도달하면 종료
                if message_count >= max_messages:
                    print(f"✅ {max_messages}개 메시지 처리 완료. Consumer 종료.")
                    break
                    
            except Exception as e:
                print(f"❌ 메시지 처리 오류: {e}")
                continue
        
        # Consumer 종료
        consumer.close()
        print("🔴 Consumer 연결 종료")
        
    except Exception as e:
        print(f"❌ Consumer Group 생성 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🎯 Kafka Consumer Group 테스트 시작")
    create_consumer_group()
    print("📢 프로그램 종료")
