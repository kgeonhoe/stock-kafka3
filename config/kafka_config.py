"""
Kafka Configuration
"""

import os

class KafkaConfig:
    """Kafka 설정 관리"""
    
    # Kafka 브로커
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # 토픽 정의
    TOPIC_REALTIME_DATA = 'nasdaq-realtime'
    TOPIC_DAILY_DATA = 'nasdaq-daily'
    TOPIC_TECHNICAL_SIGNALS = 'technical-signals'
    TOPIC_TRADING_SIGNALS = 'trading-signals'
    TOPIC_KIS_STOCK = 'kis-stock'
    TOPIC_YFINANCE_STOCK = 'yfinance-stock'
    
    # Consumer 그룹
    CONSUMER_GROUP_REALTIME = 'realtime-consumer-group'
    CONSUMER_GROUP_ANALYTICS = 'analytics-consumer-group'
    
    # Producer 설정
    PRODUCER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'value_serializer': lambda v: v.encode('utf-8') if isinstance(v, str) else v,
        'compression_type': 'gzip',
        'acks': 'all',
        'retries': 3
    }
    
    # Consumer 설정
    CONSUMER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
   'auto_commit_interval_ms': 5000,
        'value_deserializer': lambda m: m.decode('utf-8') if m else None
    }