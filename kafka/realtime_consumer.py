#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
import os
import sys

# 컨테이너 환경에서는 /app이 기본 경로이므로 Python 경로에 추가
sys.path.insert(0, '/app')

# Kafka 패키지 import 제거 (PySpark Structured Streaming만 사용)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, expr, udf, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

from common.postgresql_manager import PostgreSQLManager
from common.slack_notifier import SlackNotifier
from config.kafka_config import KafkaConfig

class RealtimeStockConsumer:
    """실시간 주식 데이터 컨슈머"""
    
    def __init__(self, bootstrap_servers: str = None,
                db_path: str = '/data/duckdb/stock_data.db'):
        """
        실시간 주식 데이터 컨슈머 초기화
        
        Args:
            bootstrap_servers: Kafka 서버 주소
            db_path: DuckDB 파일 경로
        """
        # 환경변수에서 Kafka 서버 주소 가져오기
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        print(f"🔗 Kafka 서버: {self.bootstrap_servers}")
        
        self.db_path = db_path
        self.slack = SlackNotifier()
        self.spark = self._create_spark_session()
        self.running = True
    
    def _create_spark_session(self) -> SparkSession:
        """
        스파크 세션 생성
        
        Returns:
            SparkSession 인스턴스
        """
        spark = (SparkSession.builder
                .appName("RealtimeStockAnalysis")
                .config("spark.driver.memory", "1g")
                .config("spark.executor.memory", "1g")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
                .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
                .getOrCreate())
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark 세션 생성 완료")
        return spark
    
    def process_realtime_data_with_spark(self):
        """스파크 스트리밍으로 실시간 데이터 처리"""
        print("🚀 스파크 스트리밍 처리 시작")
        
        # 스키마 정의 (test_multi_source_producer의 데이터 형식에 맞게)
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("current_price", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("data_source", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_rate", DoubleType(), True)
        ])
        
        try:
            # Kafka 스트림 읽기
            print(f"📡 Kafka 토픽 구독: {KafkaConfig.TOPIC_KIS_STOCK}, {KafkaConfig.TOPIC_YFINANCE_STOCK}")
            
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", f"{KafkaConfig.TOPIC_KIS_STOCK},{KafkaConfig.TOPIC_YFINANCE_STOCK}") \
                .option("startingOffsets", "latest") \
                .option("kafka.group.id", "stock-consumer-group") \
                .option("kafka.session.timeout.ms", "30000") \
                .option("kafka.request.timeout.ms", "40000") \
                .option("failOnDataLoss", "false") \
                .load()
            
            print("✅ Kafka 스트림 연결 성공")
            
            # JSON 파싱
            parsed_df = df \
                .select(
                    col("topic"),
                    col("timestamp").alias("kafka_timestamp"),
                    from_json(col("value").cast("string"), schema).alias("data")
                ) \
                .select("topic", "kafka_timestamp", "data.*")
            
            # 간단한 콘솔 출력으로 테스트
            query = parsed_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .option("checkpointLocation", "/data/checkpoints/console") \
                .start()
            
            print("🎯 콘솔 출력 시작 - 데이터 수신 대기 중...")
            
            # 스트리밍 처리 진행
            query.awaitTermination()
            
        except Exception as e:
            print(f"❌ Kafka 스트림 처리 오류: {e}")
            import traceback
            traceback.print_exc()
            raise e
    
    def close(self):
        """리소스 정리"""
        if hasattr(self, 'db') and self.db:
            self.db.close()
        if self.spark:
            self.spark.stop()
        self.running = False
        print("📢 컨슈머 종료")

# 메인 함수
def main():
    """메인 함수"""
    print("🚀 Realtime Stock Consumer 시작")
    print(f"📂 작업 디렉토리: {os.getcwd()}")
    print(f"🔧 Python 경로: {sys.path}")
    
    consumer = None
    try:
        consumer = RealtimeStockConsumer()
        consumer.process_realtime_data_with_spark()
    except KeyboardInterrupt:
        print("\n📢 프로그램 종료 요청 감지")
    except Exception as e:
        print(f"❌ 프로그램 실행 오류: {e}")
    finally:
        if consumer:
            consumer.close()
        print("📢 프로그램 종료")

# 엔트리 포인트
if __name__ == "__main__":
    main()
