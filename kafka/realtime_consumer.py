#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, expr, udf, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

import sys
import os
# 컨테이너 환경에서는 /app이 기본 경로이므로 Python 경로에 추가
sys.path.insert(0, '/app')

from common.database import DuckDBManager
from common.slack_notifier import SlackNotifier

class RealtimeStockConsumer:
    """실시간 주식 데이터 컨슈머"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092',
                db_path: str = '/data/duckdb/stock_data.db'):
        """
        실시간 주식 데이터 컨슈머 초기화
        
        Args:
            bootstrap_servers: Kafka 서버 주소
            db_path: DuckDB 파일 경로
        """
        self.bootstrap_servers = bootstrap_servers
        self.db = DuckDBManager(db_path)
        self.slack = SlackNotifier()
        self.spark = self._create_spark_session()
        self.running = True
    
    def _create_spark_session(self) -> SparkSession:
        """
        스파크 세션 생성
        
        Returns:
            SparkSession 인스턴스
        """
        return (SparkSession.builder
                .appName("RealtimeStockAnalysis")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate())
    
    def process_realtime_data_with_spark(self):
        """스파크 스트리밍으로 실시간 데이터 처리"""
        print("🚀 스파크 스트리밍 처리 시작")
        
        # 스키마 정의
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_rate", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        # Kafka 스트림 읽기
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", "stock-realtime-data,nasdaq-tier1-realtime,nasdaq-tier2-realtime") \
            .option("startingOffsets", "latest") \
            .load()
        
        # JSON 파싱
        parsed_df = df \
            .select(from_json(col("value").cast("string"), schema).alias("data"), "topic") \
            .select("data.*", "topic")
        
        # 타임스탬프 변환
        parsed_df = parsed_df.withColumn("event_time", 
                                         expr("to_timestamp(timestamp, 'yyyy-MM-dd''T''HH:mm:ss.SSS')"))
        
        # 10분 윈도우로 데이터 처리
        windowedDF = parsed_df \
            .withWatermark("event_time", "10 minutes") \
            .groupBy(
                col("symbol"),
                window(col("event_time"), "10 minutes", "5 minutes")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                avg("volume").alias("avg_volume")
            )
        
        # 알림 생성 함수
        def process_window_batch(batch_df, batch_id):
            """
            윈도우 배치 처리 함수
            
            Args:
                batch_df: 배치 데이터프레임
                batch_id: 배치 ID
            """
            if batch_df.isEmpty():
                return
            
            # 판다스 데이터프레임으로 변환
            pandas_df = batch_df.toPandas()
            
            # 주식 데이터 가져오기 (기술적 지표 포함)
            for _, row in pandas_df.iterrows():
                symbol = row['symbol']
                avg_price = row['avg_price']
                window_start = row['window.start']
                window_end = row['window.end']
                
                # DuckDB에서 기술적 지표 조회
                try:
                    indicators = self.db.conn.execute("""
                        SELECT * FROM stock_data_technical_indicators
                        WHERE symbol = ?
                        ORDER BY date DESC
                        LIMIT 1
                    """, (symbol,)).fetchone()
                    
                    if indicators:
                        # 알림 조건 검사
                        self._check_alert_conditions(symbol, avg_price, indicators)
                        
                except Exception as e:
                    print(f"❌ 기술적 지표 조회 오류: {e}")
        
        # 스트림 처리 및 출력
        query = windowedDF \
            .writeStream \
            .foreachBatch(process_window_batch) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()
        
        # 스트리밍 처리 진행
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n📢 스트리밍 처리 종료 요청 감지")
            query.stop()
    
    def _check_alert_conditions(self, symbol: str, current_price: float, indicators: tuple):
        """
        알림 조건 검사
        
        Args:
            symbol: 종목 심볼
            current_price: 현재 가격
            indicators: 기술적 지표 튜플
        """
        try:
            # 지표 값 추출
            bb_upper = indicators[2] if indicators[2] is not None else float('inf')
            bb_lower = indicators[4] if indicators[4] is not None else 0
            rsi = indicators[8] if indicators[8] is not None else 50
            
            # 1. 볼린저 밴드 상단/하단 돌파
            if current_price > bb_upper:
                self.slack.send_stock_alert(
                    symbol=symbol,
                    price=current_price,
                    change_percent=0,  # 변동률 정보 없음
                    alert_type="볼린저 밴드",
                    message=f"{symbol} 볼린저 밴드 상단 돌파 (상단: ${bb_upper:.2f}, 현재: ${current_price:.2f})"
                )
                print(f"🔔 {symbol} 볼린저 밴드 상단 돌파")
            
            elif current_price < bb_lower:
                self.slack.send_stock_alert(
                    symbol=symbol,
                    price=current_price,
                    change_percent=0,  # 변동률 정보 없음
                    alert_type="볼린저 밴드",
                    message=f"{symbol} 볼린저 밴드 하단 돌파 (하단: ${bb_lower:.2f}, 현재: ${current_price:.2f})"
                )
                print(f"🔔 {symbol} 볼린저 밴드 하단 돌파")
            
            # 2. RSI 과매수/과매도
            if rsi > 70:
                self.slack.send_stock_alert(
                    symbol=symbol,
                    price=current_price,
                    change_percent=0,
                    alert_type="RSI",
                    message=f"{symbol} RSI 과매수 구간 (RSI: {rsi:.2f})"
                )
                print(f"🔔 {symbol} RSI 과매수 구간 (RSI: {rsi:.2f})")
            
            elif rsi < 30:
                self.slack.send_stock_alert(
                    symbol=symbol,
                    price=current_price,
                    change_percent=0,
                    alert_type="RSI",
                    message=f"{symbol} RSI 과매도 구간 (RSI: {rsi:.2f})"
                )
                print(f"🔔 {symbol} RSI 과매도 구간 (RSI: {rsi:.2f})")
                
        except Exception as e:
            print(f"❌ 알림 조건 검사 오류: {e}")
    
    def close(self):
        """리소스 정리"""
        self.db.close()
        self.spark.stop()
        self.running = False
        print("📢 컨슈머 종료")

# 메인 함수
def main():
    """메인 함수"""
    consumer = RealtimeStockConsumer()
    
    try:
        # 스파크 스트리밍 처리 시작
        consumer.process_realtime_data_with_spark()
    except KeyboardInterrupt:
        print("\n📢 프로그램 종료 요청 감지")
    finally:
        consumer.close()
        print("📢 프로그램 종료")

# 엔트리 포인트
if __name__ == "__main__":
    main()
