#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
import os
import sys

# 컨테이너 환경에서는 /app이 기본 경로이므로 Python 경로에 추가
sys.path.insert(0, '/app')

# PySpark Structured Streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 로컬 패키지들
from common.redis_manager import RedisManager
from common.technical_indicator_calculator import TechnicalIndicatorCalculator
from config.kafka_config import KafkaConfig

def process_realtime_data_with_spark():
    """Spark로 실시간 데이터 처리 및 Redis 저장 + 기술적 지표 계산"""
    try:
        print("🚀 Redis + 기술적 지표 통합 Spark 처리 시작")
        
        # Redis 및 기술적 지표 계산기 초기화
        redis_manager = RedisManager()
        indicator_calculator = TechnicalIndicatorCalculator()
        
        # Redis 연결 테스트
        if redis_manager.redis_client.ping():
            print("✅ Redis 연결 성공")
        else:
            print("❌ Redis 연결 실패")
            return
        
        # Spark 설정
        spark = SparkSession.builder \
            .appName("RealTimeStockDataProcessorWithRedis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Kafka 설정 (config 파일 사용)
        kafka_bootstrap_servers = "kafka:29092"  # 컨테이너 내부 주소
        topics = [KafkaConfig.TOPIC_KIS_STOCK, KafkaConfig.TOPIC_YFINANCE_STOCK]
        consumer_group = KafkaConfig.CONSUMER_GROUP_REALTIME
        
        print(f"📡 Kafka 서버: {kafka_bootstrap_servers}")
        print(f"📋 구독 토픽: {topics}")
        print(f"👥 Consumer Group: {consumer_group}")
        
        # JSON 스키마 정의
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("source", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("volume", StringType(), True),
            StructField("change", StringType(), True),
            StructField("change_percent", StringType(), True)
        ])
        
        def process_batch(batch_df, batch_id):
            """배치 데이터 처리 함수"""
            try:
                print(f"\n📦 배치 {batch_id} 처리 시작...")
                
                # 배치가 비어있으면 건너뛰기
                count = batch_df.count()
                if count == 0:
                    print(f"📦 배치 {batch_id}: 데이터 없음")
                    return
                
                print(f"📦 배치 {batch_id}: {count}개 메시지 처리")
                
                # 데이터 수집 및 처리
                rows = batch_df.collect()
                processed_count = 0
                indicator_count = 0
                
                for row in rows:
                    try:
                        # 메시지 파싱
                        message_data = json.loads(row.message)
                        symbol = message_data.get('symbol')
                        price = message_data.get('price')
                        source = message_data.get('source', 'unknown')
                        
                        if not symbol or not price:
                            print(f"⚠️ 필수 데이터 누락: symbol={symbol}, price={price}")
                            continue
                        
                        current_price = float(price)
                        print(f"📊 처리중: {symbol} = ${current_price:.2f} ({source})")
                        
                        # 1. Redis에 실시간 데이터 저장
                        redis_data = {
                            'symbol': symbol,
                            'price': current_price,
                            'source': source,
                            'timestamp': datetime.now().isoformat(),
                            'kafka_timestamp': row.kafka_timestamp.isoformat() if row.kafka_timestamp else None,
                            'volume': message_data.get('volume'),
                            'change': message_data.get('change'),
                            'change_percent': message_data.get('change_percent')
                        }
                        
                        # Redis 저장
                        success = redis_manager.store_realtime_data(symbol, redis_data)
                        if success:
                            processed_count += 1
                        
                        # 2. 기술적 지표 계산 (과거 데이터 + 현재 가격)
                        try:
                            indicators = indicator_calculator.calculate_all_indicators(
                                symbol, 
                                current_price=current_price
                            )
                            
                            if indicators:
                                # Redis에 기술적 지표 저장
                                redis_manager.store_technical_indicators(symbol, indicators)
                                indicator_count += 1
                                
                                # 주요 지표 로깅
                                rsi = indicators.get('rsi')
                                macd = indicators.get('macd')
                                sentiment = indicators.get('overall_sentiment', 'neutral')
                                strength = indicators.get('strength', 0)
                                
                                print(f"  📈 {symbol} 지표: RSI={rsi}, MACD={macd:.4f if macd else None}, "
                                      f"신호={sentiment}({strength})")
                                
                                # 중요한 매매 신호만 출력
                                signals = indicators.get('signals', [])
                                if signals and abs(strength) > 30:  # 강한 신호만
                                    print(f"  🚨 {symbol} 중요신호: {signals[0]}")
                                
                            else:
                                print(f"  ⚠️ {symbol}: 기술적 지표 계산 불가 (데이터 부족)")
                                
                        except Exception as indicator_error:
                            print(f"❌ {symbol} 지표 계산 실패: {indicator_error}")
                        
                    except json.JSONDecodeError as e:
                        print(f"❌ JSON 파싱 실패: {e}")
                        continue
                    except Exception as e:
                        print(f"❌ 메시지 처리 실패: {e}")
                        continue
                
                print(f"✅ 배치 {batch_id} 완료: Redis={processed_count}, 지표={indicator_count}")
                
            except Exception as e:
                print(f"❌ 배치 {batch_id} 처리 실패: {e}")
                import traceback
                traceback.print_exc()
        
        # Kafka 스트림 읽기 (consumer group 설정 포함)
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", ",".join(topics)) \
            .option("kafka.group.id", "spark-realtime-consumer-group") \
            .option("kafka.client.id", "spark-realtime-client") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # 메시지 파싱
        parsed_df = df.select(
            col("topic").cast("string"),
            col("timestamp").cast("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("message")
        )
        
        # 배치 처리로 스트림 출력 (consumer group별 체크포인트)
        query = parsed_df.writeStream \
            .outputMode("append") \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", "/data/checkpoints/spark-realtime-consumer-group") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        print("✅ Redis + 기술적 지표 통합 스트림 시작됨")
        print("📊 30초마다 배치 처리됨")
        print("🔄 Ctrl+C로 종료...")
        
        # 쿼리 대기
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Spark 스트림 처리 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 리소스 정리
        try:
            if 'redis_manager' in locals():
                redis_manager.close()
                print("🔴 Redis 연결 종료")
            if 'indicator_calculator' in locals():
                indicator_calculator.close()
                print("📊 기술적 지표 계산기 종료")
            if 'spark' in locals():
                spark.stop()
                print("⚡ Spark 세션 종료")
        except:
            pass

# 메인 함수
def main():
    """메인 함수"""
    print("🚀 Redis + 기술적 지표 통합 실시간 처리 시작")
    print(f"📂 작업 디렉토리: {os.getcwd()}")
    
    try:
        process_realtime_data_with_spark()
    except KeyboardInterrupt:
        print("\n📢 프로그램 종료 요청 감지")
    except Exception as e:
        print(f"❌ 프로그램 실행 오류: {e}")
    finally:
        print("📢 프로그램 종료")

if __name__ == "__main__":
    main()
