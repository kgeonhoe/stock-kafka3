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
from common.technical_indicator_calculator_postgres import TechnicalIndicatorCalculatorPostgreSQL
from common.database import PostgreSQLManager
from config.kafka_config import KafkaConfig

def process_realtime_data_with_spark():
    """Spark로 실시간 데이터 처리 및 Redis 저장 + 기술적 지표 계산"""
    try:
        print("🚀 Redis + 기술적 지표 통합 Spark 처리 시작")
        
        # Redis 및 기술적 지표 계산기 초기화
        redis_manager = RedisManager()
        indicator_calculator = TechnicalIndicatorCalculatorPostgreSQL()
        db_manager = PostgreSQLManager()
        
        # PostgreSQL에서 daily_watchlist 로드
        def load_watchlist_symbols():
            try:
                query = """
                SELECT DISTINCT symbol 
                FROM daily_watchlist 
                ORDER BY symbol
                """
                result = db_manager.execute_query(query)
                if result:
                    symbols = [row[0] for row in result]
                    print(f"✅ PostgreSQL에서 {len(symbols)}개 daily_watchlist 종목 로드")
                    return symbols
                else:
                    print("⚠️ daily_watchlist가 비어있음, 빈 목록 반환")
                    print("💡 PostgreSQL daily_watchlist 테이블에 관심종목을 추가해주세요.")
                    return []  # 빈 리스트 반환
            except Exception as e:
                print(f"❌ daily_watchlist 로드 실패: {e}")
                print("💡 PostgreSQL daily_watchlist 테이블을 확인해주세요.")
                return []  # 빈 리스트 반환
        
        # Redis 연결 테스트
        if redis_manager.redis_client.ping():
            print("✅ Redis 연결 성공")
            
            # PostgreSQL watchlist에서 관심종목 데이터 초기 로딩
            try:
                watchlist_symbols = load_watchlist_symbols()
                for symbol in watchlist_symbols:
                    watchlist_data = {
                        'symbol': symbol,
                        'added_date': datetime.now().isoformat(),
                        'status': 'active',
                        'alerts_enabled': 'true',  # Redis는 문자열로 저장
                        'price_target': '',  # None 대신 빈 문자열 사용
                        'notes': f'{symbol} PostgreSQL watchlist 종목'
                    }
                    redis_manager.redis_client.hset(f"watchlist:{symbol}", mapping=watchlist_data)
                print(f"✅ PostgreSQL watchlist 데이터 Redis 동기화 완료: {len(watchlist_symbols)}개")
            except Exception as e:
                print(f"❌ watchlist 데이터 Redis 동기화 실패: {e}")
                
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
                        
                        # 데이터 소스별 필드명 통합 처리
                        price = message_data.get('price')
                        if price is None:
                            price = message_data.get('current_price')  # YFinance용
                        
                        # 소스 정보 통합
                        source = message_data.get('source', 'unknown')
                        if source == 'unknown':
                            source = message_data.get('data_source', 'unknown')
                        
                        if not symbol or price is None:
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
                            'change_percent': message_data.get('change_percent'),
                            # YFinance 추가 필드들
                            'previous_close': message_data.get('previous_close'),
                            'open_price': message_data.get('open_price'),
                            'day_high': message_data.get('day_high'),
                            'day_low': message_data.get('day_low'),
                            'change_rate': message_data.get('change_rate')
                        }
                        
                        # Redis 저장
                        success = redis_manager.store_realtime_data(symbol, redis_data)
                        if success:
                            processed_count += 1
                            print(f"  ✅ Redis 저장 성공: {symbol}")
                        else:
                            print(f"  ❌ Redis 저장 실패: {symbol}")
                        
                        # 2. 기술적 지표 계산 (PostgreSQL에서 과거 데이터 가져와서 계산)
                        try:
                            # PostgreSQL에서 최근 60일 데이터 조회
                            db = indicator_calculator.db  # PostgreSQL 연결 재사용
                            
                            query = """
                                SELECT symbol, date, open, high, low, close, volume
                                FROM stock_data
                                WHERE symbol = %s
                                  AND date >= CURRENT_DATE - INTERVAL '60 days'
                                ORDER BY date
                                LIMIT 60
                            """
                            
                            stock_data = []
                            with db.get_connection() as conn:
                                with conn.cursor() as cur:
                                    cur.execute(query, (symbol,))
                                    rows = cur.fetchall()
                                    
                                    for row in rows:
                                        stock_data.append({
                                            'symbol': row[0],
                                            'date': row[1],
                                            'open': float(row[2]) if row[2] else 0,
                                            'high': float(row[3]) if row[3] else 0, 
                                            'low': float(row[4]) if row[4] else 0,
                                            'close': float(row[5]) if row[5] else 0,
                                            'volume': int(row[6]) if row[6] else 0
                                        })
                            
                            # 현재 가격을 마지막 데이터로 추가 (오늘 데이터)
                            if stock_data and current_price:
                                from datetime import date
                                today_data = stock_data[-1].copy()  # 어제 데이터 기준
                                today_data.update({
                                    'date': date.today(),
                                    'close': current_price,
                                    'high': max(today_data['high'], current_price),
                                    'low': min(today_data['low'], current_price)
                                })
                                stock_data.append(today_data)
                            
                            # 기술적 지표 계산
                            if len(stock_data) >= 20:  # 최소 20일 데이터 필요
                                indicators_list = indicator_calculator.calculate_all_indicators(stock_data)
                                
                                if indicators_list:
                                    # Redis에 기술적 지표 저장 (최신 지표만)
                                    latest_indicator = indicators_list[-1] if indicators_list else None
                                    if latest_indicator:
                                        # Redis에 맞는 형태로 변환
                                        redis_indicators = {
                                            'rsi': latest_indicator.get('rsi'),
                                            'macd': latest_indicator.get('macd_line'),
                                            'macd_signal': latest_indicator.get('macd_signal'),
                                            'sma_20': latest_indicator.get('sma_20'),
                                            'ema_12': latest_indicator.get('ema_12'),
                                            'ema_26': latest_indicator.get('ema_26'),
                                            'bb_upper': latest_indicator.get('bb_upper'),
                                            'bb_middle': latest_indicator.get('bb_middle'),
                                            'bb_lower': latest_indicator.get('bb_lower'),
                                            'timestamp': datetime.now().isoformat()
                                        }
                                        
                                        redis_manager.store_technical_indicators(symbol, redis_indicators)
                                        indicator_count += 1
                                        
                                        # 주요 지표 로깅  
                                        rsi = redis_indicators.get('rsi')
                                        macd = redis_indicators.get('macd')
                                        
                                        if rsi is not None and macd is not None:
                                            print(f"  📈 {symbol} 지표: RSI={rsi:.1f}, MACD={macd:.4f}")
                                        else:
                                            print(f"  📈 {symbol} 지표 저장 완료")
                                        
                                else:
                                    print(f"  ⚠️ {symbol}: 기술적 지표 계산 결과 없음")
                            else:
                                print(f"  ⚠️ {symbol}: 기술적 지표 계산 불가 (데이터 부족: {len(stock_data)}개)")
                                
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
