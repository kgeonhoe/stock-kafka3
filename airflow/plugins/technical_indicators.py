#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional

# Spark 관련 임포트 (조건부)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lag, avg, stddev, when, expr, sum
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
    print("✅ PySpark 임포트 성공")
except ImportError as e:
    print(f"⚠️ PySpark 임포트 실패: {e}")
    print("📌 Docker 환경에서만 실행 가능합니다.")
    SPARK_AVAILABLE = False
    # 더미 클래스들 정의
    class SparkSession:
        pass
    class Window:
        pass

# 프로젝트 경로 추가
sys.path.insert(0, '/opt/airflow')

from common.database import DuckDBManager

class TechnicalIndicators:
    """기술적 지표 계산 클래스 (Spark 기반 - 고성능 대용량 데이터 처리)"""
    
    def __init__(self, db_path: str = "/data/duckdb/stock_data.db"):
        """
        Spark 기반 기술적 지표 계산 클래스 초기화
        
        Args:
            db_path: DuckDB 파일 경로
        """
        self.db = DuckDBManager(db_path)
        
        # Spark 세션 생성 (분산 처리를 위한 고성능 엔진)
        if SPARK_AVAILABLE:
            self.spark = self._create_spark_session()
            print("🚀 Spark 세션 생성 완료 - 고성능 분산 처리 준비")
        else:
            self.spark = None
            print("❌ Spark 세션을 생성할 수 없습니다. Docker 환경에서 실행해주세요.")
    
    def _create_spark_session(self):
        """
        고성능 Spark 세션 생성
        
        Returns:
            SparkSession 인스턴스 - 분산 데이터 처리 엔진
        """
        if not SPARK_AVAILABLE:
            return None
            
        return (SparkSession.builder
                .appName("StockTechnicalIndicators")
                .config("spark.driver.memory", "4g")          # 드라이버 메모리 증가
                .config("spark.executor.memory", "4g")        # 실행자 메모리 증가  
                .config("spark.executor.cores", "4")          # CPU 코어 수 설정
                .config("spark.sql.adaptive.enabled", "true") # 적응형 쿼리 최적화
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate())
    
    def load_stock_data(self, symbol: Optional[str] = None) -> pd.DataFrame:
        """
        주가 데이터 로드 (DuckDB → Pandas → Spark 변환용)
        
        Args:
            symbol: 종목 심볼 (None이면 모든 종목)
            
        Returns:
            주가 데이터 DataFrame (Spark 변환을 위한 중간 단계)
        """
        if symbol:
            query = f"""
                SELECT * FROM stock_data
                WHERE symbol = '{symbol}'
                ORDER BY date
            """
        else:
            query = """
                SELECT * FROM stock_data
                ORDER BY symbol, date
            """
        
        result = self.db.conn.execute(query).fetchall()
        columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(result, columns=columns)
        
        return df
    
    def calculate_indicators(self, symbol: Optional[str] = None):
        """
        Spark 기반 고성능 기술적 지표 계산 및 저장
        - 분산 처리로 대용량 데이터 고속 처리
        - 메모리 효율적인 윈도우 함수 활용
        - 병렬 계산으로 처리 시간 최소화
        
        Args:
            symbol: 종목 심볼 (None이면 모든 종목을 병렬 처리)
        """
        # Spark 사용 가능 여부 확인
        if not SPARK_AVAILABLE or self.spark is None:
            print("❌ Spark가 사용 불가능합니다. Docker 환경에서 실행해주세요.")
            return
        
        # 주가 데이터 로드
        stock_data = self.load_stock_data(symbol)
        
        if stock_data.empty:
            print(f"⚠️ {'모든 종목' if symbol is None else symbol}의 주가 데이터가 없습니다.")
            return
        
        print(f"� Spark 분산 처리로 {'모든 종목' if symbol is None else symbol}의 기술적 지표 계산 시작")
        print(f"📊 처리 대상 레코드: {len(stock_data):,}개")
        
        # Spark DataFrame으로 변환 (고성능 분산 처리 시작)
        spark_df = self.spark.createDataFrame(stock_data)
        
        # 데이터 캐싱으로 반복 연산 성능 향상
        spark_df.cache()
        
        print("⚡ Spark DataFrame 캐싱 완료 - 연산 성능 최적화")
        
        # 종목별 윈도우 정의 (Spark 분산 윈도우 함수 - 고성능 병렬 처리)
        window_spec = Window.partitionBy("symbol").orderBy("date")
        
        print("📈 1/7 단계: 이동평균선(SMA) 병렬 계산...")
        # 1. 이동평균선 (SMA) - Spark 윈도우 함수로 고속 처리
        for period in [5, 20, 60, 112, 224, 448]:
            window_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period-1), 0)
            spark_df = spark_df.withColumn(f"sma_{period}", avg("close").over(window_avg))
        
        print("📊 2/7 단계: 지수이동평균(EMA) 병렬 계산...")
        # 2. 지수이동평균 (EMA) - 재귀적 계산을 Spark로 최적화
        for period in [5, 20, 60]:
            spark_df = self._calculate_ema(spark_df, period)
        
        print("📉 3/7 단계: 볼린저 밴드 병렬 계산...")
        # 3. 볼린저 밴드 (기준: 20일 이동평균, 표준편차 2) - Spark 통계 함수 활용
        window_std = Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)
        spark_df = spark_df.withColumn("bb_middle", col("sma_20"))
        spark_df = spark_df.withColumn("bb_std", stddev("close").over(window_std))
        spark_df = spark_df.withColumn("bb_upper", col("bb_middle") + (col("bb_std") * 2))
        spark_df = spark_df.withColumn("bb_lower", col("bb_middle") - (col("bb_std") * 2))
        
        print("⚡ 4/7 단계: MACD 지표 병렬 계산...")
        # 4. MACD (12일 EMA, 26일 EMA, 9일 신호선) - Spark 고성능 계산
        spark_df = self._calculate_ema(spark_df, 12, "ema_12")
        spark_df = self._calculate_ema(spark_df, 26, "ema_26")
        spark_df = spark_df.withColumn("macd", col("ema_12") - col("ema_26"))
        
        # MACD 신호선 (9일 EMA) - Spark SQL 표현식으로 최적화
        window_signal = Window.partitionBy("symbol").orderBy("date")
        spark_df = spark_df.withColumn("macd_signal", 
                                       expr("avg(macd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)"))
        spark_df = spark_df.withColumn("macd_histogram", col("macd") - col("macd_signal"))
        
        print("🎯 5/7 단계: RSI 지표 병렬 계산...")
        # 5. RSI (14일) - Spark 윈도우 함수로 효율적 계산
        spark_df = self._calculate_rsi(spark_df, 14)
        
        print("📊 6/7 단계: CCI 지표 병렬 계산...")
        # 6. CCI (상품채널지수, 20일) - Spark 수학 함수 활용
        spark_df = self._calculate_cci(spark_df, 20)
        
        print("📈 7/7 단계: OBV 지표 병렬 계산...")
        # 7. OBV (On Balance Volume) - Spark 누적 함수로 고속 처리
        spark_df = self._calculate_obv(spark_df)
        
        print("🔥 모든 기술적 지표 계산 완료! 결과 정리 중...")
        
        # 필요한 열만 선택하여 최종 결과 생성 (Spark 컬럼 선택으로 메모리 최적화)
        result_df = spark_df.select(
            "symbol", "date", 
            "bb_upper", "bb_middle", "bb_lower",
            "macd", "macd_signal", "macd_histogram",
            "rsi",
            "sma_5", "sma_20", "sma_60",
            "ema_5", "ema_20", "ema_60",
            "cci", "obv",
            col("sma_112").alias("ma_112"),
            col("sma_224").alias("ma_224"),
            col("sma_448").alias("ma_448")
        )
        
        # Spark에서 Pandas로 변환 (최종 저장을 위한 변환)
        print("💾 Spark 결과를 DuckDB 저장용으로 변환 중...")
        pandas_df = result_df.toPandas()
        
        # NaN 값을 None으로 변환 (DuckDB 호환성)
        pandas_df = pandas_df.where(pd.notnull(pandas_df), None)
        
        # DuckDB에 배치 저장 (고성능 저장)
        print("💾 DuckDB에 배치 저장 중...")
        saved_count = 0
        for _, row in pandas_df.iterrows():
            try:
                self.db.save_technical_indicators(row.to_dict())
                saved_count += 1
            except Exception as e:
                print(f"❌ 기술적 지표 저장 오류: {e}")
        
        # 캐시 해제로 메모리 정리
        spark_df.unpersist()
        
        print(f"🎉 Spark 고성능 처리 완료! 기술적 지표 {saved_count:,}개 저장")
        print(f"📈 처리 성능: {len(stock_data):,} → {saved_count:,} 레코드 변환")
    
    def _calculate_ema(self, df, period, col_name=None):
        """
        Spark 기반 지수이동평균(EMA) 고성능 계산
        
        Args:
            df: Spark DataFrame
            period: 계산 기간
            col_name: 결과 컬럼명
            
        Returns:
            EMA 컬럼이 추가된 Spark DataFrame (분산 처리됨)
        """
        if col_name is None:
            col_name = f"ema_{period}"
            
        # 초기값으로 SMA 사용 (Spark 윈도우 함수)
        window_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period-1), 0)
        df = df.withColumn(f"{col_name}_init", avg("close").over(window_avg))
        
        # 가중치 계산 (EMA 공식)
        alpha = 2.0 / (period + 1.0)
        
        # EMA 계산 (Spark SQL 표현식으로 최적화)
        return df.withColumn(col_name, 
                             when(col("close").isNull(), None)
                             .otherwise(expr(f"""
                                 CASE 
                                     WHEN {col_name}_init IS NULL THEN NULL
                                     ELSE {col_name}_init * (1 - {alpha}) + close * {alpha} 
                                 END
                             """)))
    
    def _calculate_rsi(self, df, period=14):
        """
        Spark 기반 RSI(Relative Strength Index) 고성능 계산
        
        Args:
            df: Spark DataFrame
            period: 계산 기간 (기본 14일)
            
        Returns:
            RSI 컬럼이 추가된 Spark DataFrame (분산 처리됨)
        """
        # 전일 종가 계산 (Spark lag 윈도우 함수)
        window_lag = Window.partitionBy("symbol").orderBy("date")
        df = df.withColumn("prev_close", lag("close", 1).over(window_lag))
        
        # 상승폭과 하락폭 계산 (Spark when 조건문)
        df = df.withColumn("price_diff", col("close") - col("prev_close"))
        df = df.withColumn("gain", when(col("price_diff") > 0, col("price_diff")).otherwise(0))
        df = df.withColumn("loss", when(col("price_diff") < 0, -col("price_diff")).otherwise(0))
        
        # 이동평균 계산 (Spark 윈도우 함수로 고속 처리)
        window_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period-1), 0)
        df = df.withColumn("avg_gain", avg("gain").over(window_avg))
        df = df.withColumn("avg_loss", avg("loss").over(window_avg))
        
        # RSI 계산 (Spark 컬럼 연산)
        df = df.withColumn("rs", col("avg_gain") / col("avg_loss"))
        df = df.withColumn("rsi", 100 - (100 / (1 + col("rs"))))
        
        return df
    
    def _calculate_cci(self, df, period=20):
        """
        Spark 기반 CCI(Commodity Channel Index) 고성능 계산
        
        Args:
            df: Spark DataFrame
            period: 계산 기간 (기본 20일)
            
        Returns:
            CCI 컬럼이 추가된 Spark DataFrame (분산 처리됨)
        """
        # 전형적 가격 계산 (Spark 컬럼 연산)
        df = df.withColumn("typical_price", (col("high") + col("low") + col("close")) / 3)
        
        # 이동평균 계산 (Spark 윈도우 함수)
        window_avg = Window.partitionBy("symbol").orderBy("date").rowsBetween(-(period-1), 0)
        df = df.withColumn("tp_sma", avg("typical_price").over(window_avg))
        
        # 편차의 절대값 평균 계산 (Spark SQL 표현식으로 최적화)
        df = df.withColumn("mean_deviation", 
                           expr(f"avg(abs(typical_price - tp_sma)) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN {-(period-1)} PRECEDING AND CURRENT ROW)"))
        
        # CCI 계산 (Spark 수학 연산)
        df = df.withColumn("cci", (col("typical_price") - col("tp_sma")) / (0.015 * col("mean_deviation")))
        
        return df
    
    def _calculate_obv(self, df):
        """
        Spark 기반 OBV(On Balance Volume) 고성능 계산
        
        Args:
            df: Spark DataFrame
            
        Returns:
            OBV 컬럼이 추가된 Spark DataFrame (분산 처리됨)
        """
        # 전일 종가 계산 (Spark lag 윈도우 함수)
        window = Window.partitionBy("symbol").orderBy("date")
        df = df.withColumn("prev_close", lag("close", 1).over(window))
        
        # OBV 계산 규칙 (Spark when 조건문으로 최적화)
        df = df.withColumn("obv_value", 
                           when(col("close") > col("prev_close"), col("volume"))
                           .when(col("close") < col("prev_close"), -col("volume"))
                           .otherwise(0))
        
        # 누적합 계산 (Spark sum 윈도우 함수로 고속 처리)
        df = df.withColumn("obv", sum("obv_value").over(window))
        
        return df
    
    def close(self):
        """Spark 세션 및 데이터베이스 리소스 정리"""
        self.db.close()
        if SPARK_AVAILABLE and self.spark is not None:
            self.spark.stop()
            print("🔥 Spark 세션 종료 - 리소스 정리 완료")

# Airflow 태스크 함수
def calculate_technical_indicators_task(**kwargs):
    """
    Airflow 태스크: Spark 기반 고성능 기술적 지표 계산
    
    Args:
        **kwargs: Airflow 컨텍스트
        
    Returns:
        성공 여부 (True/False)
    """
    indicators = TechnicalIndicators()
    
    try:
        print("🚀 Spark 기반 대용량 기술적 지표 계산 시작!")
        # 모든 종목에 대해 병렬 기술적 지표 계산
        indicators.calculate_indicators()
        indicators.close()
        print("🎉 Spark 고성능 처리 완료!")
        return True
    
    except Exception as e:
        indicators.close()
        print(f"❌ Spark 기술적 지표 계산 오류: {e}")
        return False
