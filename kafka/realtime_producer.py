#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import time
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any, List, Optional

import sys
import os
# 컨테이너 환경에서는 /app이 기본 경로이므로 Python 경로에 추가
sys.path.insert(0, '/app')

from common.postgresql_manager import PostgreSQLManager
from common.kis_api_client import KISAPIClient

class RealtimeStockProducer:
    """실시간 주식 데이터 프로듀서"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092',
                db_path: str = '/data/duckdb/stock_data.db'):
        """
        실시간 주식 데이터 프로듀서 초기화
        
        Args:
            bootstrap_servers: Kafka 서버 주소
            db_path: DuckDB 파일 경로
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.db = PostgreSQLManager(
            host="postgres",  # Docker 환경
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow"
        )
        self.kis_client = KISAPIClient()
    
    async def produce_realtime_data(self, symbol: str):
        """
        실시간 주가 데이터 수집 및 전송
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            전송 성공 여부
        """
        try:
            # 시세 조회
            quote = await self.kis_client.get_nasdaq_quote(symbol)
            
            if quote:
                # 티어에 따른 토픽 결정
                tier = self._get_symbol_tier(symbol)
                topic = f"nasdaq-tier{tier}-realtime"
                
                # 메시지 전송
                self.producer.send(topic, quote)
                self.producer.send('stock-realtime-data', quote)  # 통합 토픽에도 전송
                
                print(f"✅ {symbol} 실시간 데이터 전송 완료 (Tier {tier})")
                return True
            else:
                print(f"⚠️ {symbol} 시세 데이터 없음")
                return False
                
        except Exception as e:
            print(f"❌ {symbol} 실시간 데이터 전송 오류: {e}")
            return False
    
    async def produce_all_realtime_data(self, interval_sec: float = 0.5, batch_size: int = 10):
        """
        모든 종목의 실시간 데이터 수집 및 전송
        
        Args:
            interval_sec: 요청 간격 (초)
            batch_size: 배치 크기
        """
        # 활성 심볼 조회
        symbols = self.db.get_active_symbols()
        
        print(f"🚀 총 {len(symbols)}개 종목의 실시간 데이터 수집 시작")
        
        while True:
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                
                # 각 배치에 대해 비동기 수집 작업 실행
                tasks = [self.produce_realtime_data(symbol) for symbol in batch]
                await asyncio.gather(*tasks)
                
                # 요청 간격 대기
                await asyncio.sleep(interval_sec)
            
            print(f"📊 {datetime.now().strftime('%H:%M:%S')} 전체 종목 수집 완료, 다음 사이클 대기 중...")
            await asyncio.sleep(5)  # 다음 사이클까지 대기
    
    def _get_symbol_tier(self, symbol: str) -> int:
        """
        종목의 티어 결정
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            티어 번호 (1: 대형주, 2: 중형주, 3: 소형주)
        """
        try:
            result = self.db.conn.execute("""
                SELECT market_cap FROM nasdaq_symbols
                WHERE symbol = ?
            """, (symbol,)).fetchone()
            
            if not result:
                return 3  # 기본값: 소형주
            
            market_cap = result[0]
            
            # 시가총액 문자열 처리 (예: $1.5B -> 1.5)
            if market_cap.startswith('$'):
                market_cap = market_cap[1:]
            
            market_cap_value = 0.0
            try:
                if market_cap.endswith('B'):
                    market_cap_value = float(market_cap[:-1])
                elif market_cap.endswith('T'):
                    market_cap_value = float(market_cap[:-1]) * 1000
                elif market_cap.endswith('M'):
                    market_cap_value = float(market_cap[:-1]) / 1000
            except ValueError:
                market_cap_value = 0.0
            
            # 티어 결정
            if market_cap_value >= 100:  # 1000억 달러 이상: 대형주
                return 1
            elif market_cap_value >= 10:  # 100억 달러 이상: 중형주
                return 2
            else:  # 그 외: 소형주
                return 3
                
        except Exception:
            return 3  # 오류 발생 시 기본값: 소형주
    
    def close(self):
        """리소스 정리"""
        self.producer.close()
        self.db.close()

# 메인 함수
async def main():
    """메인 함수"""
    producer = RealtimeStockProducer()
    
    try:
        # Ctrl+C로 종료할 수 있도록 함
        await producer.produce_all_realtime_data()
    except KeyboardInterrupt:
        print("\n📢 프로그램 종료 요청 감지")
    finally:
        producer.close()
        print("📢 프로그램 종료")

# 엔트리 포인트
if __name__ == "__main__":
    # 비동기 이벤트 루프 실행
    asyncio.run(main())
