import yfinance as yf
import asyncio
from datetime import datetime
from kafka import KafkaProducer
from typing import Dict, Any, List, Optional
from enum import Enum
import json
import sys
import os
# 컨테이너 환경에서는 /app이 기본 경로이므로 Python 경로에 추가
sys.path.insert(0, '/app')

from config.kafka_config import KafkaConfig

class DataSource(Enum):
    KIS = "kis"
    YFINANCE = "yfinance"

class MultiSourceStockProducer:
    """
    
    테스트용 다중 소스 실시간 주식 데이터 프로듀서 (랜덤값 생성)
    
    
    """
    
    def __init__(self, bootstrap_servers: str = None):
        # 환경변수에서 Kafka 서버 주소를 가져오거나 기본값 사용
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        print(f"🔗 Kafka 서버 연결 시도: {bootstrap_servers}")
        
        # Kafka 연결 재시도 로직
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    compression_type='gzip',
                    acks='all',
                    retries=3,
                    request_timeout_ms=10000,
                    api_version_auto_timeout_ms=10000
                )
                print(f"✅ Kafka 연결 성공: {bootstrap_servers}")
                break
            except Exception as e:
                print(f"❌ Kafka 연결 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"⏳ {retry_delay}초 후 재시도...")
                    import time
                    time.sleep(retry_delay)
                else:
                    raise e
                    
        #TODO 나스닥 종목 리스트는 일배치 TRACKING데이터로 가져옴
        # 나스닥 종목 리스트
        self.nasdaq_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
            'META', 'NVDA', 'NFLX', 'AMD', 'INTC'
        ]
        
        print(f"✅ 다중 소스 프로듀서 초기화 완료: {bootstrap_servers}")
    
    async def produce_kis_data(self, symbol: str):
        """KIS 시뮬레이션 데이터 수집"""
        try:
            # KIS 시뮬레이션 데이터 생성
            quote = {
                'symbol': symbol,
                'price': round(100 + hash(symbol + str(datetime.now().minute)) % 400, 2),
                'change': round((hash(symbol) % 20) - 10, 2),
                'change_rate': round(((hash(symbol) % 20) - 10) / 100, 2),
                'volume': (hash(symbol) % 1000000) + 100000,
                'data_source': DataSource.KIS.value,
                'timestamp': datetime.now().isoformat()
            }
            
            # KIS 토픽으로 전송
            future = self.producer.send(KafkaConfig.TOPIC_KIS_STOCK, quote)
            record_metadata = future.get(timeout=10)
            
            print(f"📈 KIS {symbol}: ${quote['price']} → {KafkaConfig.TOPIC_KIS_STOCK}")
            return True
            
        except Exception as e:
            print(f"❌ KIS {symbol} 오류: {e}")
            return False
    
    async def produce_yfinance_data(self, symbol: str):
        """yfinance 시뮬레이션 데이터 수집 (API 제한으로 인한 시뮬레이션)"""
        try:
            # yfinance 시뮬레이션 데이터 생성 (실제 API 대신)
            base_price = 150 + hash(symbol) % 300  # 150-450 범위
            current_time = datetime.now()
            
            # 시간에 따른 가격 변동 시뮬레이션
            price_variation = hash(symbol + str(current_time.minute)) % 20 - 10
            current_price = base_price + price_variation
            previous_close = current_price - (hash(symbol) % 10 - 5)
            
            quote = {
                'symbol': symbol,
                'current_price': round(current_price, 2),
                'previous_close': round(previous_close, 2),
                'open_price': round(previous_close + (hash(symbol) % 6 - 3), 2),
                'day_high': round(current_price + abs(hash(symbol) % 5), 2),
                'day_low': round(current_price - abs(hash(symbol) % 5), 2),
                'volume': (hash(symbol) % 10000000) + 1000000,
                'market_cap': (hash(symbol) % 1000000000000) + 100000000000,
                'pe_ratio': round((hash(symbol) % 30) + 10, 2),
                'data_source': DataSource.YFINANCE.value,
                'timestamp': current_time.isoformat()
            }
            
            # yfinance 토픽으로 전송
            future = self.producer.send(KafkaConfig.TOPIC_YFINANCE_STOCK, quote)
            record_metadata = future.get(timeout=10)
            
            print(f"📊 yfinance(sim) {symbol}: ${quote['current_price']} → {KafkaConfig.TOPIC_YFINANCE_STOCK}")
            return True
            
        except Exception as e:
            print(f"❌ yfinance {symbol} 오류: {e}")
            return False
    
    async def produce_all_data(self):
        """전체 데이터 수집 메인 루프"""
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                print(f"\n🔄 사이클 {cycle_count} 시작 - {datetime.now().strftime('%H:%M:%S')}")
                
                # 각 종목에 대해 KIS와 yfinance 데이터를 번갈아 수집
                for i, symbol in enumerate(self.nasdaq_symbols):
                    if i % 2 == 0:
                        # 짝수 인덱스: KIS 데이터
                        await self.produce_kis_data(symbol)
                    else:
                        # 홀수 인덱스: yfinance 데이터
                        await self.produce_yfinance_data(symbol)
                    
                    await asyncio.sleep(0.5)  # 0.5초 간격
                
                print(f"✅ 사이클 {cycle_count} 완료, 다음 사이클까지 5초 대기...")
                await asyncio.sleep(5)  # 5초 대기
                
            except KeyboardInterrupt:
                print(f"\n🛑 사용자 중단 요청 (총 {cycle_count}개 사이클 완료)")
                break
            except Exception as e:
                print(f"❌ 메인 루프 오류: {e}")
                await asyncio.sleep(10)
    
    async def test_single_messages(self):
        """단일 메시지 테스트"""
        print("🧪 단일 메시지 테스트 시작...")
        
        test_symbol = "AAPL"
        
        print(f"\n📤 {test_symbol} 데이터를 두 토픽에 전송...")
        
        # KIS 토픽에 전송
        success1 = await self.produce_kis_data(test_symbol)
        await asyncio.sleep(1)
        
        # yfinance 토픽에 전송  
        success2 = await self.produce_yfinance_data(test_symbol)
        
        if success1 and success2:
            print("✅ 단일 메시지 테스트 성공!")
            return True
        else:
            print("❌ 단일 메시지 테스트 실패!")
            return False
    
    def close(self):
        """리소스 정리"""
        self.producer.flush()
        self.producer.close()
        print("🔒 프로듀서 연결 종료")

async def main():
    """메인 함수"""
    producer = MultiSourceStockProducer()
    
    try:
        print("=" * 60)
        print("📊 다중 소스 주식 데이터 프로듀서")
        print("=" * 60)
        print(f"🎯 KIS 토픽: {KafkaConfig.TOPIC_KIS_STOCK}")
        print(f"🎯 yfinance 토픽: {KafkaConfig.TOPIC_YFINANCE_STOCK}")
        print()
        
        # 단일 메시지 테스트
        test_success = await producer.test_single_messages()
        
        if test_success:
            print()
            # 컨테이너 환경에서는 input() 대신 바로 스트리밍 시작
            if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
                print("🐳 컨테이너 환경 감지 - 자동으로 실시간 스트리밍 시작...")
            else:
                input("📌 단일 테스트 완료! 실시간 스트리밍을 시작하려면 Enter를 누르세요 (Ctrl+C로 중단)...")
            print()
            
            # 실시간 데이터 스트리밍 시작
            await producer.produce_all_data()
        else:
            print("❌ 단일 테스트 실패로 인해 실시간 스트리밍을 시작하지 않습니다.")
            
    except KeyboardInterrupt:
        print("\n📢 프로그램 종료 요청 감지")
    finally:
        producer.close()
        print("📢 프로그램 종료")

if __name__ == "__main__":
    asyncio.run(main())