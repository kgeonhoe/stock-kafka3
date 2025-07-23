import os
import json
import asyncio
import aiohttp
import yfinance as yf
from datetime import datetime, timedelta
from kafka import KafkaProducer
from typing import Dict, Any, Optional
import time
import random

# 프로젝트 루트를 Python 경로에 추가
import sys
sys.path.append('/app')

from common.database import DuckDBManager
from config.kafka_config import KafkaConfig, DataSource


class KISAPIClient:
    """한국투자증권 API 클라이언트"""
    
    def __init__(self):
        self.app_key = os.getenv('KIS_APP_KEY')
        self.app_secret = os.getenv('KIS_APP_SECRET')
        self.base_url = os.getenv('KIS_BASE_URL', 'https://openapi.koreainvestment.com:9443')
        self.paper_trading = os.getenv('KIS_PAPER_TRADING', 'true').lower() == 'true'
        self.access_token = None
        self.token_expires_at = None
        
        if not self.app_key or not self.app_secret:
            print("⚠️ KIS API 키가 설정되지 않아 시뮬레이션 모드로 실행됩니다.")
            self.simulation_mode = True
        else:
            self.simulation_mode = False
            print(f"🔑 KIS API 연결 준비 (모의투자: {self.paper_trading})")
    
    async def get_access_token(self) -> Optional[str]:
        """액세스 토큰 발급"""
        if self.simulation_mode:
            return "SIMULATION_TOKEN"
            
        if self.access_token and self.token_expires_at and datetime.now() < self.token_expires_at:
            return self.access_token
        
        url = f"{self.base_url}/oauth2/tokenP"
        headers = {"Content-Type": "application/json"}
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.access_token = result.get('access_token')
                        expires_in = result.get('expires_in', 86400)  # 기본 24시간
                        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)  # 5분 여유
                        print("✅ KIS 액세스 토큰 발급 성공")
                        return self.access_token
                    else:
                        print(f"❌ KIS 토큰 발급 실패: {response.status}")
                        return None
        except Exception as e:
            print(f"❌ KIS 토큰 발급 오류: {e}")
            return None
    
    async def get_stock_price(self, symbol: str, fallback_client=None) -> Optional[Dict[str, Any]]:
        """실시간 주식 시세 조회 (yfinance fallback 지원)"""
        if self.simulation_mode:
            # 시뮬레이션 모드에서도 폴백이 있으면 시도
            if fallback_client:
                print(f"🔄 {symbol}: KIS 시뮬레이션 모드, yfinance로 폴백")
                return await self._try_fallback(symbol, fallback_client)
            return self._generate_simulation_data(symbol)
        
        token = await self.get_access_token()
        if not token:
            if fallback_client:
                print(f"🔄 {symbol}: KIS 토큰 발급 실패, yfinance로 폴백")
                return await self._try_fallback(symbol, fallback_client)
            print(f"❌ {symbol}: KIS 토큰 발급 실패, 실제 데이터 없어 전송 취소")
            return None
        
        # KIS API에서 미국 주식 시세 조회
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS00000300"  # 해외주식 현재가 시세
        }
        
        params = {
            "AUTH": "",
            "EXCD": "NAS",  # 나스닥
            "SYMB": symbol
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        # 디버깅: 응답 데이터 로깅
                        if result.get('rt_cd') != '0':
                            print(f"🔍 {symbol} KIS API 오류 응답: {result}")
                        
                        # CRM과 ORCL에 대한 전체 응답 로깅
                        if symbol in ['CRM', 'ORCL']:
                            print(f"🔍 {symbol} KIS API 전체 응답: {result}")
                        
                        if result.get('rt_cd') == '0':  # 성공
                            output = result.get('output')
                            if output:
                                # CRM 종목에 대한 상세 디버깅
                                if symbol == 'CRM':
                                    print(f"🔍 CRM KIS API 응답: {output}")
                                
                                # 빈 문자열 또는 None 값 처리
                                last_price = output.get('last', '0') or '0'
                                diff_value = output.get('diff', '0') or '0'
                                rate_value = output.get('rate', '0') or '0'
                                tvol_value = output.get('tvol', '0') or '0'
                                
                                # 빈 문자열이면 기본값 사용
                                try:
                                    price = float(last_price) if last_price.strip() else 0.0
                                    change = float(diff_value) if diff_value.strip() else 0.0
                                    change_rate = float(rate_value) if rate_value.strip() else 0.0
                                    volume = int(float(tvol_value)) if tvol_value.strip() else 0
                                except (ValueError, AttributeError):
                                    print(f"⚠️ {symbol}: KIS API 데이터 파싱 오류")
                                    if fallback_client:
                                        print(f"🔄 {symbol}: yfinance로 폴백")
                                        return await self._try_fallback(symbol, fallback_client)
                                    print(f"❌ {symbol}: 폴백 없음, 실제 데이터 없어 전송 취소")
                                    return None
                                
                                # 가격이 0이면 fallback 시도, 폴백도 실패하면 None 반환
                                if price <= 0:
                                    print(f"⚠️ {symbol}: 가격 정보 없음 (price={price})")
                                    if fallback_client:
                                        print(f"🔄 {symbol}: yfinance로 폴백")
                                        return await self._try_fallback(symbol, fallback_client)
                                    print(f"❌ {symbol}: 폴백 없음, 실제 데이터 없어 전송 취소")
                                    return None
                                
                                return {
                                    'symbol': symbol,
                                    'price': round(price, 2),
                                    'change': round(change, 2),
                                    'change_rate': round(change_rate, 2),
                                    'volume': volume,
                                    'data_source': DataSource.KIS.value,
                                    'timestamp': datetime.now().isoformat()
                                }
                    
                    print(f"⚠️ KIS API 응답 오류: {symbol}")
                    if fallback_client:
                        print(f"🔄 {symbol}: yfinance로 폴백")
                        return await self._try_fallback(symbol, fallback_client)
                    print(f"❌ {symbol}: 폴백 없음, 실제 데이터 없어 전송 취소")
                    return None
                    
        except Exception as e:
            print(f"❌ KIS API 호출 오류 ({symbol}): {e}")
            if fallback_client:
                print(f"🔄 {symbol}: yfinance로 폴백")
                return await self._try_fallback(symbol, fallback_client)
            print(f"❌ {symbol}: 폴백 없음, 실제 데이터 없어 전송 취소")
            return None
    
    async def _try_fallback(self, symbol: str, fallback_client) -> Optional[Dict[str, Any]]:
        """yfinance 폴백 시도 (실시간 데이터만 사용, 시뮬레이션 없음)"""
        try:
            # yfinance 데이터를 KIS 형식으로 변환
            yf_data = await fallback_client.get_stock_data(symbol)
            if yf_data and yf_data.get('current_price', 0) > 0:
                # yfinance 데이터를 KIS 형식으로 변환
                price = yf_data['current_price']
                previous_close = yf_data.get('previous_close', price)
                change = price - previous_close
                change_rate = (change / previous_close * 100) if previous_close > 0 else 0
                
                return {
                    'symbol': symbol,
                    'price': round(price, 2),
                    'change': round(change, 2),
                    'change_rate': round(change_rate, 2),
                    'volume': yf_data.get('volume', 0),
                    'data_source': f"{DataSource.KIS.value}_via_yfinance",  # 폴백임을 표시
                    'timestamp': datetime.now().isoformat()
                }
            else:
                print(f"❌ {symbol}: yfinance 폴백도 실패, 실제 데이터 없음")
                return None
        except Exception as e:
            print(f"❌ {symbol}: yfinance 폴백 오류 ({e}), 실제 데이터 없음")
            return None
    
    def _generate_simulation_data(self, symbol: str) -> Dict[str, Any]:
        """KIS 시뮬레이션 데이터 생성"""
        return {
            'symbol': symbol,
            'price': round(100 + hash(symbol + str(datetime.now().minute)) % 400, 2),
            'change': round((hash(symbol) % 20) - 10, 2),
            'change_rate': round(((hash(symbol) % 20) - 10) / 100, 2),
            'volume': (hash(symbol) % 1000000) + 100000,
            'data_source': DataSource.KIS.value,
            'timestamp': datetime.now().isoformat()
        }


class YFinanceClient:
    """yfinance API 클라이언트 (요청 제한 고려)"""
    
    def __init__(self):
        self.last_request_time = {}
        self.min_interval = 1.0  # 최소 1초 간격
        self.max_retries = 3
        self.cache = {}
        self.cache_duration = 30  # 30초 캐시
        
    async def get_stock_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """yfinance 데이터 조회 (캐시 및 요청 제한 고려)"""
        
        # 캐시 확인
        cache_key = f"{symbol}_{int(time.time() // self.cache_duration)}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # 요청 간격 제어
        now = time.time()
        if symbol in self.last_request_time:
            elapsed = now - self.last_request_time[symbol]
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
        
        # 재시도 로직
        for attempt in range(self.max_retries):
            try:
                # yfinance는 동기 함수이므로 스레드에서 실행
                ticker = yf.Ticker(symbol)
                
                # 더 안정적인 방법으로 데이터 가져오기
                try:
                    # 먼저 info 시도
                    info = ticker.info
                    current_price = info.get('currentPrice') or info.get('regularMarketPrice')
                    
                    if not current_price:
                        # info가 실패하면 history 시도
                        hist = ticker.history(period="1d")
                        if not hist.empty:
                            current_price = hist['Close'].iloc[-1]
                            volume = hist['Volume'].iloc[-1]
                            open_price = hist['Open'].iloc[-1]
                            high_price = hist['High'].iloc[-1]
                            low_price = hist['Low'].iloc[-1]
                        else:
                            print(f"⚠️ {symbol}: history 데이터 비어있음")
                            continue
                    else:
                        # info에서 가져온 경우
                        volume = info.get('volume', 0)
                        open_price = info.get('open', current_price)
                        high_price = info.get('dayHigh', current_price)
                        low_price = info.get('dayLow', current_price)
                    
                    previous_close = info.get('previousClose', current_price)
                    market_cap = info.get('marketCap', 0)
                    
                except Exception as e:
                    print(f"⚠️ {symbol}: info/history 실패, download 시도... ({e})")
                    # download 메서드 시도
                    df = yf.download(symbol, period="1d", progress=False)
                    if df.empty:
                        print(f"❌ {symbol}: download도 실패")
                        continue
                        
                    current_price = float(df['Close'].iloc[-1])
                    volume = int(df['Volume'].iloc[-1])
                    open_price = float(df['Open'].iloc[-1])
                    high_price = float(df['High'].iloc[-1])
                    low_price = float(df['Low'].iloc[-1])
                    previous_close = current_price  # 추정
                    market_cap = 0
                
                data = {
                    'symbol': symbol,
                    'current_price': round(float(current_price), 2),
                    'previous_close': round(float(previous_close), 2),
                    'open_price': round(float(open_price), 2),
                    'day_high': round(float(high_price), 2),
                    'day_low': round(float(low_price), 2),
                    'volume': int(volume),
                    'market_cap': int(market_cap) if market_cap else 0,
                    'pe_ratio': 0,
                    'data_source': DataSource.YFINANCE.value,
                    'timestamp': datetime.now().isoformat()
                }
                
                self.last_request_time[symbol] = time.time()
                self.cache[cache_key] = data
                return data
                
            except Exception as e:
                print(f"❌ yfinance {symbol} 시도 {attempt + 1} 실패: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    print(f"⏳ {wait_time:.1f}초 후 재시도...")
                    await asyncio.sleep(wait_time)
                else:
                    # 최종 실패시 None 반환 (시뮬레이션 데이터 사용 안함)
                    print(f"❌ {symbol}: 모든 yfinance 시도 실패, 실제 데이터 없음")
                    return None
    
    def _generate_simulation_data(self, symbol: str) -> Dict[str, Any]:
        """yfinance 시뮬레이션 데이터 생성"""
        base_price = 150 + hash(symbol) % 300
        current_time = datetime.now()
        price_variation = hash(symbol + str(current_time.minute)) % 20 - 10
        current_price = base_price + price_variation
        previous_close = current_price - (hash(symbol) % 10 - 5)
        
        return {
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


class MultiSourceStockProducer:
    """다중 소스 주식 데이터 프로듀서"""
    #TODO 레플리카  DB에서 
    def __init__(self, bootstrap_servers: str = None):
        # Kafka 연결 설정
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        
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
                    request_timeout_ms=30000,
                    api_version_auto_timeout_ms=30000
                )
                print(f"✅ Kafka 연결 성공: {bootstrap_servers}")
                break
            except Exception as e:
                print(f"❌ Kafka 연결 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"⏳ {retry_delay}초 후 재시도...")
                    time.sleep(retry_delay)
                else:
                    raise Exception("Kafka 연결에 실패했습니다.")
        
        # API 클라이언트 초기화
        self.kis_client = KISAPIClient()
        self.yfinance_client = YFinanceClient()
        
        # 주식 심볼 목록
        self.nasdaq_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
            'ADBE', 'CRM', 'ORCL', 'INTC', 'AMD', 'QCOM', 'AVGO', 'TXN'
        ]
        
        print(f"📊 {len(self.nasdaq_symbols)}개 종목 실시간 데이터 수집 준비 완료")
    
    async def produce_kis_data(self, symbol: str) -> bool:
        """KIS API 데이터 수집 및 전송 (yfinance 폴백 지원)"""
        try:
            # yfinance 클라이언트를 폴백으로 전달
            data = await self.kis_client.get_stock_price(symbol, fallback_client=self.yfinance_client)
            if data:
                future = self.producer.send(KafkaConfig.TOPIC_KIS_STOCK, data)
                record_metadata = future.get(timeout=10)
                
                # 폴백 사용 여부에 따라 다른 메시지 출력
                if data['data_source'] == f"{DataSource.KIS.value}_via_yfinance":
                    print(f"📈 KIS(📡yfinance) {symbol}: ${data['price']} → {KafkaConfig.TOPIC_KIS_STOCK}")
                else:
                    print(f"📈 KIS {symbol}: ${data['price']} → {KafkaConfig.TOPIC_KIS_STOCK}")
                return True
            else:
                print(f"⏭️ {symbol}: 실제 데이터 없어 전송 건너뜀")
                return False
        except Exception as e:
            print(f"❌ KIS {symbol} 오류: {e}")
            return False
    
    async def produce_yfinance_data(self, symbol: str) -> bool:
        """yfinance API 데이터 수집 및 전송"""
        try:
            data = await self.yfinance_client.get_stock_data(symbol)
            if data:
                future = self.producer.send(KafkaConfig.TOPIC_YFINANCE_STOCK, data)
                record_metadata = future.get(timeout=10)
                print(f"📊 yfinance {symbol}: ${data['current_price']} → {KafkaConfig.TOPIC_YFINANCE_STOCK}")
                return True
            else:
                print(f"⏭️ {symbol}: yfinance 실제 데이터 없어 전송 건너뜀")
                return False
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
                    
                    await asyncio.sleep(2)  # API 제한 고려하여 2초 간격
                
                print(f"✅ 사이클 {cycle_count} 완료, 다음 사이클까지 10초 대기...")
                await asyncio.sleep(10)  # 사이클 간 10초 대기
                
            except KeyboardInterrupt:
                print(f"\n🛑 사용자 중단 요청 (총 {cycle_count}개 사이클 완료)")
                break
            except Exception as e:
                print(f"❌ 메인 루프 오류: {e}")
                await asyncio.sleep(30)  # 오류 발생시 30초 대기
    
    async def test_single_messages(self) -> bool:
        """단일 메시지 테스트 (폴백 지원)"""
        print("🧪 실제 API 테스트 시작...")
        
        test_symbol = "AAPL"
        
        print(f"\n📤 {test_symbol} 실제 데이터를 두 토픽에 전송...")
        
        # KIS 토픽에 전송 (yfinance 폴백 지원)
        success1 = await self.produce_kis_data(test_symbol)
        await asyncio.sleep(2)  # API 제한 고려
        
        # yfinance 토픽에 전송  
        success2 = await self.produce_yfinance_data(test_symbol)
        
        if success1 and success2:
            print("✅ 실제 데이터 테스트 성공!")
            return True
        else:
            print("❌ 실제 데이터 테스트 실패!")
            return False
    
    def close(self):
        """리소스 정리"""
        self.producer.flush()
        self.producer.close()
        print("🔒 프로듀서 연결 종료")


async def main():
    """메인 실행 함수"""
    producer = None
    try:
        print("🚀 다중 소스 주식 데이터 프로듀서 시작")
        
        # 프로듀서 초기화
        producer = MultiSourceStockProducer()
        
        # 단일 테스트 실행
        test_success = await producer.test_single_messages()
        
        if test_success:
            print("\n🎯 실제 데이터 스트리밍 시작...")
            await producer.produce_all_data()
        else:
            print("❌ 단일 테스트 실패로 인해 실시간 스트리밍을 시작하지 않습니다.")
            
    except Exception as e:
        print(f"❌ 프로그램 실행 오류: {e}")
    finally:
        if producer:
            producer.close()
        print("📢 프로그램 종료")


if __name__ == "__main__":
    asyncio.run(main())