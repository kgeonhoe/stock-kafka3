#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
신호 감지 테스트용 실시간 데이터 Producer
관심종목들에 대해 시뮬레이션 데이터를 생성하여 신호 감지 테스트
"""

import sys
import json
import time
import random
from datetime import datetime, timedelta
import threading

# 프로젝트 경로 추가
sys.path.append('/app/common')

from kafka import KafkaProducer
from redis_client import RedisClient

class SignalTestProducer:
    """신호 감지 테스트용 Producer"""
    
    def __init__(self):
        self.redis = RedisClient()
        
        # Kafka Producer 설정
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.is_running = False
        print("🚀 신호 테스트 Producer 초기화 완료")
    
    def get_watchlist_symbols(self) -> list:
        """Redis에서 관심종목 목록 조회"""
        try:
            watchlist_keys = self.redis.redis_client.keys("watchlist_data:*")
            symbols = []
            
            for key in watchlist_keys:
                symbol = key.replace("watchlist_data:", "")
                symbols.append(symbol)
            
            print(f"📋 관심종목 {len(symbols)}개 발견: {symbols[:5]}...")
            return symbols
            
        except Exception as e:
            print(f"❌ 관심종목 조회 오류: {e}")
            # 테스트용 기본 심볼
            return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    def generate_realistic_price_data(self, symbol: str, base_price: float = None) -> dict:
        """현실적인 주가 데이터 생성"""
        
        if base_price is None:
            # 관심종목 데이터에서 최근 가격 가져오기
            watchlist_data = self.redis.get_watchlist_data(symbol)
            if watchlist_data and watchlist_data['historical_data']:
                base_price = watchlist_data['historical_data'][-1]['close']
            else:
                base_price = random.uniform(50, 500)  # 기본값
        
        # 가격 변동 (일반적으로 -2% ~ +2%)
        change_pct = random.gauss(0, 0.01)  # 평균 0, 표준편차 1%
        
        # 특별한 경우 큰 변동 생성 (신호 유발용)
        if random.random() < 0.1:  # 10% 확률로 큰 변동
            change_pct = random.choice([
                random.uniform(0.02, 0.05),   # +2% ~ +5% (볼린저 상단 터치용)
                random.uniform(-0.05, -0.02)  # -2% ~ -5% (과매도 신호용)
            ])
        
        new_price = base_price * (1 + change_pct)
        
        # 일일 변동 범위 계산
        daily_range = base_price * 0.02  # 2% 범위
        
        high = new_price + random.uniform(0, daily_range * 0.5)
        low = new_price - random.uniform(0, daily_range * 0.5)
        open_price = new_price + random.uniform(-daily_range * 0.3, daily_range * 0.3)
        
        volume = random.randint(100000, 5000000)
        
        return {
            'symbol': symbol,
            'price': round(new_price, 2),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(new_price, 2),
            'volume': volume,
            'timestamp': datetime.now().isoformat(),
            'change_pct': round(change_pct * 100, 2)
        }
    
    def generate_signal_trigger_data(self, symbol: str) -> dict:
        """신호 발생을 유도하는 특별한 데이터 생성"""
        
        # 관심종목 데이터에서 기준 가격 가져오기
        watchlist_data = self.redis.get_watchlist_data(symbol)
        if watchlist_data and watchlist_data['historical_data']:
            historical_prices = [d['close'] for d in watchlist_data['historical_data']]
            base_price = historical_prices[-1]
            
            # 볼린저 밴드 추정 (간단한 계산)
            recent_prices = historical_prices[-20:] if len(historical_prices) >= 20 else historical_prices
            mean_price = sum(recent_prices) / len(recent_prices)
            std_dev = (sum((p - mean_price) ** 2 for p in recent_prices) / len(recent_prices)) ** 0.5
            
            # 신호 타입 선택
            signal_type = random.choice(['bollinger_upper', 'bollinger_lower', 'momentum_up', 'momentum_down'])
            
            if signal_type == 'bollinger_upper':
                # 볼린저 밴드 상단 터치 유도
                trigger_price = mean_price + (std_dev * 2.1)  # 상단 밴드 초과
                change_pct = ((trigger_price - base_price) / base_price) * 100
                
            elif signal_type == 'bollinger_lower':
                # 볼린저 밴드 하단 터치 유도 (과매도)
                trigger_price = mean_price - (std_dev * 2.1)  # 하단 밴드 하회
                change_pct = ((trigger_price - base_price) / base_price) * 100
                
            elif signal_type == 'momentum_up':
                # 강한 상승 모멘텀
                trigger_price = base_price * 1.03  # +3%
                change_pct = 3.0
                
            else:  # momentum_down
                # 강한 하락 모멘텀
                trigger_price = base_price * 0.97  # -3%
                change_pct = -3.0
                
        else:
            # 기본값 사용
            base_price = random.uniform(100, 300)
            trigger_price = base_price * random.choice([1.03, 0.97])  # ±3%
            change_pct = ((trigger_price - base_price) / base_price) * 100
        
        # 가격 데이터 생성
        daily_range = base_price * 0.02
        
        return {
            'symbol': symbol,
            'price': round(trigger_price, 2),
            'open': round(base_price, 2),
            'high': round(max(trigger_price, base_price) + daily_range * 0.3, 2),
            'low': round(min(trigger_price, base_price) - daily_range * 0.3, 2),
            'close': round(trigger_price, 2),
            'volume': random.randint(500000, 10000000),  # 높은 거래량
            'timestamp': datetime.now().isoformat(),
            'change_pct': round(change_pct, 2),
            'signal_test': True  # 테스트 데이터 마크
        }
    
    def start_streaming(self, interval: float = 5.0, signal_probability: float = 0.2):
        """실시간 스트리밍 시작"""
        
        self.is_running = True
        symbols = self.get_watchlist_symbols()
        
        if not symbols:
            print("❌ 관심종목이 없습니다. 먼저 관심종목 데이터를 Redis에 로딩하세요.")
            return
        
        print(f"🚀 {len(symbols)}개 종목 실시간 스트리밍 시작")
        print(f"⏱️ 간격: {interval}초")
        print(f"🎯 신호 발생 확률: {signal_probability * 100}%")
        print("🛑 Ctrl+C로 중단")
        
        try:
            while self.is_running:
                for symbol in symbols:
                    try:
                        # 신호 발생 여부 결정
                        if random.random() < signal_probability:
                            # 신호 유발 데이터 생성
                            stock_data = self.generate_signal_trigger_data(symbol)
                            print(f"🚨 {symbol} 신호 테스트 데이터: ${stock_data['price']} ({stock_data['change_pct']:+.2f}%)")
                        else:
                            # 일반 데이터 생성
                            stock_data = self.generate_realistic_price_data(symbol)
                            print(f"📊 {symbol}: ${stock_data['price']} ({stock_data['change_pct']:+.2f}%)")
                        
                        # Kafka로 전송
                        self.producer.send('realtime-stock', stock_data)
                        
                        # 짧은 지연 (종목 간)
                        time.sleep(0.1)
                        
                    except Exception as e:
                        print(f"❌ {symbol} 데이터 생성 오류: {e}")
                
                # 플러시 및 대기
                self.producer.flush()
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n🛑 사용자 중단")
        except Exception as e:
            print(f"❌ 스트리밍 오류: {e}")
        finally:
            self.stop_streaming()
    
    def stop_streaming(self):
        """스트리밍 중단"""
        self.is_running = False
        self.producer.close()
        print("✅ Producer 종료")
    
    def test_single_signal(self, symbol: str):
        """단일 신호 테스트"""
        print(f"🧪 {symbol} 단일 신호 테스트")
        
        # 신호 유발 데이터 생성
        signal_data = self.generate_signal_trigger_data(symbol)
        
        print(f"📊 테스트 데이터: {json.dumps(signal_data, indent=2)}")
        
        # Kafka로 전송
        self.producer.send('realtime-stock', signal_data)
        self.producer.flush()
        
        print("✅ 테스트 데이터 전송 완료")

def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("🧪 Signal Detection Test Producer")
    print("=" * 60)
    
    producer = SignalTestProducer()
    
    import argparse
    parser = argparse.ArgumentParser(description='신호 감지 테스트 Producer')
    parser.add_argument('--interval', type=float, default=10.0, help='데이터 전송 간격 (초)')
    parser.add_argument('--signal-prob', type=float, default=0.3, help='신호 발생 확률 (0-1)')
    parser.add_argument('--test-symbol', type=str, help='단일 종목 신호 테스트')
    
    args = parser.parse_args()
    
    if args.test_symbol:
        # 단일 신호 테스트
        producer.test_single_signal(args.test_symbol)
    else:
        # 연속 스트리밍
        producer.start_streaming(
            interval=args.interval,
            signal_probability=args.signal_prob
        )

if __name__ == "__main__":
    main()
