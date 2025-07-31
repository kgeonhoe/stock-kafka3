"""
Redis Client for Caching
"""

import redis
import json
from datetime import datetime
from typing import Any, Optional
from config.database_config import DatabaseConfig

class RedisClient:
    """Redis 클라이언트 래퍼"""
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host=DatabaseConfig.REDIS_HOST,
            port=DatabaseConfig.REDIS_PORT,
            db=DatabaseConfig.REDIS_DB,
            password=DatabaseConfig.REDIS_PASSWORD,
            decode_responses=True
        )
        self.prefix = DatabaseConfig.REDIS_KEY_PREFIX
        self.ttl = DatabaseConfig.REDIS_TTL
    
    def set_realtime_price(self, symbol: str, price_data: dict) -> bool:
        """실시간 가격 캐시"""
        key = f"{self.prefix['realtime_price']}{symbol}"
        return self.redis_client.setex(
            key, 
            self.ttl['realtime_price'], 
            json.dumps(price_data)
        )
    
    def get_realtime_price(self, symbol: str) -> Optional[dict]:
        """실시간 가격 조회"""
        key = f"{self.prefix['realtime_price']}{symbol}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None
    
    def set_technical_indicators(self, symbol: str, indicators: dict) -> bool:
        """기술적 지표 캐시"""
        key = f"{self.prefix['technical']}{symbol}"
        return self.redis_client.setex(
            key,
            self.ttl['technical'],
            json.dumps(indicators)
        )
    
    def get_technical_indicators(self, symbol: str) -> Optional[dict]:
        """기술적 지표 조회"""
        key = f"{self.prefix['technical']}{symbol}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None
    
    def increment_api_call(self, api_name: str) -> int:
        """API 호출 횟수 증가 (Rate Limiting)"""
        key = f"{self.prefix['rate_limit']}{api_name}"
        pipe = self.redis_client.pipeline()
        pipe.incr(key)
        pipe.expire(key, self.ttl['rate_limit'])
        result = pipe.execute()
        return result[0]
    
    def get_api_call_count(self, api_name: str) -> int:
        """API 호출 횟수 조회"""
        key = f"{self.prefix['rate_limit']}{api_name}"
        count = self.redis_client.get(key)
        return int(count) if count else 0
    
    # =========================
    # 관심종목 전략 관련 메서드
    # =========================
    
    def set_watchlist_data(self, symbol: str, historical_data: list, metadata: dict) -> bool:
        """관심종목 히스토리컬 데이터 저장 (30일 과거 데이터)"""
        key = f"watchlist_data:{symbol}"
        data = {
            'symbol': symbol,
            'historical_data': historical_data,  # 30일 OHLCV 데이터
            'metadata': metadata,  # 시가총액, 섹터 등
            'updated_at': datetime.now().isoformat()
        }
        return self.redis_client.setex(
            key,
            86400 * 7,  # 7일 TTL
            json.dumps(data, default=str)
        )
    
    def get_watchlist_data(self, symbol: str) -> Optional[dict]:
        """관심종목 히스토리컬 데이터 조회"""
        key = f"watchlist_data:{symbol}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None
    
    def set_signal_trigger(self, symbol: str, signal_data: dict) -> bool:
        """기술적 신호 발생 기록"""
        key = f"signal_trigger:{symbol}:{signal_data['timestamp']}"
        signal_info = {
            'symbol': symbol,
            'signal_type': signal_data['signal_type'],  # 'bollinger_upper_touch', 'rsi_oversold' 등
            'trigger_price': signal_data['trigger_price'],
            'trigger_time': signal_data['timestamp'],
            'technical_values': signal_data.get('technical_values', {}),  # RSI, MACD, BB 값들
            'condition_met': True,
            'status': 'active'  # active, completed, expired
        }
        return self.redis_client.setex(
            key,
            86400 * 30,  # 30일 TTL
            json.dumps(signal_info, default=str)
        )
    
    def get_active_signals(self, symbol: str = None) -> list:
        """활성 신호 목록 조회"""
        if symbol:
            pattern = f"signal_trigger:{symbol}:*"
        else:
            pattern = "signal_trigger:*"
            
        keys = self.redis_client.keys(pattern)
        signals = []
        
        for key in keys:
            data = self.redis_client.get(key)
            if data:
                signal = json.loads(data)
                if signal.get('status') == 'active':
                    signals.append(signal)
        
        return signals
    
    def update_signal_performance(self, symbol: str, trigger_time: str, current_price: float) -> dict:
        """신호 성과 업데이트"""
        key = f"signal_trigger:{symbol}:{trigger_time}"
        signal_data = self.redis_client.get(key)
        
        if not signal_data:
            return {}
            
        signal = json.loads(signal_data)
        trigger_price = signal['trigger_price']
        
        # 성과 계산
        price_change = current_price - trigger_price
        price_change_pct = (price_change / trigger_price) * 100
        
        # 성과 정보 추가
        signal['current_price'] = current_price
        signal['price_change'] = price_change
        signal['price_change_pct'] = price_change_pct
        signal['last_updated'] = datetime.now().isoformat()
        
        # 업데이트된 데이터 저장
        self.redis_client.setex(
            key,
            86400 * 30,
            json.dumps(signal, default=str)
        )
        
        return signal
    
    def set_realtime_analysis(self, symbol: str, analysis_data: dict) -> bool:
        """실시간 분석 결과 저장"""
        key = f"realtime_analysis:{symbol}"
        analysis_info = {
            'symbol': symbol,
            'timestamp': datetime.now().isoformat(),
            'current_price': analysis_data['current_price'],
            'technical_indicators': analysis_data['indicators'],  # RSI, MACD, BB 등
            'signals_detected': analysis_data.get('signals', []),
            'recommendation': analysis_data.get('recommendation', 'hold')
        }
        return self.redis_client.setex(
            key,
            300,  # 5분 TTL (실시간 데이터)
            json.dumps(analysis_info, default=str)
        )
    
    def get_realtime_analysis(self, symbol: str) -> Optional[dict]:
        """실시간 분석 결과 조회"""
        key = f"realtime_analysis:{symbol}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None