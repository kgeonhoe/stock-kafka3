"""
Redis Client for Caching
"""

import redis
import json
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