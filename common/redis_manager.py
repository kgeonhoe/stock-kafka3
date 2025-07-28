#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import os

class RedisManager:
    """Redis 캐시 관리 클래스"""
    
    def __init__(self, host: str = None, port: int = None, db: int = 0):
        """
        Redis 연결 초기화
        
        Args:
            host: Redis 서버 호스트
            port: Redis 서버 포트  
            db: Redis 데이터베이스 번호
        """
        self.host = host or os.getenv('REDIS_HOST', 'redis')
        self.port = port or int(os.getenv('REDIS_PORT', 6379))
        self.db = db
        
        # Redis 연결
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    decode_responses=True,
                    socket_connect_timeout=10,
                    socket_timeout=10
                )
                
                # 연결 테스트
                self.redis_client.ping()
                print(f"✅ Redis 연결 성공: {self.host}:{self.port}")
                break
                
            except Exception as e:
                print(f"❌ Redis 연결 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(retry_delay)
                else:
                    raise Exception("Redis 연결에 실패했습니다.")
    
    def store_realtime_data(self, symbol: str, data: Dict[str, Any], expire_seconds: int = 300) -> bool:
        """
        실시간 주식 데이터 저장
        
        Args:
            symbol: 종목 심볼
            data: 실시간 데이터
            expire_seconds: 만료 시간 (초)
            
        Returns:
            저장 성공 여부
        """
        try:
            key = f"realtime:{symbol}"
            value = json.dumps(data, default=str)
            
            # 데이터 저장 및 만료 시간 설정
            self.redis_client.setex(key, expire_seconds, value)
            
            # 최근 업데이트 시간 기록
            self.redis_client.hset("last_update", symbol, datetime.now().isoformat())
            
            return True
            
        except Exception as e:
            print(f"❌ Redis 실시간 데이터 저장 실패 ({symbol}): {e}")
            return False
    
    def get_realtime_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        실시간 주식 데이터 조회
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            실시간 데이터 또는 None
        """
        try:
            key = f"realtime:{symbol}"
            value = self.redis_client.get(key)
            
            if value:
                return json.loads(value)
            return None
            
        except Exception as e:
            print(f"❌ Redis 실시간 데이터 조회 실패 ({symbol}): {e}")
            return None
    
    def store_technical_indicators(self, symbol: str, indicators: Dict[str, Any], expire_seconds: int = 3600) -> bool:
        """
        기술적 지표 저장
        
        Args:
            symbol: 종목 심볼
            indicators: 기술적 지표 데이터
            expire_seconds: 만료 시간 (초, 기본 1시간)
            
        Returns:
            저장 성공 여부
        """
        try:
            key = f"indicators:{symbol}"
            value = json.dumps(indicators, default=str)
            
            # 기술적 지표 저장
            self.redis_client.setex(key, expire_seconds, value)
            
            return True
            
        except Exception as e:
            print(f"❌ Redis 기술적 지표 저장 실패 ({symbol}): {e}")
            return False
    
    def get_technical_indicators(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        기술적 지표 조회
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            기술적 지표 데이터 또는 None
        """
        try:
            key = f"indicators:{symbol}"
            value = self.redis_client.get(key)
            
            if value:
                return json.loads(value)
            return None
            
        except Exception as e:
            print(f"❌ Redis 기술적 지표 조회 실패 ({symbol}): {e}")
            return None
    
    def store_watchlist_alert(self, symbol: str, alert_data: Dict[str, Any], expire_seconds: int = 86400) -> bool:
        """
        워치리스트 알림 저장 (24시간 유지)
        
        Args:
            symbol: 종목 심볼
            alert_data: 알림 데이터
            expire_seconds: 만료 시간 (기본 24시간)
            
        Returns:
            저장 성공 여부
        """
        try:
            key = f"alert:{symbol}"
            value = json.dumps(alert_data, default=str)
            
            self.redis_client.setex(key, expire_seconds, value)
            
            # 알림 목록에 추가
            self.redis_client.lpush("active_alerts", symbol)
            self.redis_client.ltrim("active_alerts", 0, 99)  # 최대 100개 유지
            
            return True
            
        except Exception as e:
            print(f"❌ Redis 워치리스트 알림 저장 실패 ({symbol}): {e}")
            return False
    
    def get_active_symbols(self) -> List[str]:
        """
        현재 활성 종목 목록 조회 (최근 5분 이내 업데이트된 종목)
        
        Returns:
            활성 종목 목록
        """
        try:
            # 5분 전 시간
            cutoff_time = datetime.now() - timedelta(minutes=5)
            
            # 모든 종목의 마지막 업데이트 시간 조회
            all_updates = self.redis_client.hgetall("last_update")
            active_symbols = []
            
            for symbol, last_update_str in all_updates.items():
                try:
                    last_update = datetime.fromisoformat(last_update_str)
                    if last_update > cutoff_time:
                        active_symbols.append(symbol)
                except:
                    continue
            
            return active_symbols
            
        except Exception as e:
            print(f"❌ Redis 활성 종목 조회 실패: {e}")
            return []
    
    def get_market_summary(self) -> Dict[str, Any]:
        """
        시장 요약 정보 조회
        
        Returns:
            시장 요약 정보
        """
        try:
            active_symbols = self.get_active_symbols()
            
            if not active_symbols:
                return {"message": "활성 데이터 없음"}
            
            # 각 종목의 실시간 데이터 수집
            total_volume = 0
            price_changes = []
            
            for symbol in active_symbols[:20]:  # 최대 20개만 처리
                data = self.get_realtime_data(symbol)
                if data:
                    total_volume += data.get('volume', 0)
                    
                    # 가격 변화율 수집
                    if 'change_rate' in data:
                        price_changes.append(data['change_rate'])
                    elif 'current_price' in data and 'previous_close' in data:
                        current = data['current_price']
                        previous = data['previous_close']
                        if previous > 0:
                            change_rate = ((current - previous) / previous) * 100
                            price_changes.append(change_rate)
            
            # 통계 계산
            summary = {
                'active_symbols_count': len(active_symbols),
                'total_volume': total_volume,
                'average_change_rate': round(sum(price_changes) / len(price_changes), 2) if price_changes else 0,
                'positive_count': len([x for x in price_changes if x > 0]),
                'negative_count': len([x for x in price_changes if x < 0]),
                'last_updated': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            print(f"❌ Redis 시장 요약 조회 실패: {e}")
            return {"error": str(e)}
    
    def cleanup_expired_data(self):
        """만료된 데이터 정리 (선택적 실행)"""
        try:
            # 5분 이상 업데이트되지 않은 종목들의 last_update 제거
            cutoff_time = datetime.now() - timedelta(minutes=5)
            all_updates = self.redis_client.hgetall("last_update")
            
            for symbol, last_update_str in all_updates.items():
                try:
                    last_update = datetime.fromisoformat(last_update_str)
                    if last_update < cutoff_time:
                        self.redis_client.hdel("last_update", symbol)
                except:
                    continue
                    
            print("🧹 Redis 만료 데이터 정리 완료")
            
        except Exception as e:
            print(f"❌ Redis 데이터 정리 실패: {e}")
    
    def close(self):
        """Redis 연결 종료"""
        try:
            if hasattr(self, 'redis_client'):
                self.redis_client.close()
            print("🔒 Redis 연결 종료")
        except:
            pass

# 테스트 함수
def test_redis_connection():
    """Redis 연결 테스트"""
    try:
        redis_manager = RedisManager()
        
        # 테스트 데이터 저장
        test_data = {
            'symbol': 'AAPL',
            'current_price': 150.25,
            'volume': 1000000,
            'timestamp': datetime.now().isoformat()
        }
        
        success = redis_manager.store_realtime_data('AAPL', test_data)
        print(f"✅ 테스트 데이터 저장: {success}")
        
        # 데이터 조회
        retrieved_data = redis_manager.get_realtime_data('AAPL')
        print(f"📊 조회된 데이터: {retrieved_data}")
        
        # 활성 종목 조회
        active_symbols = redis_manager.get_active_symbols()
        print(f"📈 활성 종목: {active_symbols}")
        
        redis_manager.close()
        return True
        
    except Exception as e:
        print(f"❌ Redis 테스트 실패: {e}")
        return False

if __name__ == "__main__":
    test_redis_connection()
