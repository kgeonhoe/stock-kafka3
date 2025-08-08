#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
관심종목 데이터 조회 및 관리 모듈
PostgreSQL에서 daily_watchlist 테이블 데이터를 조회하고 Redis와 동기화
"""

import pandas as pd
import psycopg2
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class WatchlistManager:
    """관심종목 데이터 관리 클래스"""
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        
    def get_connection(self):
        """PostgreSQL 연결"""
        try:
            conn = psycopg2.connect(
                host=self.db_config.get('host', 'localhost'),
                port=self.db_config.get('port', 5433),
                database=self.db_config.get('database', 'stock_data'),
                user=self.db_config.get('user', 'stock_user'),
                password=self.db_config.get('password', 'stock_password')
            )
            return conn
        except Exception as e:
            logger.error(f"PostgreSQL 연결 실패: {e}")
            raise
    
    def get_watchlist_with_trigger_prices(self, days_back: int = 30) -> List[Dict]:
        """
        관심종목 목록과 트리거 가격 정보 조회
        
        Args:
            days_back: 조회할 기간 (일)
            
        Returns:
            관심종목 정보 리스트 (symbol, 등록날짜, trigger_price, condition 포함)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 관심종목과 해당 날짜의 주식 가격 데이터를 조인해서 조회
            query = """
            WITH latest_watchlist AS (
                SELECT 
                    dw.symbol,
                    dw.date as registered_date,
                    dw.condition_type,
                    dw.condition_value,
                    dw.market_cap_tier,
                    dw.created_at,
                    ROW_NUMBER() OVER (PARTITION BY dw.symbol ORDER BY dw.date DESC) as rn
                FROM daily_watchlist dw
                WHERE dw.date >= CURRENT_DATE - INTERVAL '%s days'
            ),
            trigger_prices AS (
                SELECT 
                    lw.symbol,
                    lw.registered_date,
                    lw.condition_type,
                    lw.condition_value,
                    lw.market_cap_tier,
                    lw.created_at,
                    COALESCE(sd.close, sd.open, 0) as trigger_price,
                    sd.volume as registered_day_volume
                FROM latest_watchlist lw
                LEFT JOIN stock_data sd ON lw.symbol = sd.symbol AND lw.registered_date = sd.date
                WHERE lw.rn = 1
            )
            SELECT 
                symbol,
                registered_date,
                condition_type,
                condition_value,
                market_cap_tier,
                created_at,
                trigger_price,
                registered_day_volume
            FROM trigger_prices
            ORDER BY created_at DESC
            """
            
            cursor.execute(query, (days_back,))
            results = cursor.fetchall()
            
            # 결과를 딕셔너리 리스트로 변환
            watchlist_data = []
            for row in results:
                symbol, registered_date, condition_type, condition_value, market_cap_tier, created_at, trigger_price, volume = row
                
                watchlist_data.append({
                    'symbol': symbol,
                    'registered_date': registered_date.isoformat() if registered_date else None,
                    'condition_type': condition_type,
                    'condition_value': float(condition_value) if condition_value else None,
                    'market_cap_tier': market_cap_tier,
                    'created_at': created_at.isoformat() if created_at else None,
                    'trigger_price': float(trigger_price) if trigger_price else None,
                    'registered_day_volume': int(volume) if volume else None,
                    'days_held': (datetime.now().date() - registered_date).days if registered_date else None
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"관심종목 {len(watchlist_data)}개 조회 완료")
            return watchlist_data
            
        except Exception as e:
            logger.error(f"관심종목 조회 실패: {e}")
            return []
    
    def get_current_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        주식 심볼들의 최신 가격 조회
        
        Args:
            symbols: 조회할 주식 심볼 리스트
            
        Returns:
            {symbol: current_price} 딕셔너리
        """
        if not symbols:
            return {}
            
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # 각 심볼의 최신 가격 조회
            placeholders = ','.join(['%s'] * len(symbols))
            query = f"""
            SELECT DISTINCT ON (symbol) 
                symbol, 
                close as current_price,
                date as price_date
            FROM stock_data 
            WHERE symbol IN ({placeholders})
            ORDER BY symbol, date DESC
            """
            
            cursor.execute(query, symbols)
            results = cursor.fetchall()
            
            current_prices = {}
            for symbol, price, price_date in results:
                current_prices[symbol] = float(price) if price else None
            
            cursor.close()
            conn.close()
            
            logger.info(f"현재 가격 {len(current_prices)}개 조회 완료")
            return current_prices
            
        except Exception as e:
            logger.error(f"현재 가격 조회 실패: {e}")
            return {}
    
    def calculate_watchlist_performance(self) -> List[Dict]:
        """
        관심종목 성과 계산
        
        Returns:
            성과 정보가 포함된 관심종목 리스트
        """
        # 관심종목 데이터 조회
        watchlist_data = self.get_watchlist_with_trigger_prices()
        
        if not watchlist_data:
            return []
        
        # 현재 가격 조회
        symbols = [item['symbol'] for item in watchlist_data]
        current_prices = self.get_current_prices(symbols)
        
        # 성과 계산
        performance_data = []
        for item in watchlist_data:
            symbol = item['symbol']
            trigger_price = item['trigger_price']
            current_price = current_prices.get(symbol)
            
            # 성과 계산
            if trigger_price and current_price and trigger_price > 0:
                price_change = current_price - trigger_price
                price_change_pct = (price_change / trigger_price) * 100
            else:
                price_change = 0
                price_change_pct = 0
            
            # 조건 타입을 한글로 변환
            condition_names = {
                'bollinger_upper_touch': '볼린저 밴드 상단 터치',
                'bollinger_lower_touch': '볼린저 밴드 하단 터치',
                'rsi_overbought': 'RSI 과매수 (>70)',
                'rsi_oversold': 'RSI 과매도 (<30)',
                'macd_bullish': 'MACD 상승 신호',
                'macd_bearish': 'MACD 하락 신호',
                'volume_spike': '거래량 급증',
                'price_breakout': '가격 돌파'
            }
            
            performance_item = {
                **item,  # 기존 데이터 포함
                'current_price': current_price,
                'price_change': price_change,
                'price_change_pct': price_change_pct,
                'condition_name': condition_names.get(item['condition_type'], item['condition_type']),
                'status': '🟢 수익' if price_change_pct > 0 else '🔴 손실' if price_change_pct < 0 else '⚪ 보합'
            }
            
            performance_data.append(performance_item)
        
        # 수익률 순으로 정렬
        performance_data.sort(key=lambda x: x['price_change_pct'], reverse=True)
        
        return performance_data
    
    def sync_to_redis(self, redis_client, performance_data: List[Dict]):
        """
        관심종목 성과 데이터를 Redis에 동기화
        
        Args:
            redis_client: Redis 클라이언트 객체
            performance_data: 성과 데이터 리스트
        """
        try:
            # Redis에 관심종목 데이터 저장
            redis_key = "watchlist:performance"
            redis_data = {
                'updated_at': datetime.now().isoformat(),
                'total_count': len(performance_data),
                'data': performance_data
            }
            
            # JSON으로 직렬화해서 저장 (24시간 TTL)
            redis_client.setex(
                redis_key, 
                86400,  # 24시간
                json.dumps(redis_data, ensure_ascii=False, default=str)
            )
            
            # 개별 심볼별로도 저장
            for item in performance_data:
                symbol_key = f"watchlist:symbol:{item['symbol']}"
                redis_client.setex(symbol_key, 86400, json.dumps(item, ensure_ascii=False, default=str))
            
            logger.info(f"관심종목 성과 데이터 {len(performance_data)}개를 Redis에 동기화 완료")
            
        except Exception as e:
            logger.error(f"Redis 동기화 실패: {e}")

def get_default_db_config() -> Dict:
    """기본 DB 설정 반환"""
    return {
        'host': 'localhost',
        'port': 5433,
        'database': 'stock_data',
        'user': 'stock_user',
        'password': 'stock_password'
    }

if __name__ == "__main__":
    # 테스트 실행
    db_config = get_default_db_config()
    manager = WatchlistManager(db_config)
    
    # 관심종목 성과 계산
    performance_data = manager.calculate_watchlist_performance()
    
    print(f"관심종목 {len(performance_data)}개 조회됨")
    for item in performance_data[:5]:  # 상위 5개만 출력
        print(f"{item['symbol']}: {item['price_change_pct']:+.2f}% ({item['condition_name']})")
