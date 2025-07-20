#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime
from typing import Dict, Any, List

class NasdaqSymbolCollector:
    """나스닥 심볼 수집 클래스"""
    
    def __init__(self):
        """나스닥 심볼 수집 클래스 초기화"""
        self.nasdaq_base_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=false&limit=25&download=true"
        self.user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    
    def collect_symbols(self) -> List[Dict[str, Any]]:
        """
        나스닥 전체 종목 수집
        
        Returns:
            수집된 종목 정보 리스트
        """
        headers = {'User-Agent': self.user_agent}
        
        try:
            response = requests.get(self.nasdaq_base_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'rows' in data['data']:
                    stocks = data['data']['rows']
                    print(f"✅ {len(stocks)}개의 종목 수집 완료")
                    return stocks
                else:
                    print("❌ 응답 데이터 형식 오류")
            else:
                print(f"❌ API 요청 실패: {response.status_code}")
        except Exception as e:
            print(f"❌ 심볼 수집 오류: {e}")
        
        return []
    
    def filter_symbols(self, symbols: List[Dict[str, Any]], min_market_cap: float = 1.0) -> List[Dict[str, Any]]:
        """
        시가총액 기준으로 종목 필터링
        
        Args:
            symbols: 종목 정보 리스트
            min_market_cap: 최소 시가총액 (십억 달러)
            
        Returns:
            필터링된 종목 정보 리스트
        """
        filtered_symbols = []
        debug_count = 0
        
        for symbol_data in symbols:
            # 첫 10개 종목에 대해 디버깅 정보 출력
            if debug_count < 10:
                print(f"🔍 종목 {debug_count + 1}: {symbol_data}")
                debug_count += 1
            
            # 시가총액 문자열 처리 (예: $1.5B -> 1.5)
            market_cap_str = symbol_data.get('marketCap', '0')
            original_market_cap = market_cap_str
            
            if market_cap_str and market_cap_str.startswith('$'):
                market_cap_str = market_cap_str[1:]
            
            market_cap_value = 0.0
            try:
                if market_cap_str and market_cap_str.endswith('B'):
                    market_cap_value = float(market_cap_str[:-1])
                elif market_cap_str and market_cap_str.endswith('T'):
                    market_cap_value = float(market_cap_str[:-1]) * 1000
                elif market_cap_str and market_cap_str.endswith('M'):
                    market_cap_value = float(market_cap_str[:-1]) / 1000
                elif market_cap_str and market_cap_str.replace('.', '').replace(',', '').isdigit():
                    # 숫자만 있는 경우 (단위 없음)
                    market_cap_value = float(market_cap_str.replace(',', '')) / 1000000000  # 십억달러로 변환
            except (ValueError, AttributeError):
                market_cap_value = 0.0
                
            # 디버깅: 첫 10개 종목의 시가총액 변환 과정 출력
            if debug_count <= 10:
                print(f"📊 시가총액 변환: '{original_market_cap}' -> {market_cap_value}B")
            
            # 최소 시가총액 이상인 종목만 필터링
            if market_cap_value >= min_market_cap:
                filtered_symbols.append(symbol_data)
        
        print(f"🔍 {len(filtered_symbols)}개의 종목 필터링 완료 (최소 시가총액: ${min_market_cap}B)")
        return filtered_symbols
