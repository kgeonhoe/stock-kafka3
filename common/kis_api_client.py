#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

class KISAPIClient:
    """한국투자증권 API 클라이언트"""
    
    def __init__(self, config_path: str = "/home/grey1/stock-kafka3/config/.env"):
        """
        한국투자증권 API 클라이언트 초기화
        
        Args:
            config_path: 환경설정 파일 경로
        """
        self.config = self._load_config(config_path)
        self.base_url = self.config.get('KIS_BASE_URL', 'https://openapi.koreainvestment.com:9443')
        self.app_key = self.config.get('KIS_APP_KEY', '')
        self.app_secret = self.config.get('KIS_APP_SECRET', '')
        self.account_no = self.config.get('KIS_ACCOUNT_NO', '')
        self.token = self._get_access_token()
    
    def _load_config(self, config_path: str) -> Dict[str, str]:
        """
        환경설정 파일 로드
        
        Args:
            config_path: 환경설정 파일 경로
            
        Returns:
            설정 값 딕셔너리
        """
        config = {}
        try:
            with open(config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip().strip('"\'')
        except Exception as e:
            print(f"설정 파일 로드 오류: {e}")
        return config
    
    def _get_access_token(self) -> str:
        """
        API 액세스 토큰 발급
        
        Returns:
            액세스 토큰
        """
        url = f"{self.base_url}/oauth2/tokenP"
        headers = {
            "content-type": "application/json"
        }
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        try:
            response = requests.post(url, headers=headers, data=json.dumps(body))
            if response.status_code == 200:
                return response.json().get('access_token', '')
            else:
                print(f"토큰 발급 실패: {response.text}")
                return ''
        except Exception as e:
            print(f"토큰 발급 오류: {e}")
            return ''
    
    async def get_nasdaq_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        나스닥 실시간 시세 조회
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            시세 정보 딕셔너리
        """
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/price"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.app_key,
            "appSecret": self.app_secret,
            "tr_id": "HHDFS00000300"
        }
        params = {
            "AUTH": "",
            "EXCD": "NAS",  # 나스닥
            "SYMB": symbol
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('rt_cd') == '0':  # 정상 응답
                    output = data.get('output', {})
                    return {
                        'symbol': symbol,
                        'price': float(output.get('last', '0')),
                        'open': float(output.get('open', '0')),
                        'high': float(output.get('high', '0')),
                        'low': float(output.get('low', '0')),
                        'volume': int(output.get('volume', '0')),
                        'change': float(output.get('diff', '0')),
                        'change_rate': float(output.get('diff_rt', '0')),
                        'timestamp': datetime.now().isoformat()
                    }
            
            # 토큰이 만료된 경우 갱신
            if response.status_code == 401:
                self.token = self._get_access_token()
                return await self.get_nasdaq_quote(symbol)
                
            print(f"시세 조회 실패: {response.text}")
            return None
        except Exception as e:
            print(f"시세 조회 오류: {e}")
            return None
    
    async def get_nasdaq_daily(self, symbol: str, start_date: str, end_date: str = None) -> List[Dict[str, Any]]:
        """
        나스닥 일별 시세 조회
        
        Args:
            symbol: 종목 심볼
            start_date: 시작일(YYYYMMDD)
            end_date: 종료일(YYYYMMDD), 입력하지 않으면 당일
            
        Returns:
            일별 시세 리스트
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')
            
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.token}",
            "appKey": self.app_key,
            "appSecret": self.app_secret,
            "tr_id": "HHDFS76240000"
        }
        params = {
            "AUTH": "",
            "EXCD": "NAS",  # 나스닥
            "SYMB": symbol,
            "GUBN": "0",    # 0: 일, 1: 주, 2: 월
            "BYMD": start_date,
            "MODDT": end_date,
            "KEYB": ""
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('rt_cd') == '0':  # 정상 응답
                    daily_prices = []
                    for item in data.get('output', []):
                        date_str = item.get('xymd', '')
                        date = datetime.strptime(date_str, '%Y%m%d').date()
                        
                        daily_prices.append({
                            'symbol': symbol,
                            'date': date.isoformat(),
                            'open': float(item.get('open', '0')),
                            'high': float(item.get('high', '0')),
                            'low': float(item.get('low', '0')),
                            'close': float(item.get('clos', '0')),
                            'volume': int(item.get('tvol', '0')),
                        })
                    return daily_prices
            
            # 토큰이 만료된 경우 갱신
            if response.status_code == 401:
                self.token = self._get_access_token()
                return await self.get_nasdaq_daily(symbol, start_date, end_date)
                
            print(f"일별 시세 조회 실패: {response.text}")
            return []
        except Exception as e:
            print(f"일별 시세 조회 오류: {e}")
            return []
