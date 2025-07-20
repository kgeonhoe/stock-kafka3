"""
API Configuration
"""

import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv('/home/grey1/stock-kafka3/config/.env')

class APIConfig:
    """API 설정 관리"""
    
    # 한국투자증권 API
    KIS_APP_KEY = os.getenv('KIS_APP_KEY', '')
    KIS_APP_SECRET = os.getenv('KIS_APP_SECRET', '')
    KIS_ACCOUNT_NO = os.getenv('KIS_ACCOUNT_NO', '')
    KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
    
    # NASDAQ API
    NASDAQ_BASE_URL = "https://api.nasdaq.com/api/screener/stocks"
    NASDAQ_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
    
    # Slack API
    SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')
    SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', '#stock-alerts')