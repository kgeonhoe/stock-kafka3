#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime
from typing import Dict, Any, Optional

class SlackNotifier:
    """Slack 알림 클래스"""
    
    def __init__(self, webhook_url: str = None, config_path: str = "/home/grey1/stock-kafka3/config/.env"):
        """
        Slack 알림 클래스 초기화
        
        Args:
            webhook_url: Slack 웹훅 URL
            config_path: 환경설정 파일 경로
        """
        self.webhook_url = webhook_url
        if not webhook_url:
            self.config = self._load_config(config_path)
            self.webhook_url = self.config.get('SLACK_WEBHOOK_URL', '')
    
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
    
    def send_message(self, message: str, title: Optional[str] = None, color: str = "#36a64f") -> bool:
        """
        Slack 메시지 전송
        
        Args:
            message: 메시지 내용
            title: 제목 (optional)
            color: 색상 코드 (default: 녹색)
            
        Returns:
            전송 성공 여부
        """
        if not self.webhook_url:
            print("Slack 웹훅 URL이 설정되지 않았습니다.")
            return False
        
        payload = {
            "attachments": [
                {
                    "color": color,
                    "text": message,
                    "ts": datetime.now().timestamp()
                }
            ]
        }
        
        if title:
            payload["attachments"][0]["title"] = title
        
        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            return response.status_code == 200
        except Exception as e:
            print(f"Slack 메시지 전송 오류: {e}")
            return False
    
    def send_stock_alert(self, symbol: str, price: float, change_percent: float, alert_type: str, message: str) -> bool:
        """
        주식 알림 전송
        
        Args:
            symbol: 종목 심볼
            price: 현재 가격
            change_percent: 변동률 (%)
            alert_type: 알림 유형 (ex: "가격 돌파", "기술적 신호", "거래량 급증")
            message: 알림 내용
            
        Returns:
            전송 성공 여부
        """
        # 변동률에 따라 색상 결정
        if change_percent > 0:
            color = "#36a64f"  # 녹색
        elif change_percent < 0:
            color = "#d72b3f"  # 빨간색
        else:
            color = "#9e9ea6"  # 회색
        
        # 제목 생성
        title = f"[{alert_type}] {symbol}: ${price:,.2f} ({change_percent:+.2f}%)"
        
        # 메시지 전송
        return self.send_message(message, title, color)
