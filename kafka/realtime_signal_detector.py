#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
실시간 기술적 신호 감지 및 성과 추적 Consumer
Redis 히스토리컬 데이터 + Kafka 실시간 데이터를 결합하여 기술적 지표 계산
"""

import sys
import json
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# 프로젝트 경로 추가
sys.path.append('/app/common')
sys.path.append('/app/config')

from kafka import KafkaConsumer
from redis_client import RedisClient
from technical_indicator_calculator import TechnicalIndicatorCalculator

class RealTimeSignalDetector:
    """실시간 기술적 신호 감지 및 추적 클래스"""
    
    def __init__(self):
        self.redis = RedisClient()
        self.calculator = TechnicalIndicatorCalculator()
        
        # Kafka Consumer 설정
        self.consumer = KafkaConsumer(
            'realtime-stock',  # 실시간 주식 데이터 토픽
            bootstrap_servers=['kafka:29092'],
            group_id='signal-detector-group',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print("🚀 실시간 신호 감지기 시작")
        print("📊 토픽: realtime-stock")
        print("🔍 신호 조건: 볼린저 밴드 상단 터치, RSI 과매도/과매수")
    
    def process_realtime_data(self):
        """실시간 데이터 처리 메인 루프"""
        try:
            for message in self.consumer:
                stock_data = message.value
                symbol = stock_data.get('symbol')
                
                if not symbol:
                    continue
                
                print(f"📥 {symbol} 실시간 데이터 수신: ${stock_data.get('price', 'N/A')}")
                
                # 관심종목인지 확인
                watchlist_data = self.redis.get_watchlist_data(symbol)
                if not watchlist_data:
                    print(f"⚠️ {symbol}: 관심종목이 아님, 건너뛰기")
                    continue
                
                # 기술적 지표 계산 및 신호 감지
                self.analyze_and_detect_signals(symbol, stock_data, watchlist_data)
                
                # 기존 활성 신호들의 성과 업데이트
                self.update_signal_performance(symbol, stock_data['price'])
                
        except KeyboardInterrupt:
            print("🛑 사용자 중단")
        except Exception as e:
            print(f"❌ 처리 중 오류: {e}")
        finally:
            self.consumer.close()
    
    def analyze_and_detect_signals(self, symbol: str, current_data: dict, watchlist_data: dict):
        """기술적 지표 계산 및 신호 감지"""
        try:
            # 히스토리컬 데이터 준비
            historical_data = watchlist_data['historical_data']
            
            # 현재 데이터를 히스토리컬 데이터에 추가
            current_candle = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'open': current_data.get('open', current_data['price']),
                'high': current_data.get('high', current_data['price']),
                'low': current_data.get('low', current_data['price']),
                'close': current_data['price'],
                'volume': current_data.get('volume', 0)
            }
            
            # 최신 데이터 결합 (최대 30개)
            combined_data = historical_data + [current_candle]
            combined_data = combined_data[-30:]  # 최근 30일만 유지
            
            # DataFrame으로 변환
            df = pd.DataFrame(combined_data)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # 기술적 지표 계산
            indicators = self.calculate_technical_indicators(df)
            
            # 신호 감지
            signals_detected = self.detect_signals(symbol, indicators, current_data)
            
            # 실시간 분석 결과 Redis에 저장
            analysis_data = {
                'current_price': current_data['price'],
                'indicators': indicators,
                'signals': signals_detected,
                'recommendation': self.get_recommendation(indicators)
            }
            
            self.redis.set_realtime_analysis(symbol, analysis_data)
            
            # 신호가 감지되면 로그 및 저장
            if signals_detected:
                print(f"🚨 {symbol} 신호 감지: {signals_detected}")
                for signal in signals_detected:
                    self.save_signal_trigger(symbol, signal, current_data, indicators)
        
        except Exception as e:
            print(f"❌ {symbol} 분석 중 오류: {e}")
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> dict:
        """기술적 지표 계산"""
        try:
            indicators = {}
            
            # RSI 계산
            rsi = self.calculator.calculate_rsi(df['close'].values, period=14)
            indicators['rsi'] = float(rsi[-1]) if len(rsi) > 0 else None
            
            # 볼린저 밴드 계산
            bb_upper, bb_middle, bb_lower = self.calculator.calculate_bollinger_bands(
                df['close'].values, period=20, std_dev=2
            )
            indicators['bb_upper'] = float(bb_upper[-1]) if len(bb_upper) > 0 else None
            indicators['bb_middle'] = float(bb_middle[-1]) if len(bb_middle) > 0 else None
            indicators['bb_lower'] = float(bb_lower[-1]) if len(bb_lower) > 0 else None
            
            # 현재 가격의 볼린저 밴드 내 위치 (0-1)
            if indicators['bb_upper'] and indicators['bb_lower']:
                current_price = df['close'].iloc[-1]
                bb_position = (current_price - indicators['bb_lower']) / (indicators['bb_upper'] - indicators['bb_lower'])
                indicators['bb_position'] = float(bb_position)
            
            # MACD 계산
            macd_line, signal_line, histogram = self.calculator.calculate_macd(
                df['close'].values, fast=12, slow=26, signal=9
            )
            indicators['macd'] = float(macd_line[-1]) if len(macd_line) > 0 else None
            indicators['macd_signal'] = float(signal_line[-1]) if len(signal_line) > 0 else None
            indicators['macd_histogram'] = float(histogram[-1]) if len(histogram) > 0 else None
            
            return indicators
            
        except Exception as e:
            print(f"❌ 기술적 지표 계산 오류: {e}")
            return {}
    
    def detect_signals(self, symbol: str, indicators: dict, current_data: dict) -> list:
        """신호 감지 로직"""
        signals = []
        
        try:
            current_price = current_data['price']
            
            # 1. 볼린저 밴드 상단 터치 신호
            if (indicators.get('bb_position') and 
                indicators['bb_position'] >= 0.95):  # 상단 5% 이내
                signals.append({
                    'type': 'bollinger_upper_touch',
                    'strength': indicators['bb_position'],
                    'description': f"볼린저 밴드 상단 터치 (위치: {indicators['bb_position']:.3f})"
                })
            
            # 2. RSI 과매수 신호
            if indicators.get('rsi') and indicators['rsi'] >= 70:
                signals.append({
                    'type': 'rsi_overbought',
                    'strength': indicators['rsi'] / 100,
                    'description': f"RSI 과매수 (RSI: {indicators['rsi']:.1f})"
                })
            
            # 3. RSI 과매도 신호
            if indicators.get('rsi') and indicators['rsi'] <= 30:
                signals.append({
                    'type': 'rsi_oversold',
                    'strength': (30 - indicators['rsi']) / 30,
                    'description': f"RSI 과매도 (RSI: {indicators['rsi']:.1f})"
                })
            
            # 4. MACD 골든 크로스
            if (indicators.get('macd') and indicators.get('macd_signal') and
                indicators['macd'] > indicators['macd_signal'] and
                indicators.get('macd_histogram', 0) > 0):
                signals.append({
                    'type': 'macd_bullish',
                    'strength': abs(indicators['macd_histogram']) / current_price * 1000,
                    'description': f"MACD 상승 신호"
                })
            
            return signals
            
        except Exception as e:
            print(f"❌ {symbol} 신호 감지 오류: {e}")
            return []
    
    def save_signal_trigger(self, symbol: str, signal: dict, current_data: dict, indicators: dict):
        """신호 발생 기록 저장"""
        try:
            signal_data = {
                'signal_type': signal['type'],
                'trigger_price': current_data['price'],
                'timestamp': datetime.now().isoformat(),
                'technical_values': {
                    'rsi': indicators.get('rsi'),
                    'bb_position': indicators.get('bb_position'),
                    'macd': indicators.get('macd'),
                    'signal_strength': signal['strength']
                }
            }
            
            success = self.redis.set_signal_trigger(symbol, signal_data)
            if success:
                print(f"💾 {symbol} 신호 저장 완료: {signal['type']}")
            
        except Exception as e:
            print(f"❌ {symbol} 신호 저장 오류: {e}")
    
    def update_signal_performance(self, symbol: str, current_price: float):
        """활성 신호들의 성과 업데이트"""
        try:
            active_signals = self.redis.get_active_signals(symbol)
            
            for signal in active_signals:
                updated_signal = self.redis.update_signal_performance(
                    symbol=symbol,
                    trigger_time=signal['trigger_time'],
                    current_price=current_price
                )
                
                if updated_signal:
                    change_pct = updated_signal.get('price_change_pct', 0)
                    if abs(change_pct) > 0.1:  # 0.1% 이상 변화시 로그
                        direction = "📈" if change_pct > 0 else "📉"
                        print(f"{direction} {symbol} 성과 업데이트: {change_pct:+.2f}%")
        
        except Exception as e:
            print(f"❌ {symbol} 성과 업데이트 오류: {e}")
    
    def get_recommendation(self, indicators: dict) -> str:
        """기술적 지표 기반 추천"""
        try:
            score = 0
            
            # RSI 점수
            rsi = indicators.get('rsi')
            if rsi:
                if rsi < 30:
                    score += 2  # 과매도 -> 매수 신호
                elif rsi > 70:
                    score -= 2  # 과매수 -> 매도 신호
                elif 40 <= rsi <= 60:
                    score += 0  # 중립
            
            # 볼린저 밴드 점수
            bb_position = indicators.get('bb_position')
            if bb_position:
                if bb_position < 0.2:
                    score += 1  # 하단 근처 -> 매수
                elif bb_position > 0.8:
                    score -= 1  # 상단 근처 -> 매도
            
            # MACD 점수
            if indicators.get('macd_histogram', 0) > 0:
                score += 1
            elif indicators.get('macd_histogram', 0) < 0:
                score -= 1
            
            # 추천 결정
            if score >= 2:
                return "strong_buy"
            elif score == 1:
                return "buy"
            elif score == -1:
                return "sell"
            elif score <= -2:
                return "strong_sell"
            else:
                return "hold"
                
        except Exception as e:
            print(f"❌ 추천 계산 오류: {e}")
            return "hold"

def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("🚨 Real-Time Signal Detector")
    print("=" * 60)
    
    detector = RealTimeSignalDetector()
    detector.process_realtime_data()

if __name__ == "__main__":
    main()
