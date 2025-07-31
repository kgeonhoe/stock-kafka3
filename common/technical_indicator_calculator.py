#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from common.database import DuckDBManager

class TechnicalIndicatorCalculator:
    """기술적 지표 계산 클래스"""
    
    def __init__(self, db_path: str = "/data/duckdb/stock_data_replica.db"):
        """
        기술적 지표 계산 클래스 초기화
        
        Args:
            db_path: DuckDB 파일 경로 (읽기 전용 replica 사용)
        """
        # 읽기 전용 모드로 DuckDB 연결
        try:
            import duckdb
            self.conn = duckdb.connect(db_path, read_only=True)
            print(f"✅ DuckDB 읽기 전용 연결 성공: {db_path}")
        except Exception as e:
            print(f"❌ DuckDB 연결 실패: {e}")
            # 대체 연결 시도 (인메모리)
            self.conn = duckdb.connect(":memory:")
            print("⚠️ 인메모리 DuckDB로 대체 연결")
        
        self.db_path = db_path
    
    def get_historical_data(self, symbol: str, days: int = 50) -> Optional[pd.DataFrame]:
        """
        종목의 과거 일봉 데이터 조회
        
        Args:
            symbol: 종목 심볼
            days: 조회할 일수
            
        Returns:
            과거 일봉 데이터 DataFrame 또는 None
        """
        try:
            # 테이블 존재 확인 (DuckDB 방식)
            try:
                tables = self.conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0].lower() for table in tables]
                if 'stock_data' not in table_names:
                    print(f"⚠️ {symbol}: stock_data 테이블 없음 (사용 가능한 테이블: {table_names})")
                    return None
            except Exception as table_error:
                print(f"⚠️ {symbol}: 테이블 확인 실패 - {table_error}")
                return None
            
            # 최근 N일간 데이터 조회
            query = """
                SELECT date, open, high, low, close, volume
                FROM stock_data 
                WHERE symbol = ? 
                ORDER BY date DESC 
                LIMIT ?
            """
            
            result = self.conn.execute(query, (symbol, days)).df()
            
            if result.empty:
                print(f"⚠️ {symbol}: 과거 데이터 없음")
                return None
            
            # 날짜순으로 정렬 (오래된 날짜부터)
            result = result.sort_values('date').reset_index(drop=True)
            
            return result
            
        except Exception as e:
            print(f"❌ {symbol} 과거 데이터 조회 실패: {e}")
            return None
    
    def calculate_sma(self, prices: pd.Series, period: int) -> pd.Series:
        """단순 이동평균 계산"""
        return prices.rolling(window=period, min_periods=period).mean()
    
    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """지수 이동평균 계산"""
        return prices.ewm(span=period, adjust=False).mean()
    
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """RSI (Relative Strength Index) 계산"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_bollinger_bands(self, prices: pd.Series, period: int = 20, std_multiplier: float = 2.0) -> Dict[str, pd.Series]:
        """볼린저 밴드 계산"""
        sma = self.calculate_sma(prices, period)
        std = prices.rolling(window=period).std()
        
        upper_band = sma + (std * std_multiplier)
        lower_band = sma - (std * std_multiplier)
        
        return {
            'sma': sma,
            'upper_band': upper_band,
            'lower_band': lower_band,
            'std': std
        }
    
    def calculate_macd(self, prices: pd.Series, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Dict[str, pd.Series]:
        """MACD 계산"""
        ema_fast = self.calculate_ema(prices, fast_period)
        ema_slow = self.calculate_ema(prices, slow_period)
        
        macd_line = ema_fast - ema_slow
        signal_line = self.calculate_ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        }
    
    def calculate_all_indicators(self, symbol: str, current_price: float = None) -> Optional[Dict[str, Any]]:
        """
        모든 기술적 지표 계산
        
        Args:
            symbol: 종목 심볼
            current_price: 현재 가격 (실시간 데이터)
            
        Returns:
            계산된 기술적 지표들
        """
        try:
            # 과거 50일 데이터 조회
            df = self.get_historical_data(symbol, 50)
            
            if df is None or len(df) < 20:
                print(f"⚠️ {symbol}: 기술적 지표 계산을 위한 충분한 데이터 없음 (최소 20일 필요)")
                return None
            
            # 종가 기준으로 계산
            close_prices = df['close']
            high_prices = df['high']
            low_prices = df['low']
            volumes = df['volume']
            
            # 현재 가격이 있으면 마지막에 추가
            if current_price:
                close_prices = pd.concat([close_prices, pd.Series([current_price])], ignore_index=True)
            
            # 각종 지표 계산
            sma_5 = self.calculate_sma(close_prices, 5)
            sma_20 = self.calculate_sma(close_prices, 20)
            ema_12 = self.calculate_ema(close_prices, 12)
            ema_26 = self.calculate_ema(close_prices, 26)
            
            rsi = self.calculate_rsi(close_prices, 14)
            bollinger = self.calculate_bollinger_bands(close_prices, 20)
            macd = self.calculate_macd(close_prices, 12, 26, 9)
            
            # 최신 값들 추출 (마지막 값)
            latest_indicators = {
                'symbol': symbol,
                'calculation_time': datetime.now().isoformat(),
                'data_points': len(df),
                
                # 이동평균
                'sma_5': round(float(sma_5.iloc[-1]), 2) if not pd.isna(sma_5.iloc[-1]) else None,
                'sma_20': round(float(sma_20.iloc[-1]), 2) if not pd.isna(sma_20.iloc[-1]) else None,
                'ema_12': round(float(ema_12.iloc[-1]), 2) if not pd.isna(ema_12.iloc[-1]) else None,
                'ema_26': round(float(ema_26.iloc[-1]), 2) if not pd.isna(ema_26.iloc[-1]) else None,
                
                # RSI
                'rsi': round(float(rsi.iloc[-1]), 2) if not pd.isna(rsi.iloc[-1]) else None,
                
                # 볼린저 밴드
                'bb_upper': round(float(bollinger['upper_band'].iloc[-1]), 2) if not pd.isna(bollinger['upper_band'].iloc[-1]) else None,
                'bb_middle': round(float(bollinger['sma'].iloc[-1]), 2) if not pd.isna(bollinger['sma'].iloc[-1]) else None,
                'bb_lower': round(float(bollinger['lower_band'].iloc[-1]), 2) if not pd.isna(bollinger['lower_band'].iloc[-1]) else None,
                
                # MACD
                'macd': round(float(macd['macd'].iloc[-1]), 4) if not pd.isna(macd['macd'].iloc[-1]) else None,
                'macd_signal': round(float(macd['signal'].iloc[-1]), 4) if not pd.isna(macd['signal'].iloc[-1]) else None,
                'macd_histogram': round(float(macd['histogram'].iloc[-1]), 4) if not pd.isna(macd['histogram'].iloc[-1]) else None,
                
                # 현재 가격 정보
                'current_price': current_price,
                'last_close': round(float(close_prices.iloc[-2]), 2) if len(close_prices) > 1 else None,  # 실시간 가격 제외한 마지막 종가
            }
            
            # 매매 신호 생성
            signals = self.generate_trading_signals(latest_indicators, current_price)
            latest_indicators.update(signals)
            
            return latest_indicators
            
        except Exception as e:
            print(f"❌ {symbol} 기술적 지표 계산 실패: {e}")
            return None
    
    def generate_trading_signals(self, indicators: Dict[str, Any], current_price: float = None) -> Dict[str, Any]:
        """
        기술적 지표 기반 매매 신호 생성
        
        Args:
            indicators: 계산된 기술적 지표
            current_price: 현재 가격
            
        Returns:
            매매 신호 정보
        """
        signals = {
            'signals': [],
            'overall_sentiment': 'neutral',
            'strength': 0  # -100 (강한 매도) ~ +100 (강한 매수)
        }
        
        try:
            strength_score = 0
            signal_count = 0
            
            # RSI 신호
            if indicators.get('rsi'):
                rsi = indicators['rsi']
                if rsi < 30:
                    signals['signals'].append('RSI 과매도 (매수 신호)')
                    strength_score += 30
                elif rsi > 70:
                    signals['signals'].append('RSI 과매수 (매도 신호)')
                    strength_score -= 30
                signal_count += 1
            
            # 볼린저 밴드 신호
            if current_price and indicators.get('bb_upper') and indicators.get('bb_lower'):
                bb_upper = indicators['bb_upper']
                bb_lower = indicators['bb_lower']
                bb_middle = indicators['bb_middle']
                
                if current_price >= bb_upper:
                    signals['signals'].append('볼린저 밴드 상단 돌파 (매도 고려)')
                    strength_score -= 20
                elif current_price <= bb_lower:
                    signals['signals'].append('볼린저 밴드 하단 터치 (매수 고려)')
                    strength_score += 20
                elif current_price > bb_middle:
                    signals['signals'].append('볼린저 밴드 중간선 위 (상승 추세)')
                    strength_score += 10
                else:
                    signals['signals'].append('볼린저 밴드 중간선 아래 (하락 추세)')
                    strength_score -= 10
                signal_count += 1
            
            # 이동평균 신호
            if current_price and indicators.get('sma_5') and indicators.get('sma_20'):
                sma_5 = indicators['sma_5']
                sma_20 = indicators['sma_20']
                
                if sma_5 > sma_20 and current_price > sma_5:
                    signals['signals'].append('단기 이평선 위 (상승 추세)')
                    strength_score += 15
                elif sma_5 < sma_20 and current_price < sma_5:
                    signals['signals'].append('단기 이평선 아래 (하락 추세)')
                    strength_score -= 15
                signal_count += 1
            
            # MACD 신호
            if indicators.get('macd') and indicators.get('macd_signal'):
                macd = indicators['macd']
                macd_signal = indicators['macd_signal']
                
                if macd > macd_signal and macd > 0:
                    signals['signals'].append('MACD 매수 신호')
                    strength_score += 25
                elif macd < macd_signal and macd < 0:
                    signals['signals'].append('MACD 매도 신호')
                    strength_score -= 25
                signal_count += 1
            
            # 전체 신호 강도 계산 (평균)
            if signal_count > 0:
                signals['strength'] = int(strength_score / signal_count)
            
            # 전체 심리 결정
            if signals['strength'] > 20:
                signals['overall_sentiment'] = 'bullish'
            elif signals['strength'] < -20:
                signals['overall_sentiment'] = 'bearish'
            else:
                signals['overall_sentiment'] = 'neutral'
            
            return signals
            
        except Exception as e:
            print(f"❌ 매매 신호 생성 실패: {e}")
            return signals
    
    def close(self):
        """리소스 정리"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            print("📊 DuckDB 연결 종료")

# 테스트 함수
def test_technical_indicators():
    """기술적 지표 계산 테스트"""
    try:
        calculator = TechnicalIndicatorCalculator()
        
        # AAPL 기술적 지표 계산
        indicators = calculator.calculate_all_indicators('AAPL', current_price=150.25)
        
        if indicators:
            print("✅ 기술적 지표 계산 성공:")
            for key, value in indicators.items():
                print(f"  {key}: {value}")
        else:
            print("❌ 기술적 지표 계산 실패")
        
        calculator.close()
        return indicators is not None
        
    except Exception as e:
        print(f"❌ 기술적 지표 테스트 실패: {e}")
        return False

if __name__ == "__main__":
    test_technical_indicators()
