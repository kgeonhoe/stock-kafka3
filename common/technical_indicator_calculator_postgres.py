#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PostgreSQL 기반 기술적 지표 계산기
- DuckDB 대신 PostgreSQL 직접 사용
- 배치 처리로 메모리 효율성 확보
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
import logging
from database import PostgreSQLManager
from datetime import datetime, timedelta


class TechnicalIndicatorCalculatorPostgreSQL:
    """PostgreSQL 기반 기술적 지표 계산기 (DuckDB 대체)"""
    
    def __init__(self):
        """초기화"""
        self.logger = logging.getLogger(__name__)
        self.db = PostgreSQLManager()
        self.logger.info("✅ PostgreSQL 기술적 지표 계산기 초기화 완료")
        
    def calculate_technical_indicators(self, symbols: Optional[List[str]] = None, days_back: int = 200) -> int:
        """
        기술적 지표 계산
        
        Args:
            symbols: 계산할 심볼 리스트 (None이면 전체)
            days_back: 계산할 과거 날짜 수
            
        Returns:
            처리된 레코드 수
        """
        try:
            self.logger.info("📊 PostgreSQL 기반 기술적 지표 계산 시작...")
            
            # 심볼 목록 가져오기
            if symbols is None:
                symbols = self._get_all_symbols()
            
            self.logger.info(f"🔢 처리할 심볼 수: {len(symbols)}개")
            total_processed = 0
            
            for i, symbol in enumerate(symbols, 1):
                try:
                    # 주가 데이터 조회
                    stock_data = self._get_stock_data(symbol, days_back)
                    
                    if len(stock_data) < 50:  # 최소 50일 데이터 필요
                        self.logger.debug(f"⚠️ {symbol}: 데이터 부족 ({len(stock_data)}일)")
                        continue
                    
                    # 기술적 지표 계산
                    indicators = self._calculate_indicators_for_symbol(stock_data)
                    
                    if indicators:
                        # 데이터베이스에 저장
                        saved_count = self._save_indicators(symbol, indicators)
                        total_processed += saved_count
                        self.logger.info(f"✅ [{i}/{len(symbols)}] {symbol}: {saved_count}개 지표 저장")
                    else:
                        self.logger.debug(f"⚠️ {symbol}: 지표 계산 결과 없음")
                    
                except Exception as e:
                    self.logger.error(f"❌ {symbol}: 지표 계산 오류 - {e}")
                    continue
            
            self.logger.info(f"✅ 기술적 지표 계산 완료: {total_processed}개 레코드 처리")
            return total_processed
            
        except Exception as e:
            self.logger.error(f"❌ 기술적 지표 계산 전체 오류: {e}")
            return 0
    
    def _get_all_symbols(self) -> List[str]:
        """모든 심볼 조회"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT symbol FROM stock_data ORDER BY symbol")
                    symbols = [row[0] for row in cur.fetchall()]
                    self.logger.info(f"📋 조회된 심볼 수: {len(symbols)}개")
                    return symbols
        except Exception as e:
            self.logger.error(f"❌ 심볼 조회 오류: {e}")
            return []
    
    def _get_stock_data(self, symbol: str, days_back: int) -> pd.DataFrame:
        """주가 데이터 조회"""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            
            query = """
                SELECT date, open, high, low, close, volume
                FROM stock_data 
                WHERE symbol = %s AND date >= %s AND date <= %s
                ORDER BY date ASC
            """
            
            with self.db.get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                return df
                
        except Exception as e:
            self.logger.error(f"❌ {symbol} 데이터 조회 오류: {e}")
            return pd.DataFrame()
    
    def _calculate_indicators_for_symbol(self, df: pd.DataFrame) -> List[Dict]:
        """단일 심볼에 대한 기술적 지표 계산"""
        try:
            if df.empty or len(df) < 20:
                return []
            
            indicators_list = []
            
            # 이동평균선 계산
            df['sma_20'] = df['close'].rolling(window=20).mean()
            df['sma_50'] = df['close'].rolling(window=50).mean()
            df['ema_12'] = df['close'].ewm(span=12).mean()
            df['ema_26'] = df['close'].ewm(span=26).mean()
            
            # MACD 계산
            df['macd'] = df['ema_12'] - df['ema_26']
            df['macd_signal'] = df['macd'].ewm(span=9).mean()
            df['macd_histogram'] = df['macd'] - df['macd_signal']
            
            # RSI 계산
            df['rsi'] = self._calculate_rsi(df['close'])
            
            # Bollinger Bands 계산
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            bb_std = df['close'].rolling(window=20).std()
            df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
            df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
            
            # 볼륨 이동평균
            df['volume_sma_20'] = df['volume'].rolling(window=20).mean()
            
            # 결과 리스트 생성 (최소 20일 데이터가 있는 경우만)
            for date, row in df.iterrows():
                if pd.notna(row['sma_20']):  
                    indicator_data = {
                        'date': date.date(),
                        'sma_20': float(row['sma_20']) if pd.notna(row['sma_20']) else None,
                        'sma_50': float(row['sma_50']) if pd.notna(row['sma_50']) else None,
                        'ema_12': float(row['ema_12']) if pd.notna(row['ema_12']) else None,
                        'ema_26': float(row['ema_26']) if pd.notna(row['ema_26']) else None,
                        'macd': float(row['macd']) if pd.notna(row['macd']) else None,
                        'macd_signal': float(row['macd_signal']) if pd.notna(row['macd_signal']) else None,
                        'macd_histogram': float(row['macd_histogram']) if pd.notna(row['macd_histogram']) else None,
                        'rsi': float(row['rsi']) if pd.notna(row['rsi']) else None,
                        'bb_upper': float(row['bb_upper']) if pd.notna(row['bb_upper']) else None,
                        'bb_middle': float(row['bb_middle']) if pd.notna(row['bb_middle']) else None,
                        'bb_lower': float(row['bb_lower']) if pd.notna(row['bb_lower']) else None,
                        'volume_sma_20': float(row['volume_sma_20']) if pd.notna(row['volume_sma_20']) else None,
                    }
                    indicators_list.append(indicator_data)
            
            return indicators_list
            
        except Exception as e:
            self.logger.error(f"❌ 지표 계산 오류: {e}")
            return []
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """RSI 계산"""
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            # 0으로 나누기 방지
            rs = gain / loss.replace(0, np.nan)
            rsi = 100 - (100 / (1 + rs))
            return rsi.fillna(50)  # NaN을 중립값인 50으로 대체
            
        except Exception as e:
            self.logger.error(f"❌ RSI 계산 오류: {e}")
            return pd.Series([50] * len(prices))  # 오류 시 중립값 반환
    
    def _save_indicators(self, symbol: str, indicators: List[Dict]) -> int:
        """기술적 지표 저장"""
        try:
            if not indicators:
                return 0
            
            # 배치 INSERT 쿼리 (ON CONFLICT로 업데이트)
            insert_query = """
                INSERT INTO stock_data_technical_indicators 
                (symbol, date, sma_20, sma_50, ema_12, ema_26, macd, macd_signal, 
                 macd_histogram, rsi, bb_upper, bb_middle, bb_lower, volume_sma_20, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, date) 
                DO UPDATE SET
                    sma_20 = EXCLUDED.sma_20,
                    sma_50 = EXCLUDED.sma_50,
                    ema_12 = EXCLUDED.ema_12,
                    ema_26 = EXCLUDED.ema_26,
                    macd = EXCLUDED.macd,
                    macd_signal = EXCLUDED.macd_signal,
                    macd_histogram = EXCLUDED.macd_histogram,
                    rsi = EXCLUDED.rsi,
                    bb_upper = EXCLUDED.bb_upper,
                    bb_middle = EXCLUDED.bb_middle,
                    bb_lower = EXCLUDED.bb_lower,
                    volume_sma_20 = EXCLUDED.volume_sma_20,
                    updated_at = NOW()
            """
            
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    insert_data = []
                    for ind in indicators:
                        insert_data.append((
                            symbol, ind['date'], ind['sma_20'], ind['sma_50'],
                            ind['ema_12'], ind['ema_26'], ind['macd'], ind['macd_signal'],
                            ind['macd_histogram'], ind['rsi'], ind['bb_upper'], 
                            ind['bb_middle'], ind['bb_lower'], ind['volume_sma_20']
                        ))
                    
                    cur.executemany(insert_query, insert_data)
                    conn.commit()
                    
                    return len(insert_data)
                    
        except Exception as e:
            self.logger.error(f"❌ 지표 저장 오류: {e}")
            return 0
    
    def calculate_all_indicators(self, stock_data: List[Dict]) -> List[Dict]:
        """
        모든 기술적 지표 계산 (DAG 호환성을 위한 메서드)
        
        Args:
            stock_data: 주가 데이터 리스트
            
        Returns:
            기술적 지표 데이터 리스트
        """
        try:
            if not stock_data or len(stock_data) < 20:
                return []
            
            # DataFrame 변환
            df = pd.DataFrame(stock_data)
            self.logger.info(f"🔍 DataFrame 생성 성공, 컬럼: {list(df.columns)}")
            
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)  # 인덱스 리셋
            
            symbol = stock_data[0].get('symbol', '')
            indicators_list = []
            
            # 각 날짜별로 기술적 지표 계산
            for i, row in df.iterrows():
                if i < 20:  # 최소 20일 데이터 필요
                    continue
                
                self.logger.debug(f"🔍 처리 중: {i}번째 행, 타입: {type(row)}, 인덱스: {row.index.tolist()}")
                
                # 현재 날짜까지의 데이터
                current_data = df.iloc[:i+1].copy()
                
                # 기술적 지표 계산
                sma_20 = current_data['close'].rolling(window=20).mean().iloc[-1]
                ema_12 = current_data['close'].ewm(span=12).mean().iloc[-1]
                ema_26 = current_data['close'].ewm(span=26).mean().iloc[-1]
                
                # MACD
                macd_line = ema_12 - ema_26
                macd_signal = pd.Series([macd_line]).ewm(span=9).mean().iloc[0] if len(current_data) >= 9 else None
                
                # RSI
                delta = current_data['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs)).iloc[-1] if len(current_data) >= 14 else None
                
                # Bollinger Bands
                bb_middle = sma_20
                bb_std = current_data['close'].rolling(window=20).std().iloc[-1]
                bb_upper = bb_middle + (bb_std * 2)
                bb_lower = bb_middle - (bb_std * 2)
                
                # 날짜 처리 개선
                try:
                    if 'date' in row.index:
                        date_value = row['date']
                        if hasattr(date_value, 'date'):
                            date_value = date_value.date()
                        elif not isinstance(date_value, str):
                            date_value = str(date_value)
                    else:
                        self.logger.warning(f"⚠️ 'date' 컬럼이 row에 없음: {row.index.tolist()}")
                        date_value = str(df.iloc[i]['date'].date() if hasattr(df.iloc[i]['date'], 'date') else df.iloc[i]['date'])
                except Exception as date_error:
                    self.logger.error(f"❌ 날짜 처리 오류 ({symbol}, row {i}): {date_error}")
                    self.logger.error(f"   row 타입: {type(row)}, row 내용: {dict(row) if hasattr(row, 'to_dict') else row}")
                    continue  # 이 행은 스킵하고 계속 진행
                
                indicator_data = {
                    'symbol': symbol,
                    'date': date_value,
                    'sma_20': float(sma_20) if pd.notna(sma_20) else None,
                    'ema_12': float(ema_12) if pd.notna(ema_12) else None,
                    'ema_26': float(ema_26) if pd.notna(ema_26) else None,
                    'macd_line': float(macd_line) if pd.notna(macd_line) else None,
                    'macd_signal': float(macd_signal) if pd.notna(macd_signal) else None,
                    'rsi': float(rsi) if pd.notna(rsi) else None,
                    'bb_upper': float(bb_upper) if pd.notna(bb_upper) else None,
                    'bb_middle': float(bb_middle) if pd.notna(bb_middle) else None,
                    'bb_lower': float(bb_lower) if pd.notna(bb_lower) else None
                }
                
                indicators_list.append(indicator_data)
            
            return indicators_list
            
        except Exception as e:
            self.logger.error(f"❌ 기술적 지표 계산 오류: {e}")
            return []

    def save_indicators_batch(self, indicators_list: List[Dict]) -> int:
        """
        기술적 지표 배치 저장
        
        Args:
            indicators_list: 기술적 지표 데이터 리스트
            
        Returns:
            저장된 레코드 수
        """
        try:
            if not indicators_list:
                return 0
            
            saved_count = 0
            for indicator_data in indicators_list:
                self.db.save_technical_indicators(indicator_data)
                saved_count += 1
            
            return saved_count
            
        except Exception as e:
            self.logger.error(f"❌ 기술적 지표 배치 저장 오류: {e}")
            return 0

    def close(self):
        """리소스 정리"""
        try:
            if hasattr(self, 'db'):
                self.db.close()
            self.logger.info("✅ PostgreSQL 기술적 지표 계산기 정리 완료")
        except Exception as e:
            self.logger.error(f"❌ 리소스 정리 오류: {e}")
