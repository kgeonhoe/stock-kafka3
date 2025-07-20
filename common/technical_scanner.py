import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import sys
import os

# 절대 경로로 database 모듈 임포트
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
from database import DuckDBManager

class TechnicalScanner:
    """기술적 지표 기반 종목 스캔"""
    
    def __init__(self, db_path: str):
        self.db = DuckDBManager(db_path)
    
    def scan_bollinger_band_signals(self, scan_date: date = None) -> List[Dict[str, Any]]:
        """볼린저 밴드 상단 터치 종목 스캔"""
        if scan_date is None:
            scan_date = date.today()
        
        query = """
        WITH bb_signals AS (
            SELECT 
                t.symbol,
                t.date,
                t.close_price,
                t.bb_upper,
                t.bb_middle,
                t.bb_lower,
                -- 상단 터치 조건: 현재가가 상단선의 98% 이상
                CASE WHEN t.close_price >= t.bb_upper * 0.98 THEN 1 ELSE 0 END as upper_touch,
                n.market_cap,
                CASE 
                    WHEN CAST(REPLACE(REPLACE(REPLACE(n.market_cap, '$', ''), 'B', ''), 'T', '') AS DECIMAL) >= 100 THEN 1
                    WHEN CAST(REPLACE(REPLACE(REPLACE(n.market_cap, '$', ''), 'B', ''), 'T', '') AS DECIMAL) >= 10 THEN 2
                    ELSE 3
                END as tier
            FROM stock_technical_indicators t
            JOIN nasdaq_symbols n ON t.symbol = n.symbol
            WHERE t.date = ?
              AND t.bb_upper IS NOT NULL
              AND t.close_price IS NOT NULL
        )
        SELECT * FROM bb_signals WHERE upper_touch = 1
        """
        
        results = self.db.conn.execute(query, (scan_date,)).fetchall()
        
        watchlist = []
        for row in results:
            watchlist.append({
                'symbol': row[0],
                'date': row[1],
                'close_price': row[2],
                'bb_upper': row[3],
                'condition_type': 'bollinger_upper_touch',
                'condition_value': row[2] / row[3],  # 상단선 대비 비율
                'market_cap_tier': row[6]
            })
        
        return watchlist
    
    def update_daily_watchlist(self, scan_date: date = None):
        """일별 관심종목 업데이트"""
        if scan_date is None:
            scan_date = date.today()
        
        # 볼린저 밴드 신호 스캔
        bb_signals = self.scan_bollinger_band_signals(scan_date)
        
        # 데이터베이스에 저장
        for signal in bb_signals:
            self.db.conn.execute("""
                INSERT OR REPLACE INTO daily_watchlist 
                (symbol, date, condition_type, condition_value, market_cap_tier)
                VALUES (?, ?, ?, ?, ?)
            """, (
                signal['symbol'],
                signal['date'],
                signal['condition_type'],
                signal['condition_value'],
                signal['market_cap_tier']
            ))
        
        self.db.conn.commit()
        print(f"📈 {scan_date} 관심종목 {len(bb_signals)}개 업데이트 완료")
        
        return bb_signals
    
    def get_daily_watchlist(self, scan_date: date = None, condition_type: str = None) -> List[Dict[str, Any]]:
        """일별 관심종목 조회"""
        if scan_date is None:
            scan_date = date.today()
            
        query = """
        SELECT 
            dw.symbol,
            dw.date,
            dw.condition_type,
            dw.condition_value,
            dw.market_cap_tier,
            n.name,
            n.market_cap,
            n.sector
        FROM daily_watchlist dw
        JOIN nasdaq_symbols n ON dw.symbol = n.symbol
        WHERE dw.date = ?
        """
        
        params = [scan_date]
        
        if condition_type:
            query += " AND dw.condition_type = ?"
            params.append(condition_type)
            
        query += " ORDER BY dw.market_cap_tier, dw.condition_value DESC"
        
        results = self.db.conn.execute(query, params).fetchall()
        
        watchlist = []
        for row in results:
            watchlist.append({
                'symbol': row[0],
                'date': row[1],
                'condition_type': row[2],
                'condition_value': row[3],
                'market_cap_tier': row[4],
                'name': row[5],
                'market_cap': row[6],
                'sector': row[7]
            })
        
        return watchlist
    
    def get_watchlist_summary(self, days: int = 7) -> Dict[str, Any]:
        """최근 N일간 관심종목 통계"""
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days-1)
        
        # 일별 관심종목 수
        daily_counts = self.db.conn.execute("""
            SELECT 
                date,
                condition_type,
                COUNT(*) as count
            FROM daily_watchlist
            WHERE date BETWEEN ? AND ?
            GROUP BY date, condition_type
            ORDER BY date DESC
        """, (start_date, end_date)).fetchall()
        
        # 인기 섹터 (최근 7일)
        sector_stats = self.db.conn.execute("""
            SELECT 
                n.sector,
                COUNT(*) as frequency,
                AVG(dw.condition_value) as avg_signal_strength
            FROM daily_watchlist dw
            JOIN nasdaq_symbols n ON dw.symbol = n.symbol
            WHERE dw.date BETWEEN ? AND ?
            GROUP BY n.sector
            ORDER BY frequency DESC
            LIMIT 10
        """, (start_date, end_date)).fetchall()
        
        # 상위 종목 (최근 7일, 등장 횟수)
        top_symbols = self.db.conn.execute("""
            SELECT 
                dw.symbol,
                n.name,
                COUNT(*) as appearances,
                AVG(dw.condition_value) as avg_signal_strength,
                MAX(dw.condition_value) as max_signal_strength
            FROM daily_watchlist dw
            JOIN nasdaq_symbols n ON dw.symbol = n.symbol
            WHERE dw.date BETWEEN ? AND ?
            GROUP BY dw.symbol, n.name
            ORDER BY appearances DESC, avg_signal_strength DESC
            LIMIT 20
        """, (start_date, end_date)).fetchall()
        
        return {
            'period': f"{start_date} ~ {end_date}",
            'daily_counts': [
                {
                    'date': row[0],
                    'condition_type': row[1],
                    'count': row[2]
                } for row in daily_counts
            ],
            'sector_stats': [
                {
                    'sector': row[0],
                    'frequency': row[1],
                    'avg_signal_strength': row[2]
                } for row in sector_stats
            ],
            'top_symbols': [
                {
                    'symbol': row[0],
                    'name': row[1],
                    'appearances': row[2],
                    'avg_signal_strength': row[3],
                    'max_signal_strength': row[4]
                } for row in top_symbols
            ]
        }
    
    def scan_multiple_conditions(self, scan_date: date = None) -> Dict[str, List[Dict[str, Any]]]:
        """다양한 기술적 지표 조건으로 스캔"""
        if scan_date is None:
            scan_date = date.today()
            
        results = {}
        
        # 1. 볼린저 밴드 상단 터치
        results['bollinger_upper_touch'] = self.scan_bollinger_band_signals(scan_date)
        
        # 2. RSI 과매수 (70 이상)
        # TODO: RSI 계산 로직 추가 후 구현
        
        # 3. 거래량 급증 (평균 대비 2배 이상)
        # TODO: 거래량 분석 로직 추가 후 구현
        
        return results