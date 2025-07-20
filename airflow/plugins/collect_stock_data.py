#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List

# 프로젝트 경로 추가
sys.path.insert(0, '/home/grey1/stock-kafka3')

from common.kis_api_client import KISAPIClient
from common.database import DuckDBManager

class StockDataCollector:
    """주식 데이터 수집 클래스"""
    
    def __init__(self, db_path: str = "/data/duckdb/stock_data.db"):
        """
        주식 데이터 수집 클래스 초기화
        
        Args:
            db_path: DuckDB 파일 경로
        """
        self.db = DuckDBManager(db_path)
        self.kis_client = KISAPIClient()
    
    async def collect_daily_data(self, symbol: str, days: int = 365) -> bool:
        """
        일별 주가 데이터 수집
        
        Args:
            symbol: 종목 심볼
            days: 수집할 기간(일)
            
        Returns:
            수집 성공 여부
        """
        # 시작일 계산 (days일 전)
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')
        
        try:
            # 일별 데이터 수집
            daily_data = await self.kis_client.get_nasdaq_daily(symbol, start_date, end_date)
            
            if not daily_data:
                print(f"⚠️ {symbol} 일별 데이터 없음")
                return False
            
            # DuckDB에 저장
            for data in daily_data:
                self.db.save_stock_data(data)
            
            print(f"✅ {symbol} 일별 데이터 {len(daily_data)}개 수집 완료")
            return True
        
        except Exception as e:
            print(f"❌ {symbol} 일별 데이터 수집 오류: {e}")
            return False
    
    async def collect_all_symbols_daily_data(self, max_workers: int = 5) -> Dict[str, int]:
        """
        모든 종목의 일별 데이터 수집
        
        Args:
            max_workers: 최대 동시 작업 수
            
        Returns:
            결과 통계 (총 종목 수, 성공 수, 실패 수)
        """
        # 활성 심볼 조회
        symbols = self.db.get_active_symbols()
        total_count = len(symbols)
        success_count = 0
        fail_count = 0
        
        print(f"🚀 총 {total_count}개 종목의 일별 데이터 수집 시작")
        
        # 심볼을 max_workers 크기의 청크로 나눔
        for i in range(0, total_count, max_workers):
            chunk = symbols[i:i + max_workers]
            
            # 각 청크에 대해 비동기 수집 작업 실행
            tasks = [self.collect_daily_data(symbol) for symbol in chunk]
            results = await asyncio.gather(*tasks)
            
            # 성공/실패 개수 업데이트
            for result in results:
                if result:
                    success_count += 1
                else:
                    fail_count += 1
            
            # 진행 상황 출력
            progress = min((i + max_workers) / total_count * 100, 100)
            print(f"📊 진행률: {progress:.1f}% ({i + len(chunk)}/{total_count})")
        
        print(f"✅ 일별 데이터 수집 완료: 총 {total_count}개 중 {success_count}개 성공, {fail_count}개 실패")
        
        return {
            "total": total_count,
            "success": success_count,
            "fail": fail_count
        }
    
    def close(self):
        """리소스 정리"""
        self.db.close()

# 에어플로우에서 호출하는 함수
def collect_stock_data_task(**kwargs):
    """
    에어플로우 태스크 함수: 모든 종목의 일별 데이터 수집
    
    Args:
        **kwargs: 에어플로우 컨텍스트
        
    Returns:
        결과 통계
    """
    collector = StockDataCollector()
    
    try:
        # 비동기 루프 생성 및 실행
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(collector.collect_all_symbols_daily_data())
        loop.close()
        
        collector.close()
        return results
    
    except Exception as e:
        collector.close()
        print(f"❌ 일별 데이터 수집 작업 오류: {e}")
        return {
            "total": 0,
            "success": 0,
            "fail": 0,
            "error": str(e)
        }
