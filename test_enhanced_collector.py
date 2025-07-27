#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
장애 대응 강화된 YFinanceCollector 테스트 스크립트
"""

import sys
import os
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
sys.path.insert(0, '/home/grey1/stock-kafka3/airflow/plugins')

from collect_stock_data_yfinance import YFinanceCollector
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def test_enhanced_collector():
    """장애 대응 강화된 컬렉터 테스트"""
    print("🧪 장애 대응 강화 YFinanceCollector 테스트 시작")
    
    try:
        # 컬렉터 생성
        collector = YFinanceCollector(
            host="localhost",  # 로컬 테스트
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow"
        )
        
        # 테스트 심볼들
        test_symbols = ['AAPL', 'MSFT']
        
        print(f"📊 테스트 심볼: {test_symbols}")
        
        # 배치 수집 테스트
        result = collector.collect_all_symbols(
            symbols=test_symbols,
            max_workers=1,
            period="1mo"
        )
        
        print(f"✅ 테스트 결과: {result}")
        
        # 통계 확인
        stats = collector.get_statistics()
        print(f"📈 통계: {stats}")
        
        collector.close()
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        print(f"상세 오류:\n{traceback.format_exc()}")

if __name__ == "__main__":
    test_enhanced_collector()
