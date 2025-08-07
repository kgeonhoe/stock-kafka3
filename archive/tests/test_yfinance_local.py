#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
yfinance 수집기 로컬 테스트 스크립트
"""

import sys
import os
from pathlib import Path

# 프로젝트 경로 추가
sys.path.insert(0, 'common')
sys.path.insert(0, 'airflow/plugins')

try:
    from collect_stock_data_yfinance import YFinanceCollector
    from database import DuckDBManager
except ImportError as e:
    print(f"❌ 임포트 오류: {e}")
    print("필요한 패키지를 설치해주세요: pip install yfinance pandas duckdb")
    sys.exit(1)

def test_single_symbol():
    """단일 종목 테스트"""
    print("🧪 단일 종목 테스트 시작...")
    
    # 테스트용 데이터베이스 파일
    test_db_path = "test_stock_data.db"
    
    try:
        # 컬렉터 초기화
        collector = YFinanceCollector(db_path=test_db_path)
        
        # AAPL 테스트
        symbol = "AAPL"
        print(f"📊 {symbol} 데이터 수집 테스트...")
        
        success = collector.collect_stock_data(symbol, period="1mo")  # 1개월 데이터만
        
        if success:
            print(f"✅ {symbol} 수집 성공!")
        else:
            print(f"❌ {symbol} 수집 실패")
        
        # 리소스 정리
        collector.close()
        
        # 테스트 DB 파일 삭제
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
            print("🧹 테스트 파일 정리 완료")
            
    except Exception as e:
        print(f"💥 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

def test_multiple_symbols():
    """여러 종목 테스트"""
    print("\n🧪 여러 종목 병렬 처리 테스트 시작...")
    
    # 테스트용 데이터베이스 파일
    test_db_path = "test_stock_data_multi.db"
    
    try:
        # 컬렉터 초기화
        collector = YFinanceCollector(db_path=test_db_path)
        
        # 테스트 종목들 (소수로 제한)
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        
        print(f"📊 {len(test_symbols)}개 종목 병렬 수집 테스트...")
        print(f"🎯 대상 종목: {test_symbols}")
        
        # 병렬 수집 실행
        result = collector.collect_all_symbols(
            symbols=test_symbols, 
            period="1mo",  # 1개월 데이터만
            max_workers=3  # 워커 수 제한
        )
        
        print(f"\n📊 테스트 결과:")
        print(f"   총 종목: {result['total']}")
        print(f"   성공: {result['success']}")
        print(f"   실패: {result['fail']}")
        print(f"   소요시간: {result['elapsed_time']}초")
        
        # 리소스 정리
        collector.close()
        
        # 테스트 DB 파일 삭제
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
            print("🧹 테스트 파일 정리 완료")
            
    except Exception as e:
        print(f"💥 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

def test_database_connection():
    """데이터베이스 연결 테스트"""
    print("\n🧪 데이터베이스 연결 테스트...")
    
    try:
        db = DuckDBManager("test_db_conn.db")
        print("✅ DuckDB 연결 성공")
        
        # 간단한 쿼리 테스트
        result = db.conn.execute("SELECT 1 as test").fetchall()
        print(f"✅ 쿼리 테스트 성공: {result}")
        
        db.close()
        print("✅ 연결 종료 성공")
        
        # 테스트 파일 삭제
        if os.path.exists("test_db_conn.db"):
            os.remove("test_db_conn.db")
            print("🧹 테스트 파일 정리 완료")
            
    except Exception as e:
        print(f"❌ 데이터베이스 테스트 실패: {e}")

if __name__ == "__main__":
    print("🚀 yfinance 수집기 로컬 테스트 시작!")
    print("=" * 50)
    
    # 1. 데이터베이스 연결 테스트
    test_database_connection()
    
    # 2. 단일 종목 테스트
    test_single_symbol()
    
    # 3. 여러 종목 테스트
    test_multiple_symbols()
    
    print("\n🎉 모든 테스트 완료!")
    print("=" * 50)
