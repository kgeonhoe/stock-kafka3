#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
백필 테스트 및 데이터 분석 도구
현재 DB 상태 확인 후 백필 계획 수립
"""

import sys
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
from database import DuckDBManager
from datetime import date, datetime, timedelta

def analyze_current_data_status():
    """현재 데이터 상태 분석"""
    
    print("📊 현재 데이터베이스 상태 분석")
    print("=" * 50)
    
    db = DuckDBManager('/home/grey1/stock-kafka3/data/duckdb/stock_data.db')
    
    try:
        # 1. 전체 데이터 개수
        total_result = db.execute_query('SELECT COUNT(*) as total FROM stock_data')
        if hasattr(total_result, 'iloc') and len(total_result) > 0:
            total_count = total_result.iloc[0]['total']
        else:
            total_count = total_result.data[0][0] if total_result.data else 0
        
        print(f"💾 총 주가 데이터: {total_count:,}개")
        
        # 2. 종목 수 확인
        symbol_count_result = db.execute_query('SELECT COUNT(DISTINCT symbol) as count FROM stock_data')
        if hasattr(symbol_count_result, 'iloc') and len(symbol_count_result) > 0:
            symbol_count = symbol_count_result.iloc[0]['count']
        else:
            symbol_count = symbol_count_result.data[0][0] if symbol_count_result.data else 0
        
        print(f"📋 수집된 종목 수: {symbol_count}개")
        
        if symbol_count > 0:
            avg_records = total_count // symbol_count
            print(f"📊 종목당 평균: {avg_records}개 레코드")
        
        # 3. 날짜 범위 확인
        date_range_result = db.execute_query('''
            SELECT 
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM stock_data
        ''')
        
        if hasattr(date_range_result, 'iloc') and len(date_range_result) > 0:
            row = date_range_result.iloc[0]
            earliest = row['earliest_date']
            latest = row['latest_date']
        else:
            row = date_range_result.data[0] if date_range_result.data else (None, None)
            earliest, latest = row
        
        print(f"📅 데이터 기간: {earliest} ~ {latest}")
        
        if earliest and latest:
            if isinstance(earliest, str):
                earliest = datetime.strptime(earliest, '%Y-%m-%d').date()
            if isinstance(latest, str):
                latest = datetime.strptime(latest, '%Y-%m-%d').date()
            
            total_days = (latest - earliest).days
            print(f"📊 총 기간: {total_days}일")
        
        # 4. 연도별 데이터 분포
        print("\n📅 연도별 데이터 분포:")
        yearly_result = db.execute_query('''
            SELECT 
                EXTRACT(YEAR FROM date) as year,
                COUNT(DISTINCT symbol) as symbols,
                COUNT(*) as records
            FROM stock_data 
            GROUP BY EXTRACT(YEAR FROM date)
            ORDER BY year
        ''')
        
        if hasattr(yearly_result, 'iloc'):
            for i in range(len(yearly_result)):
                row = yearly_result.iloc[i]
                year = int(row['year'])
                symbols = row['symbols']
                records = row['records']
                avg_per_symbol = records // symbols if symbols > 0 else 0
                print(f"  {year}년: {symbols}개 종목, {records:,}개 레코드 (평균 {avg_per_symbol}/종목)")
        else:
            for row in yearly_result.data:
                year, symbols, records = row
                avg_per_symbol = records // symbols if symbols > 0 else 0
                print(f"  {year}년: {symbols}개 종목, {records:,}개 레코드 (평균 {avg_per_symbol}/종목)")
        
        # 5. 백필 필요성 분석
        current_year = date.today().year
        backfill_years = [current_year - 5, current_year - 4, current_year - 3, current_year - 2]
        
        print(f"\n🎯 백필 분석 (대상 연도: {backfill_years}):")
        
        for year in backfill_years:
            year_data_result = db.execute_query('''
                SELECT 
                    COUNT(DISTINCT symbol) as symbols,
                    COUNT(*) as records
                FROM stock_data 
                WHERE EXTRACT(YEAR FROM date) = ?
            ''', (year,))
            
            if hasattr(year_data_result, 'iloc') and len(year_data_result) > 0:
                row = year_data_result.iloc[0]
                year_symbols = row['symbols']
                year_records = row['records']
            else:
                row = year_data_result.data[0] if year_data_result.data else (0, 0)
                year_symbols, year_records = row
            
            if year_symbols == 0:
                print(f"  {year}년: ❌ 데이터 없음 (백필 필요)")
            elif year_symbols < symbol_count * 0.8:  # 80% 미만이면 부족
                print(f"  {year}년: ⚠️ 부분 데이터 ({year_symbols}/{symbol_count} 종목, 백필 권장)")
            else:
                print(f"  {year}년: ✅ 충분한 데이터 ({year_symbols}/{symbol_count} 종목)")
        
        # 6. 샘플 종목 상세 분석
        print(f"\n🔍 상위 5개 종목 상세 분석:")
        
        top_symbols_result = db.execute_query('''
            SELECT 
                symbol,
                COUNT(*) as record_count,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM stock_data 
            GROUP BY symbol 
            ORDER BY record_count DESC 
            LIMIT 5
        ''')
        
        if hasattr(top_symbols_result, 'iloc'):
            for i in range(len(top_symbols_result)):
                row = top_symbols_result.iloc[i]
                symbol = row['symbol']
                count = row['record_count']
                earliest = row['earliest_date']
                latest = row['latest_date']
                print(f"  {symbol}: {count:,}개 ({earliest} ~ {latest})")
        else:
            for row in top_symbols_result.data:
                symbol, count, earliest, latest = row
                print(f"  {symbol}: {count:,}개 ({earliest} ~ {latest})")
        
        return {
            'total_records': total_count,
            'symbol_count': symbol_count,
            'earliest_date': earliest,
            'latest_date': latest,
            'needs_backfill': total_count < symbol_count * 1000  # 1000일 미만이면 백필 필요
        }
    
    except Exception as e:
        print(f"❌ 분석 중 오류: {e}")
        return None
    
    finally:
        db.close()

def estimate_backfill_time(symbol_count: int, years: int = 4):
    """백필 소요 시간 추정"""
    
    print(f"\n⏱️ 백필 소요 시간 추정:")
    print("=" * 30)
    
    # 추정 기준
    seconds_per_symbol_year = 5  # 종목당 연도당 5초 (API 지연 포함)
    
    total_operations = symbol_count * years
    estimated_seconds = total_operations * seconds_per_symbol_year
    
    estimated_minutes = estimated_seconds / 60
    estimated_hours = estimated_minutes / 60
    
    print(f"📊 추정 계산:")
    print(f"  - 대상: {symbol_count}개 종목 × {years}년 = {total_operations}회 작업")
    print(f"  - 예상 시간: {estimated_seconds:,.0f}초 ({estimated_minutes:.1f}분, {estimated_hours:.1f}시간)")
    
    if estimated_hours > 12:
        print(f"⚠️ 12시간 이상 소요 예상 - 배치 분할 권장")
    elif estimated_hours > 4:
        print(f"💡 4시간 이상 소요 예상 - 야간 실행 권장")
    else:
        print(f"✅ {estimated_hours:.1f}시간 소요 예상 - 즉시 실행 가능")
    
    # 최적화 제안
    print(f"\n🚀 최적화 방안:")
    print(f"  - 연도별 분할 실행: 각 연도당 {estimated_hours/years:.1f}시간")
    print(f"  - 종목별 배치 (50개씩): {symbol_count//50}개 배치, 각 배치당 {(50*years*seconds_per_symbol_year)/60:.1f}분")
    print(f"  - 순차 처리: API 안정성 확보")

def main():
    """메인 분석 실행"""
    
    print("🔍 백필 사전 분석 도구")
    print(f"📅 실행 시간: {datetime.now()}")
    print("=" * 60)
    
    # 현재 상태 분석
    status = analyze_current_data_status()
    
    if status:
        # 백필 시간 추정
        estimate_backfill_time(status['symbol_count'])
        
        print("\n" + "=" * 60)
        print("📝 분석 결과 요약:")
        print(f"✅ 현재 데이터: {status['total_records']:,}개 레코드, {status['symbol_count']}개 종목")
        print(f"📅 기간: {status['earliest_date']} ~ {status['latest_date']}")
        
        if status['needs_backfill']:
            print("🎯 백필 권장: 과거 4년 데이터 부족")
            print("📋 다음 단계: python backfill_historical_data.py 실행")
        else:
            print("✅ 충분한 데이터: 백필 불필요")
        
        print("\n💡 백필 실행 명령어:")
        print("cd /home/grey1/stock-kafka3")
        print("source venv/bin/activate")
        print("python backfill_historical_data.py")
    
    else:
        print("❌ 분석 실패 - 데이터베이스 연결 확인 필요")

if __name__ == "__main__":
    main()
