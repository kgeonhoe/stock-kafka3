#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime, date, timedelta
import time

class KISAPITester:
    """한국투자증권 API 테스터"""
    
    def __init__(self):
        """KIS API 테스터 초기화"""
        self.base_url = "https://openapi.koreainvestment.com:9443"
        
        # 실제 사용시에는 환경변수나 설정파일에서 가져와야 함
        self.app_key = "YOUR_APP_KEY"  # 실제 앱키 필요
        self.app_secret = "YOUR_APP_SECRET"  # 실제 시크릿 필요
        self.access_token = None
    
    def get_access_token(self):
        """액세스 토큰 발급"""
        
        print("🔑 한국투자증권 API 토큰 발급 테스트")
        
        url = f"{self.base_url}/oauth2/tokenP"
        
        headers = {
            "content-type": "application/json; charset=utf-8"
        }
        
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        print(f"📡 토큰 요청: {url}")
        print("⚠️ 실제 앱키/시크릿이 필요합니다")
        
        # 실제 API 키가 없으므로 시뮬레이션
        print("💡 API 구조 분석:")
        print("  - 엔드포인트: /oauth2/tokenP")
        print("  - 방식: POST (client_credentials)")
        print("  - 필수: appkey, appsecret")
        
        return False  # 실제 키가 없으므로 False
    
    def get_daily_stock_data(self, symbol: str, start_date: str, end_date: str):
        """일별 주식 데이터 조회 테스트"""
        
        print(f"\n📊 {symbol} 일별 데이터 조회 테스트")
        print(f"📅 조회 기간: {start_date} ~ {end_date}")
        
        # 일별 시세 조회 API 엔드포인트
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST03010100"  # 일별 시세 조회 TR_ID
        }
        
        params = {
            "fid_cond_mrkt_div_code": "J",  # 시장 구분 (J: 주식)
            "fid_input_iscd": symbol,        # 종목코드
            "fid_input_date_1": start_date,  # 조회 시작일
            "fid_input_date_2": end_date,    # 조회 종료일
            "fid_period_div_code": "D",      # 기간 구분 (D: 일별)
            "fid_org_adj_prc": "0"           # 수정주가 반영 여부
        }
        
        print("💡 KIS API 일별 데이터 구조:")
        print(f"  - 엔드포인트: {url}")
        print(f"  - TR_ID: FHKST03010100 (국내주식 일별 시세)")
        print(f"  - 파라미터:")
        for key, value in params.items():
            print(f"    • {key}: {value}")
        
        print("\n📋 응답 데이터 구조 (예상):")
        print("  - rt_cd: 응답코드")
        print("  - msg_cd: 메시지코드") 
        print("  - output1: 종목 기본정보")
        print("  - output2: 일별 시세 배열")
        print("    • stck_bsop_date: 영업일자")
        print("    • stck_oprc: 시가")
        print("    • stck_hgpr: 고가")
        print("    • stck_lwpr: 저가")
        print("    • stck_clpr: 종가")
        print("    • acml_vol: 누적거래량")
        
        return True
    
    def check_api_capabilities(self):
        """API 기능 확인"""
        
        print("\n🔍 한국투자증권 API 기능 분석:")
        print("=" * 50)
        
        print("1️⃣ 일별 데이터 수집:")
        print("  ✅ 가능 - FHKST03010100 (국내주식 일별 시세)")
        print("  📅 최대 조회 기간: 최근 30일~1년 (API 정책에 따라)")
        print("  🏢 대상: 코스피, 코스닥 전 종목")
        
        print("\n2️⃣ 실시간 데이터:")
        print("  ✅ 가능 - WebSocket 실시간 시세")
        print("  📡 방식: 웹소켓 연결")
        print("  🔄 빈도: 실시간 (체결시마다)")
        
        print("\n3️⃣ 분별 데이터:")
        print("  ✅ 가능 - FHKST03010200 (분별 시세)")
        print("  📊 단위: 1분, 3분, 5분, 10분, 15분, 30분, 60분")
        print("  📅 기간: 최근 며칠~몇주")
        
        print("\n4️⃣ API 제한사항:")
        print("  🚫 호출 제한: 초당 20건, 분당 1000건 (정확한 수치는 확인 필요)")
        print("  🔐 인증: OAuth2 토큰 (유효기간 24시간)")
        print("  📝 신청: 한국투자증권 계좌 + API 신청 필요")
        
        print("\n5️⃣ yfinance vs KIS 비교:")
        print("  yfinance:")
        print("    ✅ 무료, 신청 불필요")
        print("    ✅ 해외 주식 (NASDAQ, NYSE 등)")
        print("    ❌ 한국 주식 제한적")
        print("    ⚠️ 비공식 API (변경 위험)")
        
        print("  한국투자증권 KIS:")
        print("    ✅ 공식 API (안정성)")
        print("    ✅ 한국 주식 전체")
        print("    ✅ 실시간 데이터")
        print("    ❌ 계좌 개설 + 신청 필요")
        print("    ❌ 호출 제한 있음")

def test_date_range_strategies():
    """날짜 범위 수집 전략 테스트"""
    
    print("\n🎯 스마트 데이터 수집 전략:")
    print("=" * 50)
    
    print("1️⃣ 비어있는 날짜만 타겟 수집:")
    print("  📋 방법:")
    print("    1. DB에서 종목별 최신 날짜 조회")
    print("    2. 오늘까지의 비어있는 날짜 계산")
    print("    3. 연속된 구간별로 그룹화")
    print("    4. 각 구간별로 API 호출")
    
    print("\n  💡 구현 예시:")
    print("    # 기존: 전체 5년 수집 (1200+ API 호출)")
    print("    # 개선: 최근 3일만 수집 (1 API 호출)")
    print("    latest_date = get_latest_date('AAPL')")
    print("    if latest_date:")
    print("        start = latest_date + timedelta(days=1)")
    print("        end = date.today()")
    print("        data = ticker.history(start=start, end=end)")
    
    print("\n2️⃣ 연도별 백필 수집:")
    print("  📋 대용량 데이터 처리:")
    print("    1. 2020년 데이터만: start='2020-01-01', end='2020-12-31'")
    print("    2. 2021년 데이터만: start='2021-01-01', end='2021-12-31'")
    print("    3. 특정 월만: start='2024-07-01', end='2024-07-31'")
    
    print("\n3️⃣ 하이브리드 전략:")
    print("  📋 yfinance + KIS 조합:")
    print("    • yfinance: 해외 주식 (NASDAQ, NYSE)")
    print("    • KIS API: 한국 주식 (코스피, 코스닥)")
    print("    • 둘 다 증분 업데이트 방식 적용")

if __name__ == "__main__":
    print("🔍 한국투자증권 API 및 날짜 범위 수집 분석")
    print("=" * 60)
    
    # KIS API 테스트
    kis_tester = KISAPITester()
    kis_tester.get_access_token()
    kis_tester.get_daily_stock_data("005930", "20240701", "20240729")  # 삼성전자
    kis_tester.check_api_capabilities()
    
    # 날짜 범위 전략
    test_date_range_strategies()
    
    print("\n" + "=" * 60)
    print("✅ 분석 완료")
    print("\n🎯 답변 요약:")
    print("1️⃣ yfinance 특정 연도 수집: ✅ 가능")
    print("   - start/end 파라미터로 정확한 날짜 범위 지정")
    print("   - 비어있는 날짜만 타겟팅으로 효율성 대폭 향상")
    print("   - API 호출 최소화로 속도/안정성 개선")
    
    print("\n2️⃣ 한국투자증권 일일 데이터: ✅ 가능")
    print("   - 공식 API로 안정적 수집")
    print("   - 일별/분별/실시간 모두 지원")
    print("   - 단, 계좌 개설 + API 신청 필요")
    print("   - 호출 제한 있음 (관리 필요)")
