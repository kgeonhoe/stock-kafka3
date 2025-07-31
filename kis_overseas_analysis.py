#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
한국투자증권 KIS API - 해외 주식 데이터 수집 가능성 분석
"""

class KISOverseaStockAnalysis:
    """KIS API 해외 주식 분석 클래스"""
    
    def __init__(self):
        """KIS 해외 주식 API 분석 초기화"""
        self.base_url = "https://openapi.koreainvestment.com:9443"
    
    def analyze_overseas_apis(self):
        """해외 주식 관련 API 분석"""
        
        print("🌍 KIS API 해외 주식 데이터 수집 가능성 분석")
        print("=" * 60)
        
        print("1️⃣ 해외 주식 일별 시세 조회:")
        print("  📡 API: /uapi/overseas-price/v1/quotations/dailyprice")
        print("  🔍 TR_ID: HHDFS76240000 (미국), HHDFS76950000 (홍콩) 등")
        print("  📅 기능: 해외 주식 일별 OHLCV 데이터")
        print("  🎯 대상: 미국(NASDAQ, NYSE), 홍콩, 일본, 유럽 등")
        
        print("\n  📋 요청 파라미터:")
        print("    • AUTH: 인증 토큰")
        print("    • APPKEY: 앱키")
        print("    • APPSECRET: 앱시크릿")
        print("    • TR_ID: 거래소별 TR_ID")
        print("    • SYMB: 종목 심볼 (예: AAPL)")
        print("    • GUBN: 일별(0), 주별(1), 월별(2)")
        print("    • BYMD: 기준일자 (YYYYMMDD)")
        print("    • MODP: 수정주가 반영 여부")
        
        print("\n  📊 응답 데이터:")
        print("    • rsym: 실시간 심볼")
        print("    • zdiv: 소수점 자리수")
        print("    • base: 기준가")
        print("    • pvol: 거래량")
        print("    • output2 배열:")
        print("      - xymd: 일자")
        print("      - open: 시가")
        print("      - high: 고가")
        print("      - low: 저가")
        print("      - clos: 종가")
        print("      - tvol: 거래량")
        
        print("\n2️⃣ 해외 주식 분별 시세 조회:")
        print("  📡 API: /uapi/overseas-price/v1/quotations/inquire-time-itemchartprice")
        print("  🔍 TR_ID: HHDFS76410000")
        print("  📅 기능: 분별 차트 데이터")
        print("  ⏰ 단위: 1분, 3분, 5분, 10분, 15분, 30분, 60분")
        
        print("\n3️⃣ 제약사항 및 한계:")
        print("  🚫 과거 데이터 제한:")
        print("    • 일별: 최대 100일 (약 3-4개월)")
        print("    • 분별: 최대 120건 (1일~1주일)")
        print("    • 장기 히스토리: 제한적")
        print("  ")
        print("  🔄 호출 제한:")
        print("    • 초당 20건 제한")
        print("    • 분당 1000건 제한")
        print("    • 일일 제한 있음 (정확한 수치 미공개)")
        
        print("\n4️⃣ yfinance vs KIS 해외 주식 비교:")
        
        print("\n  📊 yfinance 장점:")
        print("    ✅ 무료, 무제한")
        print("    ✅ 5년+ 장기 데이터")
        print("    ✅ 신청/승인 불필요")
        print("    ✅ 간단한 API (period='5y')")
        print("    ✅ 전세계 거래소 지원")
        
        print("  📊 yfinance 단점:")
        print("    ❌ 비공식 API (변경 위험)")
        print("    ❌ 실시간 데이터 제한")
        print("    ❌ 일부 종목 누락 가능")
        print("    ⚠️ 호출 제한 (rate limiting)")
        
        print("\n  📊 KIS 해외 주식 장점:")
        print("    ✅ 공식 API (안정성)")
        print("    ✅ 실시간 데이터")
        print("    ✅ 정확한 데이터")
        print("    ✅ 분별 데이터 지원")
        
        print("  📊 KIS 해외 주식 단점:")
        print("    ❌ 계좌 개설 필수")
        print("    ❌ API 신청/승인 필요")
        print("    ❌ 과거 데이터 제한 (최대 100일)")
        print("    ❌ 호출 제한 (초당 20건)")
        print("    ❌ 5년 히스토리 불가능")
        
        return True
    
    def recommend_strategy(self):
        """최적 전략 추천"""
        
        print("\n🎯 해외 주식 데이터 수집 최적 전략:")
        print("=" * 60)
        
        print("1️⃣ 현재 상황 (최근 1년만 있는 경우):")
        print("  🔧 백필(Backfill) 전략:")
        print("    • yfinance로 과거 4년 데이터 백필")
        print("    • 연도별 분할 수집으로 안정성 확보")
        print("    • 예: 2020년 → 2021년 → 2022년 → 2023년")
        
        print("\n2️⃣ 하이브리드 아키텍처:")
        print("  📊 역할 분담:")
        print("    • yfinance: 과거 데이터 + 일일 증분 업데이트")
        print("    • KIS API: 실시간 데이터 + 분별 데이터")
        print("    • 보완적 사용으로 각각의 장점 활용")
        
        print("\n3️⃣ 단계별 구현 계획:")
        print("  Phase 1 (즉시): yfinance 증분 업데이트 최적화")
        print("    ✅ 현재 진행 중")
        print("    • 18시간 → 5분으로 단축")
        print("    • API 호출 최소화")
        print("    • 안정성 확보")
        
        print("\n  Phase 2 (1주일 내): 과거 데이터 백필")
        print("    🎯 목표: 5년 전체 데이터 확보")
        print("    • 연도별 분할 수집")
        print("    • 비어있는 기간만 타겟팅")
        print("    • 배치 처리로 안정성 확보")
        
        print("\n  Phase 3 (1개월 내): KIS API 추가")
        print("    🎯 목표: 실시간 + 분별 데이터")
        print("    • 계좌 개설 + API 신청")
        print("    • 실시간 웹소켓 연결")
        print("    • 분별 데이터 수집 추가")
        
        print("\n4️⃣ 백필 전략 상세:")
        print("  📅 연도별 백필 코드 예시:")
        print("    # 2020년 백필")
        print("    data_2020 = ticker.history(start='2020-01-01', end='2020-12-31')")
        print("    ")
        print("    # 2021년 백필")
        print("    data_2021 = ticker.history(start='2021-01-01', end='2021-12-31')")
        print("    ")
        print("    # 비어있는 구간만 타겟팅")
        print("    missing_ranges = find_missing_date_ranges(symbol)")
        print("    for start, end in missing_ranges:")
        print("        data = ticker.history(start=start, end=end)")
        
        print("\n5️⃣ 성능 예상:")
        print("  📊 백필 성능 (종목당):")
        print("    • 1년 데이터: ~250일, 1회 API 호출, ~3초")
        print("    • 5년 백필: 5회 API 호출, ~15초")
        print("    • 1000종목 백필: ~4시간 (배치 처리)")
        
        print("\n  📊 일일 증분 업데이트:")
        print("    • 현재: 18시간 → 개선 후: 5분")
        print("    • API 호출: 1200+ → 1-3회")
        print("    • 안정성: 대폭 향상")

def main():
    """메인 분석 실행"""
    
    print("🔍 KIS API 해외 주식 데이터 분석")
    print("현재 상황: 최근 1년 데이터만 보유")
    print("목표: 5년 전체 데이터 확보 + 실시간 업데이트")
    print("\n" + "=" * 60)
    
    analyzer = KISOverseaStockAnalysis()
    
    # API 기능 분석
    analyzer.analyze_overseas_apis()
    
    # 전략 추천
    analyzer.recommend_strategy()
    
    print("\n" + "=" * 60)
    print("📝 결론:")
    print("1️⃣ KIS API 해외 주식 과거 데이터: ❌ 제한적 (최대 100일)")
    print("2️⃣ yfinance 5년 백필: ✅ 가능하고 효율적")
    print("3️⃣ 하이브리드 전략: ✅ 최적 (yfinance + KIS)")
    print("4️⃣ 우선순위: yfinance 최적화 → 백필 → KIS 추가")
    print("\n💡 현재 Airflow 수집 완료 후 백필 전략 구현 추천!")

if __name__ == "__main__":
    main()
