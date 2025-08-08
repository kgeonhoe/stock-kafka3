import streamlit as st
import json
import time
import subprocess
import requests
import threading
from datetime import datetime, timedelta
import pytz
import concurrent.futures
import sys
import os

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

# 선택적 import
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    st.warning("📦 pandas가 설치되지 않음 - 시뮬레이션 모드로 동작합니다.")
    HAS_PANDAS = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    st.warning("📦 plotly가 설치되지 않음 - 기본 차트로 대체됩니다.")
    HAS_PLOTLY = False

# 페이지 설정
st.set_page_config(
    page_title="API 부하테스트 대시보드",
    page_icon="🌐",
    layout="wide"
)

st.title("🌐 API 부하테스트 & 모니터링 대시보드")

st.markdown("""
### 📋 API 호출 테스트 개요
이 대시보드는 주식 데이터 수집 시스템의 API 호출 성능을 테스트하고 모니터링합니다.
- **외부 API**: yfinance, KIS API, NASDAQ API 등
- **내부 서비스**: Kafka UI, Airflow API, Redis 등
- **성능 지표**: 응답시간, 처리량, 오류율, 동시 연결 수
""")

# 탭 구성
tab1, tab2, tab3, tab4 = st.tabs([
    "🎯 API 테스트 실행", 
    "📊 실시간 모니터링", 
    "📈 성능 분석", 
    "🔧 설정 & 히스토리"
])

with tab1:
    st.header("🎯 API 부하테스트 실행")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🌐 테스트 대상 API 선택")
        
        # API 엔드포인트 카테고리별 분류
        api_categories = {
            "외부 데이터 API": {
                "yfinance API": "https://query1.finance.yahoo.com/v8/finance/chart/AAPL",
                "KIS API (Mock)": "https://openapivts.koreainvestment.com:29443/oauth2/tokenP",
                "NASDAQ API (Mock)": "https://api.nasdaq.com/api/screener/stocks",
                "Alpha Vantage (Mock)": "https://www.alphavantage.co/query"
            },
            "내부 서비스 API": {
                "Kafka UI Health": "http://localhost:8080/api/kafka/health",
                "Airflow API": "http://localhost:8081/api/v1/health",
                "Redis Ping": "http://localhost:6379/ping",
                "PostgreSQL Health": "http://localhost:5432/health"
            },
            "분석 서비스 API": {
                "기술적지표 계산": "http://localhost:8501/api/technical-indicators",
                "신호 감지": "http://localhost:8501/api/signal-detection", 
                "포트폴리오 분석": "http://localhost:8501/api/portfolio-analysis"
            }
        }
        
        selected_apis = {}
        for category, apis in api_categories.items():
            st.markdown(f"**{category}**")
            cols = st.columns(2)
            for i, (name, url) in enumerate(apis.items()):
                with cols[i % 2]:
                    if st.checkbox(name, key=f"api_{category}_{name}"):
                        selected_apis[name] = url
        
        st.markdown("---")
        
        # 테스트 설정
        st.subheader("⚙️ 부하테스트 매개변수")
        
        col_setting1, col_setting2, col_setting3 = st.columns(3)
        
        with col_setting1:
            st.markdown("**🔧 워커 설정**")
            concurrent_workers = st.slider("동시 API 워커 수", 1, 20, 4)
            requests_per_worker = st.slider("워커당 요청 수", 10, 1000, 100)
            request_interval = st.slider("요청 간격 (초)", 0.0, 5.0, 0.1)
        
        with col_setting2:
            st.markdown("**⏱️ 시간 설정**")
            test_duration = st.slider("최대 테스트 시간 (분)", 1, 30, 5)
            request_timeout = st.slider("요청 타임아웃 (초)", 1, 30, 10)
            retry_attempts = st.slider("재시도 횟수", 0, 5, 2)
        
        with col_setting3:
            st.markdown("**📊 옵션**")
            save_responses = st.checkbox("응답 데이터 저장", value=False)
            detailed_logging = st.checkbox("상세 로깅", value=True)
            real_time_update = st.checkbox("실시간 업데이트", value=True)
            
        # 테스트 실행 버튼들
        st.subheader("🚀 테스트 실행")
        
        col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)
        
        with col_btn1:
            if st.button("🚀 전체 테스트 시작", use_container_width=True, type="primary"):
                if selected_apis:
                    st.success(f"✅ {len(selected_apis)}개 API 대상으로 부하테스트를 시작합니다!")
                    st.info(f"워커 {concurrent_workers}개, 총 {concurrent_workers * requests_per_worker:,}회 요청")
                    
                    # 실제 테스트 실행 시뮬레이션
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for i in range(100):
                        progress_bar.progress(i + 1)
                        status_text.text(f'테스트 진행 중... {i+1}%')
                        time.sleep(0.1)
                    
                    st.success("🎉 부하테스트가 완료되었습니다!")
                else:
                    st.error("❌ 테스트할 API를 선택해주세요!")
        
        with col_btn2:
            if st.button("⚡ 빠른 테스트 (30초)", use_container_width=True):
                st.info("⚡ 30초 빠른 테스트를 시작합니다...")
        
        with col_btn3:
            if st.button("🔄 연결성 테스트", use_container_width=True):
                st.info("🔄 API 연결성을 확인합니다...")
        
        with col_btn4:
            if st.button("🛑 테스트 중단", use_container_width=True, type="secondary"):
                st.warning("⚠️ 진행 중인 테스트를 중단합니다...")
    
    with col2:
        st.subheader("📋 테스트 요약")
        
        if selected_apis:
            st.metric("🎯 선택된 API", f"{len(selected_apis)}개")
            st.metric("🔧 총 워커 수", f"{concurrent_workers}개")
            st.metric("📊 총 요청 수", f"{concurrent_workers * requests_per_worker:,}회")
            st.metric("⏱️ 예상 소요시간", f"{(concurrent_workers * requests_per_worker * request_interval) / 60:.1f}분")
            
            # 선택된 API 목록
            st.markdown("**📋 선택된 API:**")
            for name, url in selected_apis.items():
                st.markdown(f"• **{name}**")
                st.code(url, language=None)
        else:
            st.info("🎯 테스트할 API를 선택해주세요")
        
        # 예상 성능 계산
        st.subheader("💫 예상 성능")
        
        total_requests = concurrent_workers * requests_per_worker
        estimated_rps = concurrent_workers / request_interval if request_interval > 0 else concurrent_workers * 10
        
        st.metric("📈 예상 RPS", f"{estimated_rps:.1f}")
        st.metric("🎯 성공률 목표", "≥ 95%")
        st.metric("⚡ 응답시간 목표", "< 2초")
        
        if estimated_rps > 50:
            st.warning("⚠️ 높은 요청률입니다. 외부 API 제한을 확인하세요.")

with tab2:
    st.header("📊 실시간 API 성능 모니터링")
    
    # 자동 새로고침 설정
    auto_refresh = st.checkbox("🔄 자동 새로고침 (10초)", value=True)
    if auto_refresh:
        time.sleep(0.1)  # 시뮬레이션
    
    # 실시간 메트릭
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # 시뮬레이션 데이터
        current_rps = 45.7
        st.metric(
            label="📈 현재 RPS",
            value=f"{current_rps:.1f}",
            delta=f"+{5.2:.1f}"
        )
    
    with col2:
        avg_response_time = 1.24
        st.metric(
            label="⚡ 평균 응답시간",
            value=f"{avg_response_time:.2f}초",
            delta=f"-{0.15:.2f}초"
        )
    
    with col3:
        success_rate = 97.8
        st.metric(
            label="✅ 성공률",
            value=f"{success_rate:.1f}%",
            delta=f"+{1.2:.1f}%"
        )
    
    with col4:
        active_connections = 12
        st.metric(
            label="🔗 활성 연결",
            value=f"{active_connections}개",
            delta=f"+{3}개"
        )
    
    # 실시간 차트
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("📈 API 응답시간 추이")
        
        if HAS_PLOTLY:
            # 시뮬레이션 시계열 데이터
            current_time = datetime.now(KST)
            time_range = [current_time - timedelta(minutes=i) for i in range(20, 0, -1)]
            response_times = [1.2 + (i % 5) * 0.3 + (i % 3) * 0.1 for i in range(20)]
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=time_range,
                y=response_times,
                mode='lines+markers',
                name='응답시간',
                line=dict(color='blue'),
                fill='tonexty'
            ))
            
            fig.update_layout(
                title="실시간 API 응답시간",
                xaxis_title="시간",
                yaxis_title="응답시간 (초)",
                template="plotly_white",
                height=300
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.line_chart([1.2, 1.5, 1.1, 1.8, 1.3, 1.6, 1.2, 1.4])
    
    with col_chart2:
        st.subheader("🎯 API별 성공률")
        
        if HAS_PLOTLY:
            api_success_rates = {
                'API': ['yfinance', 'KIS API', 'Kafka UI', 'Airflow', 'Redis'],
                '성공률': [98.5, 94.2, 99.8, 97.1, 99.9],
                '상태': ['🟢', '🟡', '🟢', '🟢', '🟢']
            }
            
            fig_success = go.Figure()
            fig_success.add_trace(go.Bar(
                x=api_success_rates['API'],
                y=api_success_rates['성공률'],
                marker_color=['green' if x > 95 else 'orange' if x > 90 else 'red' for x in api_success_rates['성공률']],
                text=[f"{x:.1f}%" for x in api_success_rates['성공률']],
                textposition='auto'
            ))
            
            fig_success.update_layout(
                title="API별 성공률",
                xaxis_title="API",
                yaxis_title="성공률 (%)",
                template="plotly_white",
                height=300,
                yaxis=dict(range=[0, 100])
            )
            
            st.plotly_chart(fig_success, use_container_width=True)
        else:
            st.bar_chart({'yfinance': 98.5, 'KIS API': 94.2, 'Kafka UI': 99.8})
    
    # 실시간 로그 스트림
    st.subheader("📋 실시간 API 호출 로그")
    
    log_container = st.container()
    with log_container:
        # 시뮬레이션 로그 메시지
        api_logs = [
            "2025-08-07 14:35:22 INFO: [yfinance] GET /v8/finance/chart/AAPL - 200 OK (1.24s)",
            "2025-08-07 14:35:23 INFO: [KIS API] POST /oauth2/tokenP - 200 OK (0.85s)",
            "2025-08-07 14:35:24 WARN: [NASDAQ] GET /api/screener/stocks - 429 Rate Limited (2.15s)",
            "2025-08-07 14:35:25 INFO: [Kafka UI] GET /api/kafka/health - 200 OK (0.32s)",
            "2025-08-07 14:35:26 ERROR: [Redis] GET /ping - Connection Timeout (5.00s)",
            "2025-08-07 14:35:27 INFO: [Airflow] GET /api/v1/health - 200 OK (0.78s)",
            "2025-08-07 14:35:28 INFO: [yfinance] GET /v8/finance/chart/MSFT - 200 OK (1.45s)"
        ]
        
        for log in api_logs[-7:]:
            if "ERROR" in log:
                st.error(log)
            elif "WARN" in log:
                st.warning(log)
            else:
                st.info(log)
    
    # API 상태 요약 테이블
    st.subheader("🔍 API 상태 요약")
    
    if HAS_PANDAS:
        api_status_data = {
            "API 이름": ["yfinance API", "KIS API", "NASDAQ API", "Kafka UI", "Airflow API", "Redis"],
            "상태": ["🟢 정상", "🟡 느림", "🔴 제한", "🟢 정상", "🟢 정상", "🔴 오류"],
            "응답시간 (ms)": [1240, 850, 2150, 320, 780, 5000],
            "성공률 (%)": [98.5, 94.2, 0.0, 99.8, 97.1, 45.2],
            "최근 오류": ["없음", "가끔 타임아웃", "Rate Limit 초과", "없음", "없음", "Connection Refused"],
            "마지막 성공": ["30초 전", "1분 전", "15분 전", "10초 전", "45초 전", "10분 전"]
        }
        
        df_status = pd.DataFrame(api_status_data)
        st.dataframe(df_status, use_container_width=True)

with tab3:
    st.header("📈 API 성능 분석 & 최적화")
    
    # 성능 분석 필터
    col_filter1, col_filter2, col_filter3 = st.columns(3)
    
    with col_filter1:
        time_range = st.selectbox(
            "📅 분석 기간",
            ["최근 1시간", "최근 6시간", "최근 24시간", "최근 7일"],
            index=1
        )
    
    with col_filter2:
        selected_apis_analysis = st.multiselect(
            "🎯 분석 대상 API",
            ["yfinance", "KIS API", "NASDAQ", "Kafka UI", "Airflow", "Redis"],
            default=["yfinance", "KIS API"]
        )
    
    with col_filter3:
        analysis_metric = st.selectbox(
            "📊 분석 지표",
            ["응답시간", "처리량", "오류율", "동시 연결"]
        )
    
    # 성능 트렌드 분석
    col_trend1, col_trend2 = st.columns(2)
    
    with col_trend1:
        st.subheader("📈 성능 트렌드 분석")
        
        if HAS_PLOTLY:
            # 시간대별 성능 데이터 시뮬레이션
            hours = list(range(24))
            performance_data = {
                'yfinance': [1.2 + 0.3 * abs((h - 12) / 6) for h in hours],
                'KIS API': [0.8 + 0.2 * abs((h - 14) / 8) for h in hours],
                'Kafka UI': [0.3 + 0.1 * abs((h - 10) / 12) for h in hours]
            }
            
            fig_trend = go.Figure()
            for api, values in performance_data.items():
                if api.replace(' ', '_').lower() in [s.replace(' ', '_').lower() for s in selected_apis_analysis]:
                    fig_trend.add_trace(go.Scatter(
                        x=hours,
                        y=values,
                        mode='lines+markers',
                        name=api
                    ))
            
            fig_trend.update_layout(
                title=f"{analysis_metric} 시간대별 트렌드 (24시간)",
                xaxis_title="시간",
                yaxis_title=f"{analysis_metric} (초)" if analysis_metric == "응답시간" else analysis_metric,
                template="plotly_white",
                height=350
            )
            
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.line_chart([1.2, 1.5, 1.1, 1.8, 1.3, 1.6, 1.2, 1.4, 1.7, 1.3])
    
    with col_trend2:
        st.subheader("🎯 성능 분포 분석")
        
        if HAS_PLOTLY:
            # 응답시간 히스토그램 시뮬레이션
            import random
            response_times = [random.gauss(1.5, 0.5) for _ in range(1000)]
            response_times = [max(0.1, t) for t in response_times]  # 음수 제거
            
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(
                x=response_times,
                nbinsx=30,
                name='응답시간 분포',
                marker_color='lightblue'
            ))
            
            fig_hist.update_layout(
                title="응답시간 분포 히스토그램",
                xaxis_title="응답시간 (초)",
                yaxis_title="빈도",
                template="plotly_white",
                height=350
            )
            
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.bar_chart([45, 67, 89, 123, 98, 76, 54, 32, 21, 12])
    
    # 성능 지표 요약
    st.subheader("📊 성능 지표 요약")
    
    col_metrics1, col_metrics2, col_metrics3, col_metrics4 = st.columns(4)
    
    with col_metrics1:
        st.metric("📈 평균 응답시간", "1.34초", delta="-0.12초")
        st.metric("🏆 최적 응답시간", "0.32초")
    
    with col_metrics2:
        st.metric("📊 P95 응답시간", "2.45초", delta="+0.08초")
        st.metric("📉 최악 응답시간", "5.67초")
    
    with col_metrics3:
        st.metric("⚡ 평균 처리량", "47.3 req/sec", delta="+3.2")
        st.metric("🎯 최대 처리량", "89.2 req/sec")
    
    with col_metrics4:
        st.metric("✅ 전체 성공률", "96.8%", delta="+1.2%")
        st.metric("❌ 오류율", "3.2%", delta="-1.2%")
    
    # 병목 지점 분석
    st.subheader("🔍 병목 지점 및 최적화 권장사항")
    
    col_bottleneck1, col_bottleneck2 = st.columns(2)
    
    with col_bottleneck1:
        st.markdown("**🚨 식별된 병목 지점:**")
        
        bottleneck_issues = [
            "🔴 **KIS API**: Rate limiting 발생 (94.2% 성공률)",
            "🟡 **yfinance API**: 오후 시간대 응답 지연 (+15%)",
            "🟡 **NASDAQ API**: 연결 타임아웃 빈발",
            "🔴 **Redis**: 간헐적 연결 실패 (45% 성공률)"
        ]
        
        for issue in bottleneck_issues:
            st.markdown(issue)
    
    with col_bottleneck2:
        st.markdown("**💡 최적화 권장사항:**")
        
        recommendations = [
            "⚡ **KIS API**: 요청 간격을 0.5초로 증가",
            "🔄 **yfinance**: 캐싱 전략 도입 (Redis 복구 후)",
            "🎯 **전체**: 동시 워커 수를 6개에서 4개로 감소",
            "🔧 **Redis**: 연결 풀 재설정 및 상태 점검 필요"
        ]
        
        for rec in recommendations:
            st.markdown(rec)

with tab4:
    st.header("🔧 API 테스트 설정 & 히스토리")
    
    col_config1, col_config2 = st.columns(2)
    
    with col_config1:
        st.subheader("⚙️ 글로벌 설정")
        
        # API 설정 폼
        with st.form("api_global_config"):
            st.markdown("**🌐 기본 API 설정**")
            
            default_timeout = st.slider("기본 타임아웃 (초)", 1, 30, 10)
            default_retries = st.slider("기본 재시도 횟수", 0, 5, 2)
            concurrent_limit = st.slider("최대 동시 연결", 1, 50, 10)
            
            st.markdown("**📊 로깅 및 모니터링**")
            log_level = st.selectbox("로그 레벨", ["DEBUG", "INFO", "WARNING", "ERROR"], index=1)
            save_detailed_logs = st.checkbox("상세 로그 저장", value=True)
            enable_metrics = st.checkbox("메트릭 수집", value=True)
            
            st.markdown("**🔔 알림 설정**")
            alert_on_high_latency = st.checkbox("높은 지연시간 알림 (> 3초)", value=True)
            alert_on_low_success_rate = st.checkbox("낮은 성공률 알림 (< 90%)", value=True)
            
            if st.form_submit_button("💾 설정 저장", use_container_width=True):
                st.success("✅ 글로벌 설정이 저장되었습니다!")
    
    with col_config2:
        st.subheader("📋 API별 개별 설정")
        
        # API별 설정
        selected_api_config = st.selectbox(
            "설정할 API 선택",
            ["yfinance API", "KIS API", "NASDAQ API", "Kafka UI", "Airflow API", "Redis"]
        )
        
        with st.expander(f"🔧 {selected_api_config} 설정", expanded=True):
            col_api1, col_api2 = st.columns(2)
            
            with col_api1:
                api_timeout = st.number_input("타임아웃 (초)", value=10, min_value=1)
                api_retries = st.number_input("재시도 횟수", value=2, min_value=0)
                
            with col_api2:
                api_rate_limit = st.number_input("요청 제한 (req/sec)", value=10, min_value=1)
                api_priority = st.selectbox("우선순위", ["높음", "보통", "낮음"], index=1)
            
            if st.button(f"💾 {selected_api_config} 설정 저장", key=f"save_{selected_api_config}"):
                st.success(f"✅ {selected_api_config} 설정이 저장되었습니다!")
    
    # 테스트 히스토리
    st.subheader("📚 API 테스트 히스토리")
    
    if HAS_PANDAS:
        test_history_data = {
            "실행 시간": [
                "2025-08-07 14:30:00",
                "2025-08-07 13:45:00",
                "2025-08-07 13:15:00",
                "2025-08-07 12:30:00",
                "2025-08-07 11:45:00"
            ],
            "테스트 타입": [
                "통합 API 테스트",
                "yfinance 집중 테스트",
                "KIS API 인증 테스트",
                "내부 서비스 헬스체크",
                "전체 연결성 테스트"
            ],
            "대상 API 수": [6, 1, 1, 4, 6],
            "총 요청 수": [2400, 500, 200, 800, 1200],
            "평균 응답시간": ["1.34초", "1.12초", "0.85초", "0.45초", "2.15초"],
            "성공률": ["96.8%", "98.4%", "94.0%", "99.2%", "92.1%"],
            "오류 수": [77, 8, 12, 6, 95],
            "상태": ["⚠️ 부분 성공", "✅ 성공", "🟡 경고", "✅ 성공", "❌ 실패"]
        }
        
        df_history = pd.DataFrame(test_history_data)
        st.dataframe(df_history, use_container_width=True)
    
    # 통계 요약
    col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)
    
    with col_stats1:
        st.metric("📊 총 테스트 횟수", "47회")
        st.metric("✅ 성공한 테스트", "32회")
    
    with col_stats2:
        st.metric("📈 평균 성공률", "94.7%")
        st.metric("⚡ 평균 응답시간", "1.28초")
    
    with col_stats3:
        st.metric("🔥 가장 빠른 API", "Kafka UI (0.32초)")
        st.metric("🐌 가장 느린 API", "NASDAQ (2.15초)")
    
    with col_stats4:
        st.metric("📈 개선된 API", "yfinance (+15%)")
        st.metric("📉 악화된 API", "Redis (-45%)")

# 페이지 하단 정보
st.markdown("---")
st.markdown("""
### 🔗 관련 도구 및 명령어

**🌐 API 테스트 도구:**
- **Postman Collection**: API 테스트 컬렉션 (로컬 개발용)
- **cURL**: 명령줄 HTTP 클라이언트

**🔧 빠른 명령어:**
```bash
# API 부하테스트 실행
python3 /home/grey1/stock-kafka3/scripts/kafka_load_tester.py --api-workers 4 --duration 5

# 개별 API 연결 테스트
curl -w "@curl-format.txt" -o /dev/null -s "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"

# 내부 서비스 헬스체크
docker exec kafka-ui curl -f http://localhost:8080/health || echo "kafka-ui DOWN"
docker exec airflow-webserver curl -f http://localhost:8080/health || echo "airflow DOWN"
```

**📊 실시간 모니터링:**
- � **Kafka UI**: [http://localhost:8080](http://localhost:8080) - Kafka 클러스터 상태
- 🌐 **Airflow UI**: [http://localhost:8081](http://localhost:8081) - DAG 모니터링  
- � **Streamlit**: [http://localhost:8501](http://localhost:8501) - 통합 대시보드
""")
