import streamlit as st
import json
import time
import subprocess
from datetime import datetime, timedelta
import pytz
import sys
import os

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

def format_korean_time(timestamp_str):
    """타임스탬프를 한국시간으로 포맷팅"""
    try:
        if isinstance(timestamp_str, str):
            if 'T' in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(timestamp_str[:19], '%Y-%m-%d %H:%M:%S')
                dt = dt.replace(tzinfo=pytz.UTC)
            
            kst_time = dt.astimezone(KST)
            return kst_time.strftime('%Y-%m-%d %H:%M:%S KST')
        else:
            return str(timestamp_str)
    except:
        return str(timestamp_str)

def get_current_kst_time():
    """현재 한국시간 반환"""
    return datetime.now(KST)

# 선택적 import - 없으면 시뮬레이션 모드로 동작
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

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    st.warning("📦 psutil이 설치되지 않음 - 시스템 정보를 시뮬레이션합니다.")
    HAS_PSUTIL = False

# 공통 모듈 경로 추가
sys.path.insert(0, '/home/grey1/stock-kafka3/common')
sys.path.insert(0, '/opt/airflow/common')

try:
    from redis_client import RedisClient
    HAS_REDIS = True
except ImportError:
    st.warning("📦 Redis 클라이언트를 가져올 수 없습니다 - 시뮬레이션 모드로 동작합니다.")
    HAS_REDIS = False

st.set_page_config(
    page_title="Kafka 부하테스트 모니터링", 
    page_icon="🚀", 
    layout="wide"
)

st.title("🚀 Kafka 부하테스트 실시간 모니터링")

# 현재 한국 시간 표시
current_time = get_current_kst_time()
st.info(f"🕐 현재 시간: {current_time.strftime('%Y년 %m월 %d일 %H:%M:%S KST')}")

# 사이드바 설정
st.sidebar.header("⚙️ 부하테스트 제어")

# 부하테스트 실행 버튼들
test_type = st.sidebar.selectbox(
    "테스트 타입 선택",
    ["가벼운 테스트 (5명, 3분)", "중간 테스트 (20명, 10분)", "무거운 테스트 (50명, 20분)", "Kafka 전용 테스트", "커스텀 설정"]
)

if st.sidebar.button("🚀 부하테스트 시작"):
    if test_type == "가벼운 테스트 (5명, 3분)":
        st.sidebar.success("가벼운 테스트를 시작합니다...")
        # 실제로는 subprocess로 부하테스트 실행
    elif test_type == "Kafka 전용 테스트":
        st.sidebar.success("Kafka 전용 테스트를 시작합니다...")

if st.sidebar.button("⏹️ 테스트 중지"):
    st.sidebar.warning("테스트를 중지합니다...")

# 자동 새로고침 설정
auto_refresh = st.sidebar.checkbox("자동 새로고침 (5초)", value=True)
if auto_refresh:
    time.sleep(5)
    st.rerun()

# 메인 탭 구성
tab1, tab2, tab3, tab4 = st.tabs(["📊 실시간 모니터링", "📈 성능 메트릭", "🔍 상세 분석", "📋 테스트 설정"])

with tab1:
    st.header("📊 실시간 Kafka 부하테스트 현황")
    
    # 상단 메트릭 카드
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # 시뮬레이션 데이터 (실제로는 Redis나 Kafka에서 가져옴)
        current_rps = 125.3
        st.metric(
            label="⚡ 현재 RPS",
            value=f"{current_rps:.1f}",
            delta=f"+{15.2:.1f}"
        )
    
    with col2:
        active_consumers = 3
        st.metric(
            label="👥 활성 Consumer",
            value=active_consumers,
            delta=1
        )
    
    with col3:
        total_messages = 125430
        st.metric(
            label="📤 총 메시지",
            value=f"{total_messages:,}",
            delta=f"+{1250}"
        )
    
    with col4:
        error_rate = 0.67
        st.metric(
            label="❌ 오류율",
            value=f"{error_rate:.2f}%",
            delta=f"-{0.12:.2f}%"
        )
    
    # 실시간 차트
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 실시간 RPS (Requests Per Second)")
        
        # 시뮬레이션 데이터 생성 (한국시간 기준)
        current_kst = get_current_kst_time()
        timestamps = [current_kst - timedelta(minutes=x) for x in range(10, 0, -1)]
        rps_values = [100 + (x * 5) + (x % 3 * 10) for x in range(10)]
        
        if HAS_PLOTLY:
            fig_rps = go.Figure()
            fig_rps.add_trace(go.Scatter(
                x=timestamps,
                y=rps_values,
                mode='lines+markers',
                name='RPS',
                line=dict(color='#00C851', width=3),
                marker=dict(size=6)
            ))
            
            fig_rps.update_layout(
                title="실시간 처리량 추이",
                xaxis_title="시간",
                yaxis_title="RPS",
                template="plotly_white",
                height=300
            )
            
            st.plotly_chart(fig_rps, use_container_width=True)
        else:
            # plotly가 없을 때 기본 라인 차트
            chart_data = {
                '시간': [t.strftime('%H:%M') for t in timestamps],
                'RPS': rps_values
            }
            st.line_chart(chart_data, x='시간', y='RPS')
    
    with col2:
        st.subheader("⏱️ 응답시간 분포")
        
        # 응답시간 히스토그램 데이터
        response_times = [25, 45, 35, 55, 40, 60, 50, 30, 65, 45, 52, 38, 48, 42, 58]
        
        if HAS_PLOTLY:
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(
                x=response_times,
                nbinsx=8,
                name="응답시간",
                marker_color='#FF6384',
                opacity=0.7
            ))
            
            fig_hist.update_layout(
                title="응답시간 분포 (ms)",
                xaxis_title="응답시간 (ms)",
                yaxis_title="빈도",
                template="plotly_white",
                height=300
            )
            
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            # plotly가 없을 때 기본 히스토그램
            st.write("**응답시간 분포 (ms)**")
            bins = {}
            for rt in response_times:
                bin_key = f"{(rt//10)*10}-{(rt//10)*10+9}ms"
                bins[bin_key] = bins.get(bin_key, 0) + 1
            
            for bin_range, count in bins.items():
                st.write(f"• {bin_range}: {count}개")
            
            # 간단한 바 차트
            st.bar_chart(bins)
    
    # 토픽별 상세 현황
    st.subheader("📋 토픽별 처리 현황")
    
    # 시뮬레이션 데이터
    topic_data = {
        "Topic Name": ["realtime-stock", "yfinance-stock", "kis-stock", "signal-test"],
        "Messages": [125430, 89220, 45100, 23560],
        "Consumers": [3, 2, 1, 1],
        "Throughput (msg/s)": [1250, 890, 451, 235],
        "Lag": [45, 128, 12, 8],
        "Status": ["🟢 정상", "🟡 지연", "🟢 정상", "🟢 정상"]
    }
    
    if HAS_PANDAS:
        df_topics = pd.DataFrame(topic_data)
        st.dataframe(df_topics, use_container_width=True, height=200)
    else:
        # pandas가 없을 때 기본 테이블
        st.write("**토픽별 상세 현황**")
        for i, topic in enumerate(topic_data["Topic Name"]):
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            with col1:
                st.write(topic)
            with col2:
                st.write(f"{topic_data['Messages'][i]:,}")
            with col3:
                st.write(topic_data['Consumers'][i])
            with col4:
                st.write(topic_data['Throughput (msg/s)'][i])
            with col5:
                st.write(topic_data['Lag'][i])
            with col6:
                st.write(topic_data['Status'][i])

with tab2:
    st.header("📈 상세 성능 메트릭")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🖥️ 시스템 리소스")
        
        # 시스템 리소스 메트릭
        if HAS_PSUTIL:
            cpu_usage = psutil.cpu_percent()
            memory = psutil.virtual_memory()
        else:
            # psutil이 없을 때 시뮬레이션 데이터
            cpu_usage = 45.2
            class SimMemory:
                percent = 67.8
                used = 4 * 1024 * 1024 * 1024  # 4GB
            memory = SimMemory()
        
        if HAS_PLOTLY:
            # CPU 게이지
            fig_cpu = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = cpu_usage,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "CPU 사용률 (%)"},
                gauge = {
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "lightgray"},
                        {'range': [50, 80], 'color': "yellow"},
                        {'range': [80, 100], 'color': "red"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            
            fig_cpu.update_layout(height=250)
            st.plotly_chart(fig_cpu, use_container_width=True)
        else:
            # plotly가 없을 때 간단한 프로그레스 바
            st.write("**CPU 사용률**")
            st.progress(cpu_usage / 100, text=f"CPU: {cpu_usage:.1f}%")
        
        # 메모리 사용률
        memory_percent = memory.percent
        st.metric(
            label="💾 메모리 사용률",
            value=f"{memory_percent:.1f}%",
            delta=f"{memory.used / 1024 / 1024 / 1024:.1f}GB 사용 중"
        )
    
    with col2:
        st.subheader("📊 Kafka 클러스터 상태")
        
        # Consumer Group 지연시간
        consumer_groups = ["signal-detector", "data-processor", "analytics-worker"]
        lag_values = [45, 128, 67]
        
        fig_lag = go.Figure()
        fig_lag.add_trace(go.Bar(
            x=consumer_groups,
            y=lag_values,
            marker_color=['green' if x < 100 else 'orange' for x in lag_values],
            text=lag_values,
            textposition='auto'
        ))
        
        fig_lag.update_layout(
            title="Consumer Group Lag",
            xaxis_title="Consumer Group",
            yaxis_title="Lag (messages)",
            template="plotly_white",
            height=300
        )
        
        st.plotly_chart(fig_lag, use_container_width=True)
    
    # 성능 추이 차트
    st.subheader("⏱️ 성능 지표 시간별 추이")
    
    # 시뮬레이션 시계열 데이터 (한국시간 기준)
    current_kst = get_current_kst_time()
    time_range = pd.date_range(start=current_kst - timedelta(hours=1), end=current_kst, freq='5min')
    performance_data = {
        'timestamp': time_range,
        'rps': [100 + (i % 5) * 20 + (i % 3) * 10 for i in range(len(time_range))],
        'latency_p95': [50 + (i % 4) * 15 + (i % 2) * 5 for i in range(len(time_range))],
        'error_rate': [0.5 + (i % 6) * 0.2 for i in range(len(time_range))]
    }
    
    df_perf = pd.DataFrame(performance_data)
    
    # 멀티 라인 차트
    fig_multi = go.Figure()
    
    fig_multi.add_trace(go.Scatter(
        x=df_perf['timestamp'],
        y=df_perf['rps'],
        mode='lines+markers',
        name='RPS',
        yaxis='y',
        line=dict(color='blue')
    ))
    
    fig_multi.add_trace(go.Scatter(
        x=df_perf['timestamp'],
        y=df_perf['latency_p95'],
        mode='lines+markers',
        name='지연시간 P95 (ms)',
        yaxis='y2',
        line=dict(color='red')
    ))
    
    fig_multi.add_trace(go.Scatter(
        x=df_perf['timestamp'],
        y=df_perf['error_rate'],
        mode='lines+markers',
        name='오류율 (%)',
        yaxis='y3',
        line=dict(color='orange')
    ))
    
    fig_multi.update_layout(
        title="성능 지표 추이 (최근 1시간)",
        xaxis_title="시간",
        yaxis=dict(title="RPS", side="left"),
        yaxis2=dict(title="지연시간 (ms)", side="right", overlaying="y"),
        yaxis3=dict(title="오류율 (%)", side="right", overlaying="y", position=0.85),
        template="plotly_white",
        height=400
    )
    
    st.plotly_chart(fig_multi, use_container_width=True)

with tab3:
    st.header("🔍 상세 분석 및 진단")
    
    # 실시간 로그 스트림
    st.subheader("📋 실시간 로그 스트림")
    
    log_container = st.container()
    with log_container:
        # 시뮬레이션 로그 데이터
        log_messages = [
            "2025-07-30 14:30:15 INFO: Kafka message sent successfully - Topic: realtime-stock, Partition: 2",
            "2025-07-30 14:30:16 INFO: Consumer processed 125 messages - Group: signal-detector",
            "2025-07-30 14:30:17 WARN: High consumer lag detected - Group: data-processor, Lag: 128",
            "2025-07-30 14:30:18 INFO: Signal detected for AAPL - Type: bollinger_upper_touch",
            "2025-07-30 14:30:19 ERROR: Redis connection timeout - Retrying...",
            "2025-07-30 14:30:20 INFO: Performance metrics updated - RPS: 125.3, Latency: 45ms"
        ]
        
        for log in log_messages[-10:]:  # 최근 10개 로그만 표시
            if "ERROR" in log:
                st.error(log)
            elif "WARN" in log:
                st.warning(log)
            else:
                st.info(log)
    
    # 오류 분석
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("❌ 오류 유형 분석")
        
        error_types = ["Connection Timeout", "Message Parse Error", "Redis Error", "Network Error"]
        error_counts = [5, 2, 8, 3]
        
        fig_errors = px.pie(
            values=error_counts,
            names=error_types,
            title="오류 유형별 분포"
        )
        
        fig_errors.update_layout(height=300)
        st.plotly_chart(fig_errors, use_container_width=True)
    
    with col2:
        st.subheader("📊 처리량 vs 지연시간 상관관계")
        
        # 스캐터 플롯 데이터
        rps_data = [80, 100, 120, 140, 160, 180, 200, 220]
        latency_data = [20, 25, 35, 50, 75, 100, 140, 200]
        
        fig_scatter = go.Figure()
        fig_scatter.add_trace(go.Scatter(
            x=rps_data,
            y=latency_data,
            mode='markers+lines',
            marker=dict(size=10, color='purple'),
            name='RPS vs Latency'
        ))
        
        fig_scatter.update_layout(
            title="처리량-지연시간 상관관계",
            xaxis_title="RPS",
            yaxis_title="지연시간 (ms)",
            template="plotly_white",
            height=300
        )
        
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # 병목 지점 분석
    st.subheader("🔍 병목 지점 분석")
    
    bottleneck_data = {
        "구성요소": ["Kafka Producer", "Kafka Consumer", "Redis Client", "Database", "Network"],
        "사용률 (%)": [45, 67, 23, 34, 12],
        "상태": ["🟢 정상", "🟡 주의", "🟢 정상", "🟢 정상", "🟢 정상"],
        "권장사항": [
            "현재 상태 유지",
            "Consumer 수 증가 고려",
            "현재 상태 유지", 
            "현재 상태 유지",
            "현재 상태 유지"
        ]
    }
    
    df_bottleneck = pd.DataFrame(bottleneck_data)
    st.dataframe(df_bottleneck, use_container_width=True)

with tab4:
    st.header("📋 부하테스트 설정 및 제어")
    
    # 워커 개념 명확화
    st.info("""
    **🔧 부하테스트에서 사용하는 워커 타입 정의:**
    
    **📤 Kafka Producer 워커** 
    - **역할**: 주식 데이터 메시지를 생성하여 Kafka 토픽으로 전송
    - **대상 토픽**: `realtime-stock`, `yfinance-stock-data`
    - **부하 강도 조절**: 워커 수량으로 동시 전송 성능 제어
    
    **📥 Kafka Consumer 워커**
    - **역할**: Kafka 토픽에서 메시지를 소비하고 신호 감지 처리
    - **처리 방식**: 실시간 스트림 처리 및 기술적 지표 계산
    - **부하 강도 조절**: 워커 수량으로 동시 처리 성능 제어
    
    **🌐 API 호출 워커**
    - **역할**: 외부 API (yfinance, KIS API 등) 동시 호출 테스트
    - **연관**: BulkDataCollector의 `max_workers=4` 설정과 동일 개념
    - **부하 강도 조절**: 워커 수량으로 동시 API 요청 수 제어
    
    ⚠️ **주의**: Docker 컨테이너 워커(`airflow-worker`, `spark-worker`)와는 다른 개념입니다.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⚙️ 테스트 매개변수")
        
        # 테스트 설정 폼
        with st.form("load_test_config"):
            st.markdown("**⏱️ 테스트 기본 설정**")
            test_duration = st.slider("테스트 지속시간 (분)", 1, 60, 10)
            signal_probability = st.slider("신호 발생 확률 (%)", 0, 100, 30)
            
            st.markdown("**📤 Kafka Producer 워커 설정**")
            col_prod1, col_prod2 = st.columns(2)
            with col_prod1:
                kafka_producers = st.slider("Producer 워커 수", 1, 20, 10)
            with col_prod2:
                messages_per_producer = st.slider("Producer당 메시지 수", 100, 5000, 1000)
            
            st.markdown("**📥 Kafka Consumer 워커 설정**")
            col_cons1, col_cons2 = st.columns(2)
            with col_cons1:
                kafka_consumers = st.slider("Consumer 워커 수", 1, 15, 5)
            with col_cons2:
                consumer_timeout = st.slider("Consumer 타임아웃 (초)", 30, 300, 60)
            
            st.markdown("**🌐 API 호출 워커 설정**")
            col_api1, col_api2 = st.columns(2)
            with col_api1:
                api_workers = st.slider("API 워커 수 (BulkDataCollector max_workers)", 1, 10, 4)
            with col_api2:
                calls_per_api_worker = st.slider("워커당 API 호출 수", 50, 500, 100)
            
            st.markdown("**📊 토픽 및 엔드포인트 설정**")
            test_topics = st.multiselect(
                "테스트 대상 토픽",
                ["realtime-stock", "yfinance-stock-data", "kis-stock", "signal-test"],
                default=["realtime-stock", "yfinance-stock-data"]
            )
            
            api_endpoints = st.multiselect(
                "API 테스트 엔드포인트",
                [
                    "http://localhost:8080/api/kafka/health",
                    "http://localhost:8081/api/v1/health",
                    "http://localhost:6379/ping",
                    "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"
                ],
                default=["http://localhost:8080/api/kafka/health"]
            )
            
            submitted = st.form_submit_button("🚀 부하테스트 시작", use_container_width=True)
            
            if submitted:
                st.success(f"🎯 부하테스트 설정 완료!")
                
                # 실제 테스트 실행 명령어 생성 (kafka_load_tester.py 사용)
                test_command = f"""
# Kafka + API 통합 부하테스트 실행
cd /home/grey1/stock-kafka3
python3 scripts/kafka_load_tester.py \\
    --duration {test_duration} \\
    --kafka-producers {kafka_producers} \\
    --kafka-consumers {kafka_consumers} \\
    --api-workers {api_workers} \\
    --messages-per-worker {messages_per_producer} \\
    --topics {' '.join(test_topics)}

# 또는 Docker 환경에서 실행
docker exec kafka-producer python3 /app/scripts/kafka_load_tester.py \\
    --duration {test_duration} --kafka-producers {kafka_producers}
                """.strip()
                
                st.code(test_command, language="bash")
                
                with st.expander("📋 설정 요약", expanded=True):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("⏱️ 지속시간", f"{test_duration}분")
                        st.metric("📤 Producer 워커", f"{kafka_producers}개")
                        st.metric("📥 Consumer 워커", f"{kafka_consumers}개")
                    with col2:
                        st.metric("🌐 API 워커", f"{api_workers}개")
                        st.metric("📨 총 메시지", f"{kafka_producers * messages_per_producer:,}")
                        st.metric("📞 총 API 호출", f"{api_workers * calls_per_api_worker:,}")
                    with col3:
                        st.metric("🎯 신호 확률", f"{signal_probability}%")
                        st.metric("📊 토픽 수", f"{len(test_topics)}개")
                        st.metric("🔗 API 엔드포인트", f"{len(api_endpoints)}개")
    
    with col2:
        st.subheader("📊 예상 성능 계산")
        
        # 성능 예측 계산 (워커별)
        total_producer_messages = kafka_producers * messages_per_producer
        total_api_calls = api_workers * calls_per_api_worker
        producer_rps = total_producer_messages / (test_duration * 60) if test_duration > 0 else 0
        api_rps = total_api_calls / (test_duration * 60) if test_duration > 0 else 0
        
        col_metric1, col_metric2 = st.columns(2)
        with col_metric1:
            st.metric("📤 예상 Producer RPS", f"{producer_rps:.1f}")
            st.metric("🎯 예상 신호 수", f"{int(total_producer_messages * signal_probability / 100):,}")
            st.metric("🔄 토픽당 메시지", f"{total_producer_messages // len(test_topics) if test_topics else 0:,}")
        
        with col_metric2:
            st.metric("🌐 예상 API RPS", f"{api_rps:.1f}")
            st.metric("📊 처리 예상 지연", f"{kafka_consumers * 10:.0f}ms")
            st.metric("💾 예상 캐시 적중률", "85%")
        
        # 리소스 요구사항 추정 (워커별로 더 정확하게)
        st.subheader("💻 예상 리소스 요구사항")
        
        # CPU: Producer(2%), Consumer(3%), API(1%) per worker
        estimated_cpu = min((kafka_producers * 2) + (kafka_consumers * 3) + (api_workers * 1), 80)
        # Memory: Producer(50MB), Consumer(80MB), API(30MB) per worker  
        estimated_memory = (kafka_producers * 50) + (kafka_consumers * 80) + (api_workers * 30)
        
        col_resource1, col_resource2 = st.columns(2)
        with col_resource1:
            cpu_color = "🟢" if estimated_cpu < 50 else "🟡" if estimated_cpu < 70 else "🔴"
            st.metric("🔥 예상 CPU 사용률", f"{cpu_color} {estimated_cpu}%")
            
            network_usage = (total_producer_messages * 0.5) + (total_api_calls * 2)  # KB 단위
            st.metric("🌐 예상 네트워크 사용", f"{network_usage:.1f} KB")
        
        with col_resource2:
            memory_color = "🟢" if estimated_memory < 1000 else "🟡" if estimated_memory < 1500 else "🔴"
            st.metric("💾 예상 메모리 사용", f"{memory_color} {estimated_memory} MB")
            
            disk_io = total_producer_messages * 0.1  # 로그, 메트릭 저장
            st.metric("💽 예상 디스크 I/O", f"{disk_io:.1f} KB")
        
        # 성능 차트
        st.subheader("📈 예상 부하 분포")
        
        if HAS_PLOTLY:
            workload_data = {
                '워커 타입': ['Kafka Producer', 'Kafka Consumer', 'API Worker'],
                '워커 수': [kafka_producers, kafka_consumers, api_workers],
                '예상 처리량': [producer_rps, producer_rps * 0.9, api_rps],  # Consumer는 Producer의 90% 처리
                'CPU 사용률': [kafka_producers * 2, kafka_consumers * 3, api_workers * 1]
            }
            
            df_workload = pd.DataFrame(workload_data)
            
            fig_workload = go.Figure()
            fig_workload.add_trace(go.Bar(
                name='워커 수',
                x=df_workload['워커 타입'],
                y=df_workload['워커 수'],
                yaxis='y',
                marker_color='lightblue'
            ))
            
            fig_workload.add_trace(go.Scatter(
                name='예상 처리량 (msg/sec)',
                x=df_workload['워커 타입'],
                y=df_workload['예상 처리량'],
                yaxis='y2',
                mode='lines+markers',
                line=dict(color='red'),
                marker=dict(size=10)
            ))
            
            fig_workload.update_layout(
                title="워커별 부하 분포 예측",
                xaxis_title="워커 타입",
                yaxis=dict(title="워커 수", side="left"),
                yaxis2=dict(title="처리량 (msg/sec)", side="right", overlaying="y"),
                template="plotly_white",
                height=300
            )
            
            st.plotly_chart(fig_workload, use_container_width=True)
        
        # 경고 및 권장사항
        warnings = []
        if estimated_cpu > 70:
            warnings.append("⚠️ 높은 CPU 사용률이 예상됩니다. Producer 워커 수를 줄이세요.")
        if estimated_memory > 1500:
            warnings.append("⚠️ 높은 메모리 사용량이 예상됩니다. Consumer 워커 수를 조절하세요.")
        if producer_rps > 200:
            warnings.append("⚠️ 높은 메시지 처리율이 예상됩니다. Kafka 클러스터 상태를 확인하세요.")
        if len(test_topics) > 2 and kafka_consumers < len(test_topics):
            warnings.append("💡 토픽 수보다 Consumer 워커가 적습니다. 병렬 처리 효율성을 고려하세요.")
            
        for warning in warnings:
            st.warning(warning)
        
        if not warnings:
            st.success("✅ 리소스 사용량이 적절합니다. 테스트를 시작하세요!")
    
    # 실시간 테스트 실행 및 모니터링
    st.subheader("🔴 실시간 테스트 실행")
    
    col_control1, col_control2, col_control3, col_control4 = st.columns(4)
    
    with col_control1:
        if st.button("🚀 빠른 테스트 (1분)", use_container_width=True):
            st.info("1분 빠른 테스트가 시작되었습니다...")
            # 실제 구현에서는 subprocess로 테스트 실행
    
    with col_control2:
        if st.button("📊 API 전용 테스트", use_container_width=True):
            st.info("API 호출 전용 테스트가 시작되었습니다...")
    
    with col_control3:
        if st.button("🔍 신호 감지 테스트", use_container_width=True):
            st.info("신호 감지 집중 테스트가 시작되었습니다...")
    
    with col_control4:
        if st.button("🛑 테스트 중단", use_container_width=True, type="secondary"):
            st.warning("진행 중인 모든 테스트가 중단됩니다...")
    
    # 테스트 히스토리 (워커별 상세 정보 포함)
    st.subheader("📚 최근 테스트 히스토리")
    
    test_history = {
        "실행 시간": [
            "2025-08-07 14:20:00",
            "2025-08-07 13:45:00", 
            "2025-08-07 13:15:00",
            "2025-08-07 12:30:00"
        ],
        "테스트 타입": ["통합 부하테스트", "API 전용", "신호 감지", "Producer 스트레스"],
        "지속시간": ["10분", "15분", "5분", "20분"],
        "Producer 워커": [10, 0, 5, 20],
        "Consumer 워커": [5, 0, 8, 10],
        "API 워커": [4, 6, 2, 0],
        "평균 RPS": [125.3, 45.8, 78.2, 256.7],
        "성공률": ["98.5%", "99.2%", "97.8%", "96.1%"],
        "CPU 평균": ["45%", "25%", "35%", "72%"],
        "상태": ["✅ 완료", "✅ 완료", "✅ 완료", "⚠️ 높은 부하"]
    }
    
    df_history = pd.DataFrame(test_history)
    st.dataframe(df_history, use_container_width=True)
    
    # 테스트 결과 상세 분석
    with st.expander("📊 최근 테스트 결과 상세 분석", expanded=False):
        col_detail1, col_detail2 = st.columns(2)
        
        with col_detail1:
            st.subheader("워커별 성능 비교")
            if HAS_PLOTLY:
                worker_performance = {
                    '워커 타입': ['Producer', 'Consumer', 'API'] * 4,
                    '테스트': ['통합'] * 3 + ['API전용'] * 3 + ['신호감지'] * 3 + ['스트레스'] * 3,
                    '처리량': [125, 110, 45, 0, 0, 46, 78, 75, 25, 257, 240, 0],
                    '성공률': [98.5, 98.2, 99.2, 0, 0, 99.2, 97.8, 98.1, 97.0, 96.1, 95.8, 0]
                }
                
                df_performance = pd.DataFrame(worker_performance)
                df_performance = df_performance[df_performance['처리량'] > 0]  # 0인 값 제외
                
                fig_performance = px.bar(
                    df_performance, 
                    x='테스트', 
                    y='처리량', 
                    color='워커 타입',
                    title="테스트별 워커 성능",
                    barmode='group'
                )
                fig_performance.update_layout(height=300)
                st.plotly_chart(fig_performance, use_container_width=True)
        
        with col_detail2:
            st.subheader("리소스 사용률 추이")
            if HAS_PLOTLY:
                resource_trend = {
                    '시간': ['12:30', '13:15', '13:45', '14:20'],
                    'CPU (%)': [72, 35, 25, 45],
                    '메모리 (%)': [68, 42, 38, 52],
                    '네트워크 (MB/s)': [15.2, 8.5, 4.2, 12.8]
                }
                
                df_resource = pd.DataFrame(resource_trend)
                
                fig_resource = go.Figure()
                fig_resource.add_trace(go.Scatter(x=df_resource['시간'], y=df_resource['CPU (%)'], mode='lines+markers', name='CPU'))
                fig_resource.add_trace(go.Scatter(x=df_resource['시간'], y=df_resource['메모리 (%)'], mode='lines+markers', name='메모리'))
                fig_resource.update_layout(title="시간대별 리소스 사용률", height=300)
                st.plotly_chart(fig_resource, use_container_width=True)

# 페이지 하단 정보
st.markdown("---")
st.markdown("""
### 📌 모니터링 도구 링크
- 🌐 **Kafka UI**: [http://localhost:8080](http://localhost:8080) - Kafka 클러스터 상태
- 🕷️ **Locust UI**: [http://localhost:8089](http://localhost:8089) - API 부하테스트  
- 📊 **Streamlit**: [http://localhost:8501](http://localhost:8501) - 통합 모니터링
- 🐳 **Docker Stats**: `docker stats` - 컨테이너 리소스 상태

### 🔧 빠른 명령어
```bash
# 새로운 Kafka + API 통합 부하테스트 실행 (kafka_load_tester.py 사용)
python3 /home/grey1/stock-kafka3/scripts/kafka_load_tester.py --kafka-producers 10 --kafka-consumers 5 --api-workers 4 --duration 5

# Producer 전용 부하테스트
python3 /home/grey1/stock-kafka3/scripts/kafka_load_tester.py --kafka-producers 15 --kafka-consumers 0 --api-workers 0 --duration 3

# Consumer + API 테스트
python3 /home/grey1/stock-kafka3/scripts/kafka_load_tester.py --kafka-producers 0 --kafka-consumers 8 --api-workers 6 --duration 10

# 로그 모니터링  
docker compose logs kafka-producer kafka-consumer -f
```
""")
