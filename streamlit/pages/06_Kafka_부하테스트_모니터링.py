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
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⚙️ 테스트 매개변수")
        
        # 테스트 설정 폼
        with st.form("load_test_config"):
            test_duration = st.slider("테스트 지속시간 (분)", 1, 60, 10)
            kafka_threads = st.slider("Kafka 스레드 수", 1, 50, 10)
            messages_per_thread = st.slider("스레드당 메시지 수", 100, 5000, 1000)
            signal_probability = st.slider("신호 발생 확률 (%)", 0, 100, 30)
            
            test_topics = st.multiselect(
                "테스트 대상 토픽",
                ["realtime-stock", "yfinance-stock", "kis-stock", "signal-test"],
                default=["realtime-stock"]
            )
            
            submitted = st.form_submit_button("🚀 테스트 시작")
            
            if submitted:
                st.success(f"부하테스트 설정 완료!")
                st.info(f"""
                **설정 요약:**
                - 지속시간: {test_duration}분
                - Kafka 스레드: {kafka_threads}개
                - 메시지/스레드: {messages_per_thread}개
                - 신호 확률: {signal_probability}%
                - 대상 토픽: {', '.join(test_topics)}
                """)
    
    with col2:
        st.subheader("📊 예상 성능 계산")
        
        # 성능 예측 계산
        total_messages = kafka_threads * messages_per_thread
        messages_per_minute = total_messages / test_duration
        expected_rps = messages_per_minute / 60
        
        st.metric("📤 총 예상 메시지 수", f"{total_messages:,}")
        st.metric("⚡ 예상 평균 RPS", f"{expected_rps:.1f}")
        st.metric("🎯 예상 신호 발생 수", f"{int(total_messages * signal_probability / 100):,}")
        
        # 리소스 요구사항 추정
        st.subheader("💻 예상 리소스 요구사항")
        
        estimated_cpu = min(kafka_threads * 2, 80)  # 스레드당 2% CPU, 최대 80%
        estimated_memory = min(kafka_threads * 50, 2000)  # 스레드당 50MB, 최대 2GB
        
        st.progress(estimated_cpu / 100, text=f"예상 CPU 사용률: {estimated_cpu}%")
        st.progress(estimated_memory / 2000, text=f"예상 메모리 사용량: {estimated_memory}MB")
        
        if estimated_cpu > 70:
            st.warning("⚠️ 높은 CPU 사용률이 예상됩니다. 스레드 수를 줄이는 것을 고려하세요.")
        
        if estimated_memory > 1500:
            st.warning("⚠️ 높은 메모리 사용량이 예상됩니다. 메시지 크기를 확인하세요.")
    
    # 테스트 히스토리
    st.subheader("📚 최근 테스트 히스토리")
    
    test_history = {
        "실행 시간": [
            "2025-07-30 14:20:00",
            "2025-07-30 13:45:00", 
            "2025-07-30 13:15:00"
        ],
        "테스트 타입": ["Kafka 전용", "통합 테스트", "신호 감지"],
        "지속시간": ["10분", "15분", "5분"],
        "평균 RPS": [125.3, 98.7, 156.2],
        "성공률": ["98.5%", "99.2%", "97.8%"],
        "상태": ["✅ 완료", "✅ 완료", "✅ 완료"]
    }
    
    df_history = pd.DataFrame(test_history)
    st.dataframe(df_history, use_container_width=True)

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
# Kafka 부하테스트 실행
python3 run_load_test.py --test-type kafka --kafka-threads 10 --kafka-messages 1000

# 신호 감지 테스트
docker compose exec kafka-producer python /app/kafka/signal_test_producer.py --interval 5 --signal-prob 0.3

# 로그 모니터링  
docker compose logs kafka-consumer kafka-producer -f
```
""")
