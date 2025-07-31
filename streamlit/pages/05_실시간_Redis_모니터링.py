#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import time
from datetime import datetime
import pytz
import sys

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

def format_korean_time(timestamp_str):
    """타임스탬프를 한국시간으로 포맷팅"""
    try:
        if isinstance(timestamp_str, str):
            # ISO 형식의 타임스탬프를 파싱
            if 'T' in timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(timestamp_str[:19], '%Y-%m-%d %H:%M:%S')
                dt = dt.replace(tzinfo=pytz.UTC)
            
            # 한국 시간으로 변환
            kst_time = dt.astimezone(KST)
            return kst_time.strftime('%Y-%m-%d %H:%M:%S KST')
        else:
            return str(timestamp_str)
    except:
        return str(timestamp_str)

def get_current_kst_time():
    """현재 한국시간 반환"""
    return datetime.now(KST)

# 프로젝트 경로 추가
sys.path.append('/app/common')
from redis_client import RedisClient
from redis_manager import RedisManager

# 페이지 설정
st.set_page_config(
    page_title="📡 실시간 Redis 모니터링",
    page_icon="📡",
    layout="wide"
)

st.title("📡 실시간 Redis 데이터 모니터링")

# 현재 한국 시간 표시
current_time = get_current_kst_time()
st.info(f"🕐 현재 시간: {current_time.strftime('%Y년 %m월 %d일 %H:%M:%S KST')}")

try:
    # Redis 클라이언트 초기화
    redis_client = RedisClient()
    redis_manager = RedisManager()
    
    # 사이드바 설정
    st.sidebar.title("⚙️ 설정")
    
    auto_refresh = st.sidebar.checkbox("자동 새로고침", value=False)
    refresh_interval = st.sidebar.slider("새로고침 간격 (초)", 1, 10, 5)
    
    if st.sidebar.button("🔄 수동 새로고침"):
        st.rerun()
    
    # 메인 컨텐츠
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["� 신호 추적", "�📊 실시간 데이터", "🔧 Redis 상태", "📈 캐시 통계", "🔑 키 관리"])
    
    with tab1:
        st.subheader("🚨 기술적 신호 추적 대시보드")
        
        # 활성 신호 조회
        try:
            active_signals = redis_client.get_active_signals()
            
            if active_signals:
                st.success(f"🎯 현재 활성 신호: {len(active_signals)}개")
                
                # 신호 성과 계산 및 표시
                signal_performance = []
                
                for signal in active_signals:
                    symbol = signal['symbol']
                    
                    # 현재 가격 조회 (실시간 분석 데이터에서)
                    current_analysis = redis_client.get_realtime_analysis(symbol)
                    current_price = None
                    
                    if current_analysis:
                        current_price = current_analysis['current_price']
                        
                        # 성과 업데이트
                        updated_signal = redis_client.update_signal_performance(
                            symbol=symbol,
                            trigger_time=signal['trigger_time'],
                            current_price=current_price
                        )
                        
                        if updated_signal:
                            signal = updated_signal
                    
                    # 신호 타입 한글 변환
                    signal_type_names = {
                        'bollinger_upper_touch': '🔴 볼린저 상단 터치',
                        'rsi_overbought': '📈 RSI 과매수',
                        'rsi_oversold': '📉 RSI 과매도',
                        'macd_bullish': '🟢 MACD 상승'
                    }
                    
                    # 성과 색상 결정
                    change_pct = signal.get('price_change_pct', 0)
                    if change_pct > 2:
                        performance_icon = "🟢"
                        performance_color = "green"
                    elif change_pct > 0:
                        performance_icon = "🔵"
                        performance_color = "blue"
                    elif change_pct < -2:
                        performance_icon = "🔴"
                        performance_color = "red"
                    else:
                        performance_icon = "🟡"
                        performance_color = "orange"
                    
                    signal_performance.append({
                        'Symbol': signal['symbol'],
                        'Signal': signal_type_names.get(signal['signal_type'], signal['signal_type']),
                        'Trigger Price': f"${signal['trigger_price']:.2f}",
                        'Current Price': f"${signal.get('current_price', 0):.2f}" if signal.get('current_price') else "N/A",
                        'Change': f"{change_pct:+.2f}%" if change_pct != 0 else "0.00%",
                        'Performance': performance_icon,
                        'Trigger Time': format_korean_time(signal['trigger_time']),
                        '_change_pct': change_pct,
                        '_color': performance_color
                    })
                
                if signal_performance:
                    # 성과별 정렬
                    signal_performance.sort(key=lambda x: x['_change_pct'], reverse=True)
                    
                    # 성과 요약
                    col1, col2, col3, col4 = st.columns(4)
                    
                    positive_signals = [s for s in signal_performance if s['_change_pct'] > 0]
                    negative_signals = [s for s in signal_performance if s['_change_pct'] < 0]
                    avg_performance = sum(s['_change_pct'] for s in signal_performance) / len(signal_performance)
                    
                    with col1:
                        st.metric("수익 신호", len(positive_signals), delta=None)
                    
                    with col2:
                        st.metric("손실 신호", len(negative_signals), delta=None)
                    
                    with col3:
                        st.metric("평균 성과", f"{avg_performance:.2f}%", delta=None)
                    
                    with col4:
                        best_performance = max(s['_change_pct'] for s in signal_performance)
                        st.metric("최고 성과", f"{best_performance:+.2f}%", delta=None)
                    
                    # 신호 목록 테이블
                    st.markdown("### 📋 활성 신호 목록")
                    
                    # 성과에 따른 색상 코딩을 위한 스타일링
                    df = pd.DataFrame(signal_performance)
                    display_df = df[['Symbol', 'Signal', 'Trigger Price', 'Current Price', 'Change', 'Performance', 'Trigger Time']]
                    
                    # 조건부 포맷팅
                    def highlight_performance(row):
                        if row['_change_pct'] > 2:
                            return ['background-color: #d4edda'] * len(row)
                        elif row['_change_pct'] > 0:
                            return ['background-color: #cce5ff'] * len(row)
                        elif row['_change_pct'] < -2:
                            return ['background-color: #f8d7da'] * len(row)
                        else:
                            return ['background-color: #fff3cd'] * len(row)
                    
                    styled_df = display_df.style.apply(
                        lambda row: highlight_performance(df.iloc[row.name]), 
                        axis=1
                    )
                    
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)
                    
                    # 성과 차트
                    st.markdown("### 📊 신호별 성과 차트")
                    
                    fig = px.bar(
                        df,
                        x='Symbol',
                        y='_change_pct',
                        color='_change_pct',
                        color_continuous_scale=['red', 'yellow', 'green'],
                        title="신호별 성과 (%)",
                        labels={'_change_pct': '성과 (%)', 'Symbol': '종목'}
                    )
                    
                    fig.add_hline(y=0, line_dash="dash", line_color="black")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # 신호 타입별 통계
                    st.markdown("### 📈 신호 타입별 성과")
                    
                    signal_type_stats = {}
                    for perf in signal_performance:
                        signal_type = perf['Signal']
                        if signal_type not in signal_type_stats:
                            signal_type_stats[signal_type] = []
                        signal_type_stats[signal_type].append(perf['_change_pct'])
                    
                    type_summary = []
                    for signal_type, performances in signal_type_stats.items():
                        type_summary.append({
                            'Signal Type': signal_type,
                            'Count': len(performances),
                            'Avg Performance': f"{sum(performances)/len(performances):.2f}%",
                            'Best': f"{max(performances):+.2f}%",
                            'Worst': f"{min(performances):+.2f}%"
                        })
                    
                    if type_summary:
                        st.dataframe(pd.DataFrame(type_summary), use_container_width=True, hide_index=True)
                
            else:
                st.info("📭 현재 활성 신호가 없습니다.")
                
                # 최근 완료된 신호들 표시 (선택적)
                st.markdown("### 🔍 Redis 신호 키 검색")
                if st.button("최근 신호 데이터 확인"):
                    signal_keys = redis_client.redis_client.keys("signal_trigger:*")
                    if signal_keys:
                        st.info(f"총 {len(signal_keys)}개의 신호 기록이 있습니다.")
                        
                        # 최근 5개 신호 표시
                        recent_signals = []
                        for key in signal_keys[:5]:
                            signal_data = redis_client.redis_client.get(key)
                            if signal_data:
                                signal = json.loads(signal_data)
                                recent_signals.append({
                                    'Symbol': signal.get('symbol', 'N/A'),
                                    'Type': signal.get('signal_type', 'N/A'),
                                    'Trigger Time': format_korean_time(signal.get('trigger_time', 'N/A')),
                                    'Status': signal.get('status', 'N/A')
                                })
                        
                        if recent_signals:
                            st.dataframe(pd.DataFrame(recent_signals), use_container_width=True, hide_index=True)
                    else:
                        st.warning("신호 데이터가 없습니다.")
        
        except Exception as e:
            st.error(f"❌ 신호 추적 데이터 로딩 오류: {e}")
        
        # 관심종목 상태 확인
        st.markdown("### 🎯 관심종목 데이터 상태")
        
        try:
            watchlist_keys = redis_client.redis_client.keys("watchlist_data:*")
            realtime_keys = redis_client.redis_client.keys("realtime_analysis:*")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Redis 관심종목 데이터", len(watchlist_keys))
            
            with col2:
                st.metric("실시간 분석 데이터", len(realtime_keys))
                
            if watchlist_keys:
                st.success("✅ 관심종목 데이터가 Redis에 로딩되어 있습니다.")
            else:
                st.warning("⚠️ 관심종목 데이터를 Redis에 로딩해야 합니다.")
                st.code("python /app/scripts/load_watchlist_to_redis.py")
        
        except Exception as e:
            st.error(f"❌ 관심종목 상태 확인 오류: {e}")
    
    with tab2:
        st.subheader("📊 실시간 주식 데이터")
        
        # 실시간 가격 데이터 조회
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 💰 실시간 가격")
            
            # Redis에서 실시간 가격 데이터 조회
            try:
                # 모든 실시간 가격 키 조회
                price_keys = redis_client.redis_client.keys("realtime_price:*")
                
                if price_keys:
                    prices_data = []
                    for key in price_keys[:10]:  # 최대 10개만 표시
                        symbol = key.replace("realtime_price:", "")
                        price_data = redis_client.get_realtime_price(symbol)
                        
                        if price_data:
                            prices_data.append({
                                'Symbol': symbol,
                                'Price': price_data.get('price', 'N/A'),
                                'Change': price_data.get('change', 'N/A'),
                                'Volume': price_data.get('volume', 'N/A'),
                                'Timestamp': format_korean_time(price_data.get('timestamp', 'N/A'))
                            })
                    
                    if prices_data:
                        df = pd.DataFrame(prices_data)
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info("실시간 가격 데이터가 없습니다.")
                else:
                    st.info("Redis에 저장된 실시간 가격 데이터가 없습니다.")
                    
            except Exception as e:
                st.error(f"실시간 가격 데이터 조회 오류: {e}")
        
        with col2:
            st.markdown("### 📊 기술적 지표")
            
            # Redis에서 기술적 지표 데이터 조회
            try:
                indicators_keys = redis_client.redis_client.keys("technical_indicator:*")
                
                if indicators_keys:
                    indicators_data = []
                    for key in indicators_keys[:10]:
                        symbol = key.replace("technical_indicator:", "")
                        indicator_data = redis_client.get_technical_indicators(symbol)
                        
                        if indicator_data:
                            indicators_data.append({
                                'Symbol': symbol,
                                'RSI': indicator_data.get('rsi', 'N/A'),
                                'MACD': indicator_data.get('macd', 'N/A'),
                                'BB_Position': indicator_data.get('bb_position', 'N/A'),
                                'Signal': indicator_data.get('signal', 'N/A')
                            })
                    
                    if indicators_data:
                        df = pd.DataFrame(indicators_data)
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.info("기술적 지표 데이터가 없습니다.")
                else:
                    st.info("Redis에 저장된 기술적 지표 데이터가 없습니다.")
                    
            except Exception as e:
                st.error(f"기술적 지표 데이터 조회 오류: {e}")
    
    with tab2:
        st.subheader("🔧 Redis 서버 상태")
        
        # Redis 정보 조회
        try:
            info = redis_client.redis_client.info()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("연결된 클라이언트", info.get('connected_clients', 'N/A'))
                st.metric("사용된 메모리", f"{info.get('used_memory_human', 'N/A')}")
            
            with col2:
                st.metric("총 키 수", info.get('db0', {}).get('keys', 0) if 'db0' in info else 0)
                st.metric("만료된 키", info.get('expired_keys', 'N/A'))
            
            with col3:
                st.metric("총 명령어 수", info.get('total_commands_processed', 'N/A'))
                st.metric("초당 명령어", info.get('instantaneous_ops_per_sec', 'N/A'))
            
            # 상세 정보
            st.markdown("### 📋 상세 서버 정보")
            
            server_info = {
                'Redis Version': info.get('redis_version', 'N/A'),
                'Uptime (Days)': info.get('uptime_in_days', 'N/A'),
                'Role': info.get('role', 'N/A'),
                'Max Memory': info.get('maxmemory_human', 'N/A'),
                'Memory Policy': info.get('maxmemory_policy', 'N/A')
            }
            
            for key, value in server_info.items():
                st.text(f"{key}: {value}")
                
        except Exception as e:
            st.error(f"Redis 상태 조회 오류: {e}")
    
    with tab3:
        st.subheader("📈 캐시 성능 통계")
        
        try:
            # 캐시 성능 메트릭
            stats = redis_manager.get_cache_stats()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("캐시 히트율", f"{stats.get('hit_rate', 0):.1%}")
                st.metric("총 요청", stats.get('total_requests', 0))
            
            with col2:
                st.metric("캐시 미스", stats.get('misses', 0))
                st.metric("평균 응답시간", f"{stats.get('avg_response_time', 0):.3f}ms")
            
            # 캐시 타입별 통계
            st.markdown("### 📊 캐시 타입별 사용량")
            
            cache_types = ['realtime_price', 'technical_indicator', 'market_data', 'user_session']
            type_stats = []
            
            for cache_type in cache_types:
                keys = redis_client.redis_client.keys(f"{cache_type}:*")
                type_stats.append({
                    'Cache Type': cache_type,
                    'Key Count': len(keys),
                    'Memory Usage': 'N/A'  # Redis에서 메모리 사용량 계산 복잡
                })
            
            df = pd.DataFrame(type_stats)
            st.dataframe(df, use_container_width=True)
            
        except Exception as e:
            st.error(f"캐시 통계 조회 오류: {e}")
    
    with tab4:
        st.subheader("🔑 Redis 키 관리")
        
        # 키 검색
        search_pattern = st.text_input("키 패턴 검색", value="*", help="와일드카드(*) 사용 가능")
        
        if st.button("🔍 검색"):
            try:
                keys = redis_client.redis_client.keys(search_pattern)
                
                if keys:
                    st.success(f"총 {len(keys)}개의 키를 찾았습니다.")
                    
                    # 키 목록 표시 (최대 100개)
                    display_keys = keys[:100]
                    
                    key_data = []
                    for key in display_keys:
                        try:
                            ttl = redis_client.redis_client.ttl(key)
                            key_type = redis_client.redis_client.type(key)
                            
                            key_data.append({
                                'Key': key,
                                'Type': key_type,
                                'TTL': ttl if ttl > 0 else 'No expiry'
                            })
                        except:
                            continue
                    
                    if key_data:
                        df = pd.DataFrame(key_data)
                        st.dataframe(df, use_container_width=True)
                    
                    if len(keys) > 100:
                        st.warning(f"처음 100개만 표시됩니다. 총 {len(keys)}개 키가 있습니다.")
                else:
                    st.info("일치하는 키가 없습니다.")
                    
            except Exception as e:
                st.error(f"키 검색 오류: {e}")
        
        # 개별 키 조회
        st.markdown("### 🔍 개별 키 조회")
        
        specific_key = st.text_input("조회할 키 이름")
        
        if specific_key and st.button("📋 키 값 조회"):
            try:
                if redis_client.redis_client.exists(specific_key):
                    value = redis_client.redis_client.get(specific_key)
                    key_type = redis_client.redis_client.type(specific_key)
                    ttl = redis_client.redis_client.ttl(specific_key)
                    
                    st.success("키 정보:")
                    st.text(f"Type: {key_type}")
                    st.text(f"TTL: {ttl if ttl > 0 else 'No expiry'}")
                    st.text("Value:")
                    
                    try:
                        # JSON 형태라면 포맷팅해서 표시
                        json_value = json.loads(value)
                        st.json(json_value)
                    except:
                        # 일반 텍스트로 표시
                        st.text(value)
                else:
                    st.error("키가 존재하지 않습니다.")
                    
            except Exception as e:
                st.error(f"키 조회 오류: {e}")
    
    # 자동 새로고침
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

except Exception as e:
    st.error(f"❌ Redis 연결 오류: {str(e)}")
    st.info("Redis 서버가 실행 중인지 확인해주세요.")

# 푸터
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <small>📡 Redis Real-time Monitoring | Stock Pipeline Dashboard</small>
    </div>
    """,
    unsafe_allow_html=True
)
