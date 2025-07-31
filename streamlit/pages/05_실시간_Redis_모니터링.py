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

# 다크 테마 CSS 적용
st.markdown("""
<style>
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    .stDataFrame {
        background-color: #262730;
    }
    .stMetric {
        background-color: #262730;
        padding: 10px;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

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
            # Redis에서 직접 기술적 지표 데이터를 조회하여 신호 생성
            indicators_keys = redis_client.redis_client.keys("indicators:*")
            active_signals = []
            
            for key in indicators_keys:
                symbol = key.replace("indicators:", "")
                indicator_data_str = redis_client.redis_client.get(key)
                
                if indicator_data_str:
                    try:
                        indicator_data = json.loads(indicator_data_str)
                        
                        # 실시간 가격 데이터 조회
                        realtime_key = f"realtime:{symbol}"
                        realtime_data_str = redis_client.redis_client.get(realtime_key)
                        current_price = None
                        
                        if realtime_data_str:
                            realtime_data = json.loads(realtime_data_str)
                            current_price = realtime_data.get('price')
                        
                        # 신호 조건 확인
                        rsi = indicator_data.get('rsi')
                        macd = indicator_data.get('macd')
                        macd_signal = indicator_data.get('macd_signal')
                        bb_upper = indicator_data.get('bb_upper')
                        signals = indicator_data.get('signals', [])
                        
                        detected_signals = []
                        
                        # RSI 기반 신호
                        if rsi and rsi > 70:
                            detected_signals.append({
                                'symbol': symbol,
                                'signal_type': 'rsi_overbought',
                                'trigger_price': current_price or indicator_data.get('current_price'),
                                'current_price': current_price,
                                'rsi_value': rsi,
                                'trigger_time': indicator_data.get('calculation_time'),
                                'strength': abs(rsi - 70)
                            })
                        elif rsi and rsi < 30:
                            detected_signals.append({
                                'symbol': symbol,
                                'signal_type': 'rsi_oversold',
                                'trigger_price': current_price or indicator_data.get('current_price'),
                                'current_price': current_price,
                                'rsi_value': rsi,
                                'trigger_time': indicator_data.get('calculation_time'),
                                'strength': abs(30 - rsi)
                            })
                        
                        # MACD 기반 신호
                        if macd and macd_signal and macd > macd_signal:
                            detected_signals.append({
                                'symbol': symbol,
                                'signal_type': 'macd_bullish',
                                'trigger_price': current_price or indicator_data.get('current_price'),
                                'current_price': current_price,
                                'macd_value': macd,
                                'trigger_time': indicator_data.get('calculation_time'),
                                'strength': abs(macd - macd_signal)
                            })
                        
                        # 볼린저 밴드 신호 (신호 리스트에서 확인)
                        if signals and any('볼린저' in str(signal) for signal in signals):
                            detected_signals.append({
                                'symbol': symbol,
                                'signal_type': 'bollinger_upper_touch',
                                'trigger_price': current_price or indicator_data.get('current_price'),
                                'current_price': current_price,
                                'trigger_time': indicator_data.get('calculation_time'),
                                'strength': 5  # 기본값
                            })
                        
                        active_signals.extend(detected_signals)
                        
                    except json.JSONDecodeError:
                        continue
            
            if active_signals:
                st.success(f"🎯 현재 활성 신호: {len(active_signals)}개")
                
                # 신호 성과 계산 및 표시
                signal_performance = []
                
                for signal in active_signals:
                    symbol = signal['symbol']
                    trigger_price = signal.get('trigger_price', 0)
                    current_price = signal.get('current_price', trigger_price)
                    
                    # 성과 계산
                    if trigger_price and current_price and trigger_price > 0:
                        price_change_pct = ((current_price - trigger_price) / trigger_price) * 100
                    else:
                        price_change_pct = 0
                    
                    # 신호 타입 한글 변환
                    signal_type_names = {
                        'bollinger_upper_touch': '🔴 볼린저 상단 터치',
                        'rsi_overbought': '📈 RSI 과매수',
                        'rsi_oversold': '📉 RSI 과매도',
                        'macd_bullish': '🟢 MACD 상승'
                    }
                    
                    # 성과 색상 결정
                    if price_change_pct > 2:
                        performance_icon = "🟢"
                        performance_color = "green"
                    elif price_change_pct > 0:
                        performance_icon = "🔵"
                        performance_color = "blue"
                    elif price_change_pct < -2:
                        performance_icon = "🔴"
                        performance_color = "red"
                    else:
                        performance_icon = "🟡"
                        performance_color = "orange"
                    
                    signal_performance.append({
                        'Symbol': signal['symbol'],
                        'Signal': signal_type_names.get(signal['signal_type'], signal['signal_type']),
                        'Trigger Price': f"${trigger_price:.2f}" if trigger_price else "N/A",
                        'Current Price': f"${current_price:.2f}" if current_price else "N/A",
                        'Change': f"{price_change_pct:+.2f}%" if price_change_pct != 0 else "0.00%",
                        'Performance': performance_icon,
                        'Trigger Time': format_korean_time(signal.get('trigger_time', 'N/A')),
                        'Strength': f"{signal.get('strength', 0):.2f}",
                        '_change_pct': price_change_pct,
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
                    display_df = df[['Symbol', 'Signal', 'Trigger Price', 'Current Price', 'Change', 'Performance', 'Strength', 'Trigger Time']]
                    
                    # 조건부 포맷팅 - 다크 테마 적용
                    def highlight_performance(row):
                        # display_df 컬럼 수에 맞춰 스타일 적용
                        change_pct = df.loc[row.name, '_change_pct']
                        
                        if change_pct > 2:
                            color = 'background-color: #1e3d2f; color: #7bc96f;'  # 진한 녹색 배경 + 밝은 녹색 텍스트
                        elif change_pct > 0:
                            color = 'background-color: #1e2c3d; color: #70a7ff;'  # 진한 파란색 배경 + 밝은 파란색 텍스트
                        elif change_pct < -2:
                            color = 'background-color: #3d1e1e; color: #ff7070;'  # 진한 빨간색 배경 + 밝은 빨간색 텍스트
                        else:
                            color = 'background-color: #3d3d1e; color: #ffff70;'  # 진한 황색 배경 + 밝은 황색 텍스트
                        
                        # display_df의 컬럼 수만큼 반환
                        return [color] * len(display_df.columns)
                    
                    styled_df = display_df.style.apply(highlight_performance, axis=1)
                    
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
            # 실제 키 패턴에 맞게 수정
            watchlist_keys = redis_client.redis_client.keys("watchlist:*")
            realtime_keys = redis_client.redis_client.keys("realtime:*")
            indicators_keys = redis_client.redis_client.keys("indicators:*")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("관심종목 데이터", len(watchlist_keys))
            
            with col2:
                st.metric("실시간 데이터", len(realtime_keys))
            
            with col3:
                st.metric("기술적 지표", len(indicators_keys))
                
            if watchlist_keys:
                st.success("✅ 관심종목 데이터가 Redis에 로딩되어 있습니다.")
                
                # 관심종목 데이터 샘플 표시
                sample_data = []
                for key in watchlist_keys[:5]:
                    symbol = key.replace("watchlist:", "")
                    watchlist_data_str = redis_client.redis_client.hgetall(key)
                    if watchlist_data_str:
                        sample_data.append({
                            'Symbol': symbol,
                            'Status': watchlist_data_str.get('status', 'N/A'),
                            'Added Date': format_korean_time(watchlist_data_str.get('added_date', 'N/A')),
                            'Notes': watchlist_data_str.get('notes', 'N/A')
                        })
                
                if sample_data:
                    st.markdown("#### 📋 관심종목 샘플 데이터")
                    st.dataframe(pd.DataFrame(sample_data), use_container_width=True, hide_index=True)
            else:
                st.warning("⚠️ 관심종목 데이터를 Redis에 로딩해야 합니다.")
                st.info("💡 Redis 컨슈머가 실행되면 자동으로 관심종목 데이터가 로딩됩니다.")
        
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
                # 모든 실시간 가격 키 조회 (실제 키 패턴: realtime:SYMBOL)
                price_keys = redis_client.redis_client.keys("realtime:*")
                
                if price_keys:
                    prices_data = []
                    for key in price_keys[:10]:  # 최대 10개만 표시
                        symbol = key.replace("realtime:", "")
                        
                        # Redis에서 JSON 형태로 저장된 데이터 조회
                        price_data_str = redis_client.redis_client.get(key)
                        if price_data_str:
                            try:
                                price_data = json.loads(price_data_str)
                                prices_data.append({
                                    'Symbol': symbol,
                                    'Price': f"${price_data.get('price', 'N/A'):.2f}" if isinstance(price_data.get('price'), (int, float)) else 'N/A',
                                    'Source': price_data.get('source', 'N/A'),
                                    'Change': price_data.get('change', 'N/A'),
                                    'Volume': price_data.get('volume', 'N/A'),
                                    'Timestamp': format_korean_time(price_data.get('timestamp', 'N/A'))
                                })
                            except json.JSONDecodeError:
                                continue
                    
                    if prices_data:
                        df = pd.DataFrame(prices_data)
                        st.dataframe(df, use_container_width=True, hide_index=True)
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
                # 실제 키 패턴: indicators:SYMBOL
                indicators_keys = redis_client.redis_client.keys("indicators:*")
                
                if indicators_keys:
                    indicators_data = []
                    for key in indicators_keys[:10]:
                        symbol = key.replace("indicators:", "")
                        
                        # Redis에서 JSON 형태로 저장된 데이터 조회
                        indicator_data_str = redis_client.redis_client.get(key)
                        if indicator_data_str:
                            try:
                                indicator_data = json.loads(indicator_data_str)
                                indicators_data.append({
                                    'Symbol': symbol,
                                    'RSI': f"{indicator_data.get('rsi', 'N/A'):.2f}" if isinstance(indicator_data.get('rsi'), (int, float)) else 'N/A',
                                    'MACD': f"{indicator_data.get('macd', 'N/A'):.4f}" if isinstance(indicator_data.get('macd'), (int, float)) else 'N/A',
                                    'SMA 5': f"{indicator_data.get('sma_5', 'N/A'):.2f}" if isinstance(indicator_data.get('sma_5'), (int, float)) else 'N/A',
                                    'Sentiment': indicator_data.get('overall_sentiment', 'N/A'),
                                    'Strength': indicator_data.get('strength', 'N/A')
                                })
                            except json.JSONDecodeError:
                                continue
                    
                    if indicators_data:
                        df = pd.DataFrame(indicators_data)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        st.info("기술적 지표 데이터가 없습니다.")
                else:
                    st.info("Redis에 저장된 기술적 지표 데이터가 없습니다.")
                    
            except Exception as e:
                st.error(f"기술적 지표 데이터 조회 오류: {e}")
    
    with tab3:
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
    
    with tab4:
        st.subheader("📈 캐시 성능 통계")
        
        try:
            # Redis 기본 정보에서 통계 계산
            info = redis_client.redis_client.info()
            
            # 기본 메트릭 계산
            total_commands = info.get('total_commands_processed', 0)
            keyspace_hits = info.get('keyspace_hits', 0)
            keyspace_misses = info.get('keyspace_misses', 0)
            
            # 히트율 계산
            total_requests = keyspace_hits + keyspace_misses
            hit_rate = (keyspace_hits / total_requests) if total_requests > 0 else 0
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("캐시 히트율", f"{hit_rate:.1%}")
                st.metric("총 요청", total_requests)
            
            with col2:
                st.metric("캐시 미스", keyspace_misses)
                st.metric("초당 명령어", info.get('instantaneous_ops_per_sec', 0))
            
            # 캐시 타입별 통계
            st.markdown("### 📊 캐시 타입별 사용량")
            
            cache_types = ['realtime', 'indicators', 'watchlist', 'signal_trigger']
            type_stats = []
            
            for cache_type in cache_types:
                keys = redis_client.redis_client.keys(f"{cache_type}:*")
                type_stats.append({
                    'Cache Type': cache_type,
                    'Key Count': len(keys),
                    'Status': '✅ Active' if len(keys) > 0 else '❌ Empty'
                })
            
            df = pd.DataFrame(type_stats)
            st.dataframe(df, use_container_width=True)
            
        except Exception as e:
            st.error(f"캐시 통계 조회 오류: {e}")
    
    with tab5:
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
