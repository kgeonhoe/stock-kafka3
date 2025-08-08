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
    
    # 관심종목 동기화 섹션
    st.sidebar.markdown("---")
    st.sidebar.subheader("📊 관심종목 관리")
    
    if st.sidebar.button("🔄 관심종목 동기화", help="PostgreSQL에서 Redis로 관심종목 데이터를 동기화합니다"):
        with st.spinner("관심종목 동기화 중..."):
            try:
                # Docker 컨테이너에서 동기화 스크립트 실행
                import subprocess
                import os
                
                # 현재 작업 디렉토리 확인
                docker_path = "/home/grey1/stock-kafka3/docker"
                
                if os.path.exists(docker_path):
                    # 경로가 존재하는 경우 실행
                    result = subprocess.run([
                        "docker", "compose", "exec", "-T", "airflow-scheduler", 
                        "python", "/opt/airflow/scripts/load_watchlist_to_redis.py", 
                        "--mode", "smart"
                    ], 
                    cwd=docker_path,
                    capture_output=True, 
                    text=True, 
                    timeout=300
                    )
                else:
                    # 절대 경로로 Docker Compose 파일 지정
                    result = subprocess.run([
                        "docker", "compose", "-f", "/home/grey1/stock-kafka3/docker/docker-compose.yml",
                        "exec", "-T", "airflow-scheduler", 
                        "python", "/opt/airflow/scripts/load_watchlist_to_redis.py", 
                        "--mode", "smart"
                    ], 
                    capture_output=True, 
                    text=True, 
                    timeout=300
                    )
                
                if result.returncode == 0:
                    st.sidebar.success("✅ 관심종목 동기화 완료!")
                    if result.stdout:
                        with st.sidebar.expander("📋 동기화 로그 보기"):
                            st.code(result.stdout)
                else:
                    st.sidebar.error("❌ 동기화 실패")
                    if result.stderr:
                        with st.sidebar.expander("🚨 오류 로그 보기"):
                            st.code(result.stderr)
                            
            except subprocess.TimeoutExpired:
                st.sidebar.error("⏰ 동기화 타임아웃 (5분 초과)")
            except FileNotFoundError as e:
                st.sidebar.error(f"❌ Docker 명령어를 찾을 수 없습니다: {str(e)}")
                st.sidebar.info("💡 대안: Airflow 웹 UI에서 redis_watchlist_sync DAG을 수동 실행해보세요.")
            except Exception as e:
                st.sidebar.error(f"❌ 동기화 오류: {str(e)}")
                st.sidebar.info("💡 문제가 지속되면 터미널에서 수동으로 실행해보세요.")
    
    # 동기화 모드 선택
    sync_mode = st.sidebar.selectbox(
        "동기화 모드",
        ["smart", "incremental", "full"],
        help="smart: 자동 최적화, incremental: 증분 업데이트, full: 전체 재로딩"
    )
    
    if st.sidebar.button("🚀 고급 동기화 실행"):
        with st.spinner(f"{sync_mode} 모드로 동기화 중..."):
            try:
                import subprocess
                import os
                
                # Docker 경로 확인 및 명령어 구성
                docker_path = "/home/grey1/stock-kafka3/docker"
                cmd = [
                    "docker", "compose", "exec", "-T", "airflow-scheduler", 
                    "python", "/opt/airflow/scripts/load_watchlist_to_redis.py", 
                    "--mode", sync_mode
                ]
                
                if sync_mode == "full":
                    cmd.extend(["--days", "60"])
                elif sync_mode == "incremental":
                    cmd.extend(["--days", "7"])
                
                # 경로가 없는 경우 docker-compose 파일을 직접 지정
                if not os.path.exists(docker_path):
                    cmd = [
                        "docker", "compose", "-f", "/home/grey1/stock-kafka3/docker/docker-compose.yml",
                        "exec", "-T", "airflow-scheduler", 
                        "python", "/opt/airflow/scripts/load_watchlist_to_redis.py", 
                        "--mode", sync_mode
                    ]
                    
                    if sync_mode == "full":
                        cmd.extend(["--days", "60"])
                    elif sync_mode == "incremental":
                        cmd.extend(["--days", "7"])
                    
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
                else:
                    result = subprocess.run(cmd, cwd=docker_path, capture_output=True, text=True, timeout=600)
                
                if result.returncode == 0:
                    st.sidebar.success(f"✅ {sync_mode} 동기화 완료!")
                    if result.stdout:
                        with st.sidebar.expander("📋 동기화 로그 보기"):
                            st.code(result.stdout)
                else:
                    st.sidebar.error("❌ 동기화 실패")
                    if result.stderr:
                        with st.sidebar.expander("🚨 오류 로그 보기"):
                            st.code(result.stderr)
                            
            except subprocess.TimeoutExpired:
                st.sidebar.error("⏰ 동기화 타임아웃 (10분 초과)")
            except FileNotFoundError as e:
                st.sidebar.error(f"❌ Docker 명령어를 찾을 수 없습니다: {str(e)}")
                st.sidebar.info("💡 대안: Airflow 웹 UI에서 redis_watchlist_sync DAG을 수동 실행해보세요.")
            except Exception as e:
                st.sidebar.error(f"❌ 동기화 오류: {str(e)}")
                st.sidebar.info("💡 문제가 지속되면 터미널에서 수동으로 실행해보세요.")
    
    st.sidebar.markdown("---")
    
    # 메인 컨텐츠
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["� 신호 추적", "�📊 실시간 데이터", "🔧 Redis 상태", "📈 캐시 통계", "🔑 키 관리"])
    
    with tab1:
        st.subheader("🚨 기술적 신호 추적 대시보드")
        
        # 활성 신호 조회
        try:
            # PostgreSQL daily_watchlist에서 활성 관심종목 목록 먼저 가져오기
            valid_watchlist_symbols = set()
            try:
                # PostgreSQL에서 실제 관심종목 목록 조회
                from common.database import PostgreSQLManager
                db_manager = PostgreSQLManager()
                query = """
                SELECT DISTINCT symbol 
                FROM daily_watchlist 
                ORDER BY symbol
                """
                result = db_manager.execute_query(query)
                
                # DataFrame 체크를 올바르게 수정
                if result is not None:
                    if hasattr(result, 'empty'):  # DataFrame인 경우
                        if not result.empty:
                            valid_watchlist_symbols = set(result['symbol'].tolist())
                            print(f"✅ PostgreSQL에서 {len(valid_watchlist_symbols)}개 관심종목 확인: {list(valid_watchlist_symbols)}")
                        else:
                            print("⚠️ PostgreSQL daily_watchlist가 비어있음")
                    elif result:  # tuple/list인 경우
                        valid_watchlist_symbols = set([row[0] for row in result])
                        print(f"✅ PostgreSQL에서 {len(valid_watchlist_symbols)}개 관심종목 확인: {list(valid_watchlist_symbols)}")
                else:
                    print("⚠️ PostgreSQL daily_watchlist가 비어있음")
                    
                db_manager.close()
            except Exception as db_e:
                print(f"❌ PostgreSQL 관심종목 조회 실패: {db_e}")
            
            # 기존 저장된 활성 신호들 조회 (관심종목에 있는 것만)
            stored_signals_keys = redis_client.redis_client.keys("active_signal:*")
            active_signals = []
            
            # 저장된 신호들 처리
            for key in stored_signals_keys:
                signal_data_str = redis_client.redis_client.get(key)
                if signal_data_str:
                    try:
                        stored_signal = json.loads(signal_data_str)
                        symbol = stored_signal['symbol']
                        
                        # 관심종목에 없는 종목의 신호는 제거
                        if valid_watchlist_symbols and symbol not in valid_watchlist_symbols:
                            print(f"🗑️ {symbol}: 관심종목에 없는 종목의 신호 제거")
                            redis_client.redis_client.delete(key)
                            continue
                        
                        # trigger_price가 없거나 유효하지 않은 신호는 제거
                        if not stored_signal.get('trigger_price') or stored_signal.get('trigger_price') <= 0:
                            print(f"🗑️ {symbol}: 유효하지 않은 trigger_price 신호 제거")
                            redis_client.redis_client.delete(key)
                            continue
                        
                        # 현재 실시간 가격 조회
                        realtime_key = f"realtime:{symbol}"
                        realtime_data_str = redis_client.redis_client.get(realtime_key)
                        current_price = None
                        
                        if realtime_data_str:
                            realtime_data = json.loads(realtime_data_str)
                            current_price = realtime_data.get('price')
                        
                        # 저장된 신호에 현재 가격 추가
                        stored_signal['current_price'] = current_price
                        active_signals.append(stored_signal)
                        
                    except json.JSONDecodeError:
                        continue
            
            # 신규 신호 감지 및 저장 (관심종목에 있는 것만)
            indicators_keys = redis_client.redis_client.keys("indicators:*")
            
            for key in indicators_keys:
                symbol = key.replace("indicators:", "")
                
                # 관심종목에 없는 종목은 건너뛰기
                if valid_watchlist_symbols and symbol not in valid_watchlist_symbols:
                    print(f"⏭️ {symbol}: 관심종목에 없는 종목, 신호 생성 건너뛰기")
                    continue
                
                indicator_data_str = redis_client.redis_client.get(key)
                
                if indicator_data_str:
                    try:
                        indicator_data = json.loads(indicator_data_str)
                        
                        # 실시간 가격 데이터 조회 (현재 가격)
                        realtime_key = f"realtime:{symbol}"
                        realtime_data_str = redis_client.redis_client.get(realtime_key)
                        current_price = None
                        current_timestamp = None
                        
                        if realtime_data_str:
                            try:
                                realtime_data = json.loads(realtime_data_str)
                                current_price = realtime_data.get('price')
                                current_timestamp = realtime_data.get('timestamp')
                            except json.JSONDecodeError:
                                pass
                        
                        # 지표 계산 시점의 가격 (신호 발생 기준가)
                        indicator_price = indicator_data.get('current_price')
                        indicator_timestamp = indicator_data.get('calculation_time')
                        
                        # 신호 조건 확인 (실시간 기술적 지표 기반)
                        rsi = indicator_data.get('rsi')
                        macd = indicator_data.get('macd')
                        macd_signal = indicator_data.get('macd_signal')
                        bb_upper = indicator_data.get('bb_upper')
                        bb_lower = indicator_data.get('bb_lower')
                        signals = indicator_data.get('signals', [])
                        
                        detected_signals = []
                        
                        # 활성 신호의 Trigger Price: 실시간 기술적 조건 충족 시점의 현재가
                        # (관심종목 등록 시점과는 다른 개념)
                        realtime_trigger_price = current_price or indicator_price or 100.0
                        
                        # RSI 기반 신호 (새로운 실시간 조건 충족)
                        if rsi and rsi > 70:
                            signal_key = f"active_signal:{symbol}:rsi_overbought"
                            if not redis_client.redis_client.exists(signal_key):
                                # 실시간 RSI 과매수 조건 충족 시점의 가격을 Trigger Price로 사용
                                
                                new_signal = {
                                    'symbol': symbol,
                                    'signal_type': 'rsi_overbought',
                                    'trigger_price': realtime_trigger_price,  # 실시간 RSI 조건 충족 시점가
                                    'current_price': current_price,  # 현재 실시간 가격
                                    'rsi_value': rsi,
                                    'trigger_time': current_timestamp or indicator_timestamp,  # 실시간 시점
                                    'strength': abs(rsi - 70),  # RSI 70 기준점에서 얼마나 벗어났는지
                                    'created_at': current_timestamp or indicator_timestamp,
                                    'signal_description': f'실시간 RSI({rsi:.1f}) > 70 조건 충족'
                                }
                                # Redis에 신호 저장 (24시간 TTL)
                                redis_client.redis_client.setex(signal_key, 86400, json.dumps(new_signal))
                                detected_signals.append(new_signal)
                        
                        elif rsi and rsi < 30:
                            signal_key = f"active_signal:{symbol}:rsi_oversold"
                            if not redis_client.redis_client.exists(signal_key):
                                # 실시간 RSI 과매도 조건 충족 시점의 가격을 Trigger Price로 사용
                                
                                new_signal = {
                                    'symbol': symbol,
                                    'signal_type': 'rsi_oversold',
                                    'trigger_price': realtime_trigger_price,  # 실시간 RSI 조건 충족 시점가
                                    'current_price': current_price,  # 현재 실시간 가격
                                    'rsi_value': rsi,
                                    'trigger_time': current_timestamp or indicator_timestamp,  # 실시간 시점
                                    'strength': abs(30 - rsi),  # RSI 30 기준점에서 얼마나 벗어났는지
                                    'created_at': current_timestamp or indicator_timestamp,
                                    'signal_description': f'실시간 RSI({rsi:.1f}) < 30 조건 충족'
                                }
                                redis_client.redis_client.setex(signal_key, 86400, json.dumps(new_signal))
                                detected_signals.append(new_signal)
                        
                        # MACD 기반 신호 (실시간 조건 충족)
                        if macd and macd_signal and macd > macd_signal:
                            signal_key = f"active_signal:{symbol}:macd_bullish"
                            if not redis_client.redis_client.exists(signal_key):
                                # 실시간 MACD 상승 신호 조건 충족 시점의 가격을 Trigger Price로 사용
                                
                                new_signal = {
                                    'symbol': symbol,
                                    'signal_type': 'macd_bullish',
                                    'trigger_price': realtime_trigger_price,  # 실시간 MACD 조건 충족 시점가
                                    'current_price': current_price,  # 현재 실시간 가격
                                    'macd_value': macd,
                                    'trigger_time': current_timestamp or indicator_timestamp,  # 실시간 시점
                                    'strength': abs(macd - macd_signal),  # MACD와 Signal의 차이
                                    'created_at': current_timestamp or indicator_timestamp,
                                    'signal_description': f'실시간 MACD({macd:.3f}) > Signal({macd_signal:.3f}) 조건 충족'
                                }
                                redis_client.redis_client.setex(signal_key, 86400, json.dumps(new_signal))
                                detected_signals.append(new_signal)
                        
                        # 볼린저 밴드 신호 (실시간 조건 충족)
                        if current_price and bb_upper and current_price >= bb_upper * 0.995:  # 볼린저 상단 근접
                            signal_key = f"active_signal:{symbol}:bollinger_upper_touch"
                            if not redis_client.redis_client.exists(signal_key):
                                # 실시간 볼린저 상단 터치 조건 충족 시점의 가격을 Trigger Price로 사용
                                
                                new_signal = {
                                    'symbol': symbol,
                                    'signal_type': 'bollinger_upper_touch',
                                    'trigger_price': realtime_trigger_price,  # 실시간 볼린저 조건 충족 시점가
                                    'current_price': current_price,  # 현재 실시간 가격
                                    'bb_upper': bb_upper,
                                    'trigger_time': current_timestamp or indicator_timestamp,  # 실시간 시점
                                    'strength': ((current_price - bb_upper) / bb_upper) * 100 if bb_upper > 0 else 0,
                                    'created_at': current_timestamp or indicator_timestamp,
                                    'signal_description': f'실시간 가격({current_price:.2f}) 볼린저 상단({bb_upper:.2f}) 터치'
                                }
                                redis_client.redis_client.setex(signal_key, 86400, json.dumps(new_signal))
                                detected_signals.append(new_signal)
                        
                        elif current_price and bb_lower and current_price <= bb_lower * 1.005:  # 볼린저 하단 근접
                            signal_key = f"active_signal:{symbol}:bollinger_lower_touch"
                            if not redis_client.redis_client.exists(signal_key):
                                # 실시간 볼린저 하단 터치 조건 충족 시점의 가격을 Trigger Price로 사용
                                
                                new_signal = {
                                    'symbol': symbol,
                                    'signal_type': 'bollinger_lower_touch',
                                    'trigger_price': realtime_trigger_price,  # 실시간 볼린저 조건 충족 시점가
                                    'current_price': current_price,  # 현재 실시간 가격
                                    'bb_lower': bb_lower,
                                    'trigger_time': current_timestamp or indicator_timestamp,  # 실시간 시점
                                    'strength': ((bb_lower - current_price) / bb_lower) * 100 if bb_lower > 0 else 0,
                                    'created_at': current_timestamp or indicator_timestamp,
                                    'signal_description': f'실시간 가격({current_price:.2f}) 볼린저 하단({bb_lower:.2f}) 터치'
                                }
                                redis_client.redis_client.setex(signal_key, 86400, json.dumps(new_signal))
                                detected_signals.append(new_signal)
                        
                        active_signals.extend(detected_signals)
                        
                    except json.JSONDecodeError:
                        continue
            
            if active_signals:
                st.success(f"🎯 현재 활성 신호: {len(active_signals)}개")
                
                # 신호 성과 계산 및 표시
                signal_performance = []
                
                for signal in active_signals:
                    symbol = signal['symbol']
                    trigger_price = signal.get('trigger_price')
                    current_price = signal.get('current_price')  # None일 수 있음
                    
                    # trigger_price가 None이거나 0인 경우 처리
                    if not trigger_price:
                        # 실시간 데이터에서 현재 가격을 trigger_price로 사용
                        realtime_key = f"realtime:{symbol}"
                        realtime_data_str = redis_client.redis_client.get(realtime_key)
                        if realtime_data_str:
                            try:
                                realtime_data = json.loads(realtime_data_str)
                                trigger_price = realtime_data.get('price')
                                print(f"[DEBUG] {symbol}: trigger_price 보완={trigger_price}")
                            except:
                                trigger_price = 100.0  # 기본값
                        else:
                            trigger_price = 100.0  # 기본값
                    
                    # 현재 가격이 없으면 실시간 데이터에서 다시 조회 시도
                    if current_price is None:
                        realtime_key = f"realtime:{symbol}"
                        realtime_data_str = redis_client.redis_client.get(realtime_key)
                        if realtime_data_str:
                            try:
                                realtime_data = json.loads(realtime_data_str)
                                current_price = realtime_data.get('price')
                                print(f"[DEBUG] {symbol}: 재조회 current_price={current_price}")
                            except:
                                pass
                    
                    # 여전히 현재 가격이 없으면 표시용으로만 trigger_price 사용하되, 성과 계산은 건너뛰기
                    display_current_price = current_price if current_price is not None else trigger_price
                    
                    # 성과 계산 (실제 현재 가격이 있을 때만)
                    if trigger_price and current_price is not None and trigger_price > 0 and current_price != trigger_price:
                        price_change_pct = ((current_price - trigger_price) / trigger_price) * 100
                        price_change_abs = current_price - trigger_price
                    else:
                        price_change_pct = 0
                        price_change_abs = 0
                        if current_price is None:
                            print(f"[DEBUG] {symbol}: 현재 가격 없음 - 성과 계산 불가")
                        elif current_price == trigger_price:
                            print(f"[DEBUG] {symbol}: 현재가={current_price}, 트리거가={trigger_price} - 동일함")
                    
                    # 신호 타입 한글 변환 (실시간 조건 충족 기반)
                    signal_type_names = {
                        'bollinger_upper_touch': '🔴 실시간 볼린저 상단 터치',
                        'bollinger_lower_touch': '🟢 실시간 볼린저 하단 터치',
                        'rsi_overbought': '📈 실시간 RSI 과매수',
                        'rsi_oversold': '📉 실시간 RSI 과매도',
                        'macd_bullish': '🟢 실시간 MACD 상승'
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
                        'Trigger Price': f"${trigger_price:.2f}" if trigger_price and trigger_price > 0 else "N/A",
                        'Current Price': f"${display_current_price:.2f}" if display_current_price and display_current_price > 0 else "N/A",
                        'Price Change': f"${price_change_abs:+.2f}" if price_change_abs != 0 else "$0.00",
                        'Return (%)': f"{price_change_pct:+.2f}%" if price_change_pct != 0 else "0.00%",
                        'Performance': performance_icon,
                        'Signal Time': format_korean_time(signal.get('trigger_time', 'N/A')),
                        'Strength': f"{signal.get('strength', 0):.2f}",
                        'Status': "📊 Active" if current_price is not None and current_price != trigger_price else "⏸️ No Price Data",
                        'RSI': f"{signal.get('rsi_value', 'N/A'):.1f}" if signal.get('rsi_value') else '-',
                        'MACD': f"{signal.get('macd_value', 'N/A'):.3f}" if signal.get('macd_value') else '-',
                        'Description': signal.get('signal_description', 'N/A'),
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
                    
                    st.info("""
                    **💡 활성 신호의 Trigger Price 정의:**
                    - **Trigger Price**: 실시간으로 기술적 지표 조건이 충족된 시점의 주식 가격
                    - **Current Price**: 현재 실시간 주식 가격  
                    - **Return (%)**: (현재가 - Trigger Price) / Trigger Price × 100%
                    - **Strength**: 신호 강도 (RSI: 기준점 이탈 정도, MACD: 라인 간 차이, 볼린저: 밴드 이탈 정도)
                    - **실시간 조건**: 과거 관심종목 등록 조건과 별개로, 현재 시점에서 새로운 기술적 지표 조건 충족
                    """)
                    
                    # 성과에 따른 색상 코딩을 위한 스타일링
                    df = pd.DataFrame(signal_performance)
                    display_df = df[['Symbol', 'Signal', 'Trigger Price', 'Current Price', 'Price Change', 'Return (%)', 'Performance', 'RSI', 'MACD', 'Strength', 'Status', 'Description', 'Signal Time']]
                    
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
            # Redis에서 watchlist 데이터 조회 (날짜별로 저장된 형태)
            watchlist_keys = redis_client.redis_client.keys("watchlist:*")
            realtime_keys = redis_client.redis_client.keys("realtime:*")
            indicators_keys = redis_client.redis_client.keys("indicators:*")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("관심종목 키", len(watchlist_keys))
            
            with col2:
                st.metric("실시간 데이터", len(realtime_keys))
            
            with col3:
                st.metric("기술적 지표", len(indicators_keys))
                
            if watchlist_keys:
                st.success("✅ 관심종목 데이터가 Redis에 로딩되어 있습니다.")
                
                # 실제 관심종목 데이터 표시 (심볼별 키 구조에 맞게 수정)
                try:
                    # 심볼별 키에서 데이터 수집 (watchlist:SYMBOL 형태)
                    symbol_watchlist_data = {}
                    
                    for key in list(watchlist_keys)[:10]:  # 최대 10개만 처리
                        key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                        
                        # watchlist:SYMBOL 패턴에서 심볼 추출
                        if key_str.startswith('watchlist:') and len(key_str.split(':')) == 2:
                            symbol = key_str.split(':')[1]
                            
                            # 날짜 패턴이 아닌 심볼 키만 처리 (YYYY-MM-DD 제외)
                            if not (len(symbol) == 10 and symbol[4] == '-' and symbol[7] == '-'):  # 날짜 패턴이 아닌 경우
                                try:
                                    # Redis 키 타입 확인
                                    key_type = redis_client.redis_client.type(key_str)
                                    key_type_str = key_type.decode('utf-8') if isinstance(key_type, bytes) else str(key_type)
                                    
                                    if key_type_str == 'hash':
                                        # Hash 타입 키에서 모든 필드 조회
                                        hash_data = redis_client.redis_client.hgetall(key_str)
                                        if hash_data:
                                            # Hash 데이터를 딕셔너리로 변환
                                            hash_dict = {}
                                            for field, value in hash_data.items():
                                                field_str = field.decode('utf-8') if isinstance(field, bytes) else str(field)
                                                value_str = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                                                
                                                # JSON 파싱 시도
                                                try:
                                                    hash_dict[field_str] = json.loads(value_str)
                                                except json.JSONDecodeError:
                                                    hash_dict[field_str] = value_str
                                            
                                            # 관심종목 데이터 구조로 변환
                                            symbol_watchlist_data[symbol] = {
                                                'symbol': hash_dict.get('symbol', symbol),
                                                'added_date': hash_dict.get('added_date', datetime.now().isoformat()),
                                                'status': hash_dict.get('status', 'active'),
                                                'alerts_enabled': hash_dict.get('alerts_enabled', 'true'),
                                                'price_target': hash_dict.get('price_target', ''),
                                                'notes': hash_dict.get('notes', ''),
                                                'hash_fields': hash_dict,
                                                'date': datetime.now().strftime('%Y-%m-%d'),
                                                'last_updated': datetime.now().isoformat(),
                                                'data_type': 'hash'
                                            }
                                    
                                    elif key_type_str == 'string':
                                        # String 타입 키 처리 (기존 로직)
                                        watchlist_data_str = redis_client.redis_client.get(key_str)
                                        if watchlist_data_str:
                                            # 데이터 타입 확인 후 처리
                                            if isinstance(watchlist_data_str, bytes):
                                                watchlist_data_str = watchlist_data_str.decode('utf-8')
                                            
                                            # JSON 파싱 시도
                                            try:
                                                symbol_data = json.loads(watchlist_data_str)
                                                symbol_watchlist_data[symbol] = symbol_data
                                            except json.JSONDecodeError:
                                                # JSON이 아닌 경우 문자열로 처리
                                                symbol_watchlist_data[symbol] = {
                                                    'symbol': symbol,
                                                    'conditions': [{'type': 'string_data', 'value': watchlist_data_str[:50]}],
                                                    'date': datetime.now().strftime('%Y-%m-%d'),
                                                    'last_updated': datetime.now().isoformat(),
                                                    'data_type': 'string'
                                                }
                                    
                                    else:
                                        print(f"⚠️ 키 {key_str}는 {key_type_str} 타입입니다. 지원하지 않는 타입")
                                        
                                except Exception as key_error:
                                    print(f"키 {key_str} 처리 오류: {key_error}")
                                    continue
                    
                    # 수집된 데이터로 샘플 생성
                    sample_data = []
                    if symbol_watchlist_data:
                        for symbol, data in symbol_watchlist_data.items():
                            # 데이터가 딕셔너리인지 확인
                            if isinstance(data, dict):
                                data_type = data.get('data_type', 'unknown')
                                
                                if data_type == 'hash':
                                    # Hash 타입 데이터 처리
                                    status = data.get('status', 'unknown')
                                    alerts_enabled = data.get('alerts_enabled', 'false')
                                    added_date = data.get('added_date', 'N/A')
                                    notes = data.get('notes', '')
                                    
                                    # 조건 텍스트 구성
                                    condition_parts = []
                                    if status == 'active':
                                        condition_parts.append("✅ 활성")
                                    if alerts_enabled == 'true':
                                        condition_parts.append("🔔 알림")
                                    if notes:
                                        # 한글 디코딩 시도
                                        try:
                                            if isinstance(notes, str) and '\\x' in notes:
                                                # UTF-8 바이트 시퀀스 디코딩
                                                decoded_notes = bytes(notes.encode('utf-8').decode('unicode_escape'), 'utf-8').decode('utf-8')
                                            else:
                                                decoded_notes = str(notes)
                                            condition_parts.append(f"📝 {decoded_notes[:20]}")
                                        except:
                                            condition_parts.append(f"📝 {str(notes)[:20]}")
                                    
                                    condition_text = " | ".join(condition_parts) if condition_parts else "기본 관심종목"
                                    
                                    # 날짜 포맷팅
                                    try:
                                        if added_date and added_date != 'N/A':
                                            # ISO 형식 날짜를 한국어 형식으로 변환
                                            added_dt = datetime.fromisoformat(added_date.replace('Z', '+00:00'))
                                            formatted_date = added_dt.strftime('%Y-%m-%d')
                                            formatted_time = format_korean_time(added_date)
                                        else:
                                            formatted_date = datetime.now().strftime('%Y-%m-%d')
                                            formatted_time = format_korean_time(datetime.now().isoformat())
                                    except:
                                        formatted_date = datetime.now().strftime('%Y-%m-%d')
                                        formatted_time = format_korean_time(datetime.now().isoformat())
                                
                                else:
                                    # String 타입 또는 기타 데이터 처리
                                    conditions = data.get('conditions', [])
                                    condition_text = f"{len(conditions)} conditions" if conditions else "No conditions"
                                    if conditions and len(conditions) > 0:
                                        first_condition = conditions[0]
                                        if isinstance(first_condition, dict):
                                            condition_text = f"{first_condition.get('type', 'unknown')} ({first_condition.get('value', 'N/A')})"
                                    
                                    formatted_date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
                                    formatted_time = format_korean_time(data.get('last_updated', datetime.now().isoformat()))
                                    data_type = data.get('data_type', 'unknown')
                                
                                sample_data.append({
                                    'Symbol': symbol,
                                    'Status': '✅ Active' if data_type == 'hash' and data.get('status') == 'active' else '📊 Active',
                                    'Date': formatted_date if data_type == 'hash' else data.get('date', datetime.now().strftime('%Y-%m-%d')),
                                    'Condition': condition_text,
                                    'Updated': formatted_time if data_type == 'hash' else format_korean_time(data.get('last_updated', datetime.now().isoformat())),
                                    'Type': data_type.upper(),
                                    'Alerts': ('🔔 ON' if data.get('alerts_enabled') == 'true' else '🔕 OFF') if data_type == 'hash' else 'N/A'
                                })
                            else:
                                # 딕셔너리가 아닌 경우 기본 데이터로 처리
                                sample_data.append({
                                    'Symbol': symbol,
                                    'Status': '📊 Active',
                                    'Date': datetime.now().strftime('%Y-%m-%d'),
                                    'Condition': 'Raw data',
                                    'Updated': format_korean_time(datetime.now().isoformat()),
                                    'Type': 'RAW',
                                    'Alerts': 'N/A'
                                })
                    else:
                        st.warning("⚠️ 유효한 관심종목 데이터를 찾을 수 없습니다.")
                        # 디버그 정보 표시
                        st.write("Redis 키 타입 정보:")
                        for key in list(watchlist_keys)[:5]:
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            key_type = redis_client.redis_client.type(key_str)
                            st.write(f"- {key_str}: {key_type}")
                        sample_data = []  # 빈 리스트로 초기화
                    
                    if sample_data:
                                st.markdown("#### 📋 실제 관심종목 정보")
                                st.info("""
                                **📊 관심종목 정보의 Trigger Price 기준:**
                                - **Trigger Price**: PostgreSQL daily_watchlist 테이블 등록 시점의 기준가 (과거 등록일 종가)
                                - **등록날짜 종가**: 관심종목으로 등록한 날의 종가 (PostgreSQL daily_watchlist 테이블 기준)
                                - **신호 조건**: 볼린저 밴드, RSI, MACD 등 기술적 지표 조건 (등록 당시 조건)
                                - **성과 계산**: (현재가 - 등록시점 Trigger Price) / 등록시점 Trigger Price × 100%
                                - **보유일수**: 등록일로부터 경과 일수
                                - **활성 신호와 구별**: 등록 당시 조건 vs 실시간 신호 조건은 별개 개념
                                """)
                                
                                # 실제 DB 연동
                                try:
                                    # DB 연결 확인
                                    if 'db_manager' not in locals():
                                        from common.database import PostgreSQLManager
                                        db_manager = PostgreSQLManager()
                                    
                                    # PostgreSQL 테이블 상태 확인 (디버깅)
                                    try:
                                        count_query = "SELECT COUNT(*) FROM daily_watchlist"
                                        count_result = db_manager.execute_query(count_query)
                                        
                                        # DataFrame/tuple 안전하게 처리
                                        total_count = 0
                                        if count_result is not None:
                                            if hasattr(count_result, 'iloc'):  # DataFrame
                                                if not count_result.empty:
                                                    total_count = count_result.iloc[0, 0]
                                            elif count_result:  # tuple/list
                                                total_count = count_result[0][0] if count_result else 0
                                        
                                        print(f"[DEBUG] PostgreSQL daily_watchlist 총 레코드 수: {total_count}")
                                        
                                        if total_count > 0:
                                            sample_query = "SELECT DISTINCT symbol FROM daily_watchlist"
                                            sample_result = db_manager.execute_query(sample_query)
                                            
                                            sample_symbols = []
                                            if sample_result is not None:
                                                if hasattr(sample_result, 'empty'):  # DataFrame
                                                    if not sample_result.empty:
                                                        sample_symbols = sample_result['symbol'].tolist()
                                                elif sample_result:  # tuple/list
                                                    sample_symbols = [row[0] for row in sample_result]
                                            
                                            print(f"[DEBUG] PostgreSQL 샘플 심볼들: {sample_symbols}")
                                        
                                        # Redis 심볼들과 비교
                                        redis_symbols = [item['Symbol'] for item in sample_data]
                                        print(f"[DEBUG] Redis 심볼들: {redis_symbols}")
                                    except Exception as debug_e:
                                        print(f"[DEBUG] PostgreSQL 상태 확인 오류: {debug_e}")
                                    
                                    # PostgreSQL에서 실제 관심종목 성과 데이터 조회
                                    real_watchlist_sample = []
                                    
                                    # PostgreSQL에서 모든 관심종목 조회 (Redis 무관)
                                    all_symbols_query = """
                                    SELECT DISTINCT symbol 
                                    FROM daily_watchlist 
                                    ORDER BY symbol
                                    """
                                    all_symbols_result = db_manager.execute_query(all_symbols_query)
                                    
                                    # DataFrame이든 tuple이든 안전하게 처리
                                    all_db_symbols = []
                                    if all_symbols_result is not None:
                                        if hasattr(all_symbols_result, 'empty'):  # DataFrame인 경우
                                            if not all_symbols_result.empty:
                                                all_db_symbols = all_symbols_result['symbol'].tolist()
                                        elif all_symbols_result:  # tuple list인 경우
                                            all_db_symbols = [row[0] for row in all_symbols_result]
                                    
                                    print(f"[DEBUG] PostgreSQL에서 {len(all_db_symbols)}개 종목 조회")
                                    
                                    # PostgreSQL의 모든 종목에 대해 처리 (Redis 무관)
                                    for symbol in all_db_symbols:
                                        try:
                                            print(f"[DEBUG] {symbol}: PostgreSQL 관심종목 데이터 조회 중...")
                                            
                                            # DB 연결 체크
                                            if not hasattr(db_manager, 'connection') or db_manager.connection is None:
                                                print(f"[ERROR] {symbol}: DB 연결이 없음, 재연결 시도")
                                                db_manager = PostgreSQLManager()
                                            
                                            query = """
                                            SELECT 
                                                dw.symbol,
                                                dw.date as registered_date,
                                                sd.close AS trigger_price,
                                                dw.condition_type,
                                                dw.condition_value,
                                                dw.market_cap_tier,
                                                dw.created_at
                                            FROM daily_watchlist dw 
                                            LEFT JOIN stock_data sd 
                                            ON dw.date = sd.date 
                                            AND dw.symbol = sd.symbol 
                                            WHERE dw.symbol = %s 
                                            ORDER BY dw.created_at DESC 
                                            LIMIT 1
                                            """
                                            
                                            db_result = db_manager.execute_query(query, (symbol,))
                                            
                                            # DataFrame이든 tuple이든 안전하게 처리
                                            has_data = False
                                            if db_result is not None:
                                                if hasattr(db_result, 'empty'):  # DataFrame인 경우
                                                    has_data = not db_result.empty
                                                else:  # tuple list인 경우
                                                    has_data = bool(db_result)
                                            
                                            if has_data:
                                                # 실제 DB 데이터 처리 (기존 코드와 동일)
                                                if hasattr(db_result, 'iloc'):  # DataFrame인 경우
                                                    row = db_result.iloc[0]
                                                    registered_date = row['registered_date'] if 'registered_date' in row else row[1]
                                                    db_trigger_price = row['trigger_price'] if 'trigger_price' in row else row[2]
                                                    condition_type = row['condition_type'] if 'condition_type' in row else row[3]
                                                    condition_value = row['condition_value'] if 'condition_value' in row else row[4]
                                                    market_cap_tier = row['market_cap_tier'] if 'market_cap_tier' in row else row[5]
                                                    created_at = row['created_at'] if 'created_at' in row else row[6]
                                                else:  # tuple인 경우
                                                    row = db_result[0]
                                                    registered_date = row[1]
                                                    db_trigger_price = row[2]
                                                    condition_type = row[3]
                                                    condition_value = row[4]
                                                    market_cap_tier = row[5]
                                                    created_at = row[6]
                                                
                                                # Redis에서 현재 실시간 가격 조회
                                                realtime_key = f"realtime:{symbol}"
                                                realtime_data_str = redis_client.redis_client.get(realtime_key)
                                                current_price = None
                                                
                                                if realtime_data_str:
                                                    try:
                                                        realtime_data = json.loads(realtime_data_str)
                                                        price_value = realtime_data.get('price')
                                                        if price_value is not None:
                                                            current_price = float(price_value)
                                                            if current_price <= 0:
                                                                current_price = None
                                                    except (json.JSONDecodeError, ValueError, TypeError):
                                                        current_price = None
                                                
                                                # 가격 데이터 안전 처리
                                                safe_trigger_price = None
                                                safe_current_price = None
                                                
                                                # DB에서 가져온 trigger_price 사용
                                                if db_trigger_price and db_trigger_price > 0:
                                                    safe_trigger_price = db_trigger_price
                                                else:
                                                    safe_trigger_price = 100.0  # DB에 없을 때만 기본값
                                                
                                                # 유효한 current_price 설정
                                                if current_price and current_price > 0:
                                                    safe_current_price = current_price
                                                elif safe_trigger_price and safe_trigger_price > 0:
                                                    safe_current_price = safe_trigger_price
                                                else:
                                                    safe_current_price = 100.0
                                                
                                                # 성과 계산
                                                try:
                                                    if safe_trigger_price and safe_current_price and safe_trigger_price > 0:
                                                        price_change_pct = ((safe_current_price - safe_trigger_price) / safe_trigger_price) * 100
                                                    else:
                                                        price_change_pct = 0.0
                                                except (ZeroDivisionError, TypeError, ValueError):
                                                    price_change_pct = 0.0
                                                
                                                # 보유일수 계산
                                                holding_days = 0
                                                try:
                                                    if isinstance(registered_date, str):
                                                        reg_date_obj = datetime.strptime(registered_date[:10], '%Y-%m-%d')
                                                    elif hasattr(registered_date, 'year'):
                                                        reg_date_obj = registered_date
                                                    else:
                                                        reg_date_obj = datetime.now()
                                                    
                                                    holding_days = max(0, (datetime.now() - reg_date_obj).days)
                                                except:
                                                    reg_date_obj = datetime.now()
                                                    holding_days = 0
                                                
                                                # 조건 타입 한글 변환
                                                condition_names = {
                                                    'bollinger_upper_touch': '볼린저 상단',
                                                    'bollinger_lower_touch': '볼린저 하단', 
                                                    'rsi_oversold': 'RSI 과매도',
                                                    'rsi_overbought': 'RSI 과매수',
                                                    'macd_bullish': 'MACD 상승',
                                                    'macd_bearish': 'MACD 하락',
                                                    'volume_spike': '거래량 급증',
                                                    'price_breakout': '가격 돌파',
                                                    'support_bounce': '지지선 반등'
                                                }
                                                
                                                real_watchlist_sample.append({
                                                    'Symbol': symbol,
                                                    '등록일': reg_date_obj.strftime('%Y-%m-%d'),
                                                    'Trigger Price': f"${safe_trigger_price:.2f}",
                                                    '현재가': f"${safe_current_price:.2f}" if current_price else "N/A",
                                                    '수익률': f"{price_change_pct:+.2f}%" if price_change_pct != 0 else "0.00%",
                                                    '보유일수': f"{holding_days}일",
                                                    '감시조건': condition_names.get(condition_type, condition_type or 'N/A'),
                                                    '조건값': f"{condition_value:.1f}" if condition_value else "-",
                                                    '시장등급': market_cap_tier if market_cap_tier else "미분류",
                                                    '상태': '🟢 수익' if price_change_pct > 0 else '🔴 손실' if price_change_pct < 0 else '⚪ 보합',
                                                    '등록시간': registered_date.strftime('%H:%M') if hasattr(registered_date, 'strftime') else str(registered_date)[:16] if registered_date else "N/A",
                                                    '_price_change_pct': price_change_pct
                                                })
                                        
                                        except Exception as symbol_error:
                                            print(f"❌ {symbol} 처리 오류: {symbol_error}")
                                            # 에러가 발생해도 기본 데이터 추가
                                            real_watchlist_sample.append({
                                                'Symbol': symbol,
                                                '등록일': datetime.now().strftime('%Y-%m-%d'),
                                                'Trigger Price': "$N/A",
                                                '현재가': "$N/A",
                                                '수익률': "N/A",
                                                '보유일수': "N/A",
                                                '감시조건': "오류",
                                                '조건값': "-",
                                                '시장등급': "오류",
                                                '상태': '⚠️ 오류',
                                                '등록시간': "N/A",
                                                '_price_change_pct': 0.0
                                            })
                                    
                                    # 수익률로 정렬
                                    real_watchlist_sample.sort(key=lambda x: x.get('_price_change_pct', 0), reverse=True)
                                    
                                    print(f"[DEBUG] 최종 결과: Redis에서 {len(sample_data)}개 심볼, PostgreSQL에서 {len(real_watchlist_sample)}개 데이터 조회")
                                    
                                    # 성과 요약
                                    col1, col2, col3, col4 = st.columns(4)
                                    
                                    profit_count = len([x for x in real_watchlist_sample if '🟢' in x['상태']])
                                    loss_count = len([x for x in real_watchlist_sample if '🔴' in x['상태']])
                                    
                                    # Division by zero 방지
                                    if len(real_watchlist_sample) > 0:
                                        avg_return = sum(x.get('_price_change_pct', 0) for x in real_watchlist_sample) / len(real_watchlist_sample)
                                        profit_pct = (profit_count / len(real_watchlist_sample)) * 100
                                        loss_pct = (loss_count / len(real_watchlist_sample)) * 100
                                    else:
                                        avg_return = 0.0
                                        profit_pct = 0.0
                                        loss_pct = 0.0
                                    
                                    with col1:
                                        st.metric("📈 수익 종목", f"{profit_count}개", delta=f"{profit_pct:.1f}%")
                                    
                                    with col2:
                                        st.metric("📉 손실 종목", f"{loss_count}개", delta=f"-{loss_pct:.1f}%")
                                    
                                    with col3:
                                        st.metric("📊 평균 수익률", f"{avg_return:+.2f}%", delta=None)
                                    
                                    with col4:
                                        if real_watchlist_sample:
                                            best_performer = max(real_watchlist_sample, key=lambda x: x.get('_price_change_pct', 0))
                                            st.metric("🏆 최고 수익", best_performer['수익률'], delta=best_performer['Symbol'])
                                        else:
                                            st.metric("🏆 최고 수익", "N/A", delta="데이터 없음")
                                    
                                    st.dataframe(pd.DataFrame(real_watchlist_sample), use_container_width=True, hide_index=True)
                                    st.success(f"총 {len(symbol_watchlist_data)}개 관심종목 중 {len(real_watchlist_sample)}개 전체 표시 (실제 PostgreSQL 데이터)")
                                    
                                    # 데이터 소스 안내
                                    st.info(f"""
                                    **📋 데이터 소스 정보:**
                                    - **PostgreSQL 연동**: daily_watchlist 테이블에서 실제 등록 데이터 조회
                                    - **Redis 실시간**: 현재 주가 정보는 Redis에서 실시간 조회  
                                    - **관심종목 Trigger Price**: PostgreSQL에 저장된 등록 시점의 기준가 사용 (과거 고정값)
                                    - **활성 신호 Trigger Price**: 실시간 기술적 지표 조건 충족 시점의 가격 사용 (실시간 동적값)
                                    - **전체 표시**: 제한 없이 모든 {len(real_watchlist_sample)}개 종목 표시
                                    - **시뮬레이션 제거**: 더 이상 랜덤 데이터가 아닌 실제 DB 데이터 사용
                                    """)
                                    
                                except Exception as db_error:
                                    st.error(f"❌ PostgreSQL 연동 오류: {db_error}")
                                    st.warning("⚠️ 실제 DB 데이터 조회 실패, Redis 데이터만 표시합니다.")
                                    
                                    # DB 연동 실패 시 Redis 데이터로 대체
                                    fallback_watchlist_sample = []
                                    
                                    for item in sample_data:  # 전체 종목 표시
                                        symbol = item['Symbol']
                                        
                                        # Redis 실시간 가격 조회
                                        realtime_key = f"realtime:{symbol}"
                                        realtime_data_str = redis_client.redis_client.get(realtime_key)
                                        current_price = 150.0  # 기본값
                                        
                                        if realtime_data_str:
                                            try:
                                                realtime_data = json.loads(realtime_data_str)
                                                price_value = realtime_data.get('price', current_price)
                                                if price_value and float(price_value) > 0:
                                                    current_price = float(price_value)
                                            except (json.JSONDecodeError, ValueError, TypeError):
                                                pass
                                        
                                        # 안전한 가격 보장
                                        safe_price = max(current_price, 100.0) if current_price else 100.0
                                        
                                        fallback_watchlist_sample.append({
                                            'Symbol': symbol,
                                            '등록일': datetime.now().strftime('%Y-%m-%d'),
                                            'Trigger Price': f"${safe_price:.2f}",
                                            '현재가': f"${safe_price:.2f}",
                                            '수익률': "0.00%",
                                            '보유일수': "0일",
                                            '감시조건': "Redis 데이터만 사용",
                                            '조건값': "-",
                                            '시장등급': "Redis Fallback",
                                            '상태': '⚪ 신규',
                                            '등록시간': datetime.now().strftime('%H:%M')
                                        })
                                    
                                    if fallback_watchlist_sample:
                                        st.dataframe(pd.DataFrame(fallback_watchlist_sample), use_container_width=True, hide_index=True)
                                        st.info(f"총 {len(symbol_watchlist_data)}개 관심종목 중 {len(fallback_watchlist_sample)}개 표시 (Redis 데이터만)")
                                        st.info("""
                                        **� DB 연결 문제 해결 방법:**
                                        1. PostgreSQL 서버가 실행 중인지 확인
                                        2. Docker 컨테이너 상태 확인: `docker ps`
                                        3. 데이터베이스 연결 설정 확인
                                        4. daily_watchlist 테이블 존재 여부 확인
                                        """)
                                    
                                    # DB 매니저 정리
                                    try:
                                        if 'db_manager' in locals():
                                            db_manager.close()
                                    except:
                                        pass
                                        
                                except Exception as db_error:
                                    st.error(f"❌ PostgreSQL 연동 오류: {db_error}")
                                    st.warning("⚠️ 실제 DB 데이터 조회 실패, Redis 데이터만 표시합니다.")
                    else:
                        st.warning("📭 표시할 관심종목 데이터가 없습니다.")
                        
                except Exception as parse_error:
                    st.error(f"관심종목 데이터 파싱 오류: {parse_error}")
                    st.write("Debug info:")
                    st.write(f"Available keys: {[k.decode('utf-8') if isinstance(k, bytes) else k for k in watchlist_keys]}")
                    
                    # 추가 디버그 정보
                    st.write("키별 타입 확인:")
                    for key in list(watchlist_keys)[:5]:
                        key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                        try:
                            key_type = redis_client.redis_client.type(key_str)
                            st.write(f"- {key_str}: {key_type.decode('utf-8') if isinstance(key_type, bytes) else key_type}")
                        except Exception as type_error:
                            st.write(f"- {key_str}: Error getting type - {type_error}")
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
        
        # 관심종목 데이터 현황
        st.markdown("### 📊 관심종목 데이터 현황")
        
        try:
            # watchlist_data:* 키들 조회
            watchlist_keys = redis_client.redis_client.keys("watchlist_data:*")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("관심종목 개수", len(watchlist_keys))
            
            with col2:
                # 마지막 업데이트 시간 확인
                last_update_key = "watchlist_last_update"
                if redis_client.redis_client.exists(last_update_key):
                    last_update_data = redis_client.redis_client.get(last_update_key)
                    if last_update_data:
                        try:
                            update_info = json.loads(last_update_data)
                            timestamp = update_info.get('timestamp', 'Unknown')
                            st.metric("마지막 업데이트", timestamp)
                        except:
                            st.metric("마지막 업데이트", "Unknown")
                else:
                    st.metric("마지막 업데이트", "없음")
            
            with col3:
                # 실시간 데이터 개수
                realtime_keys = redis_client.redis_client.keys("realtime:*")
                st.metric("실시간 데이터", len(realtime_keys))
            
            # 관심종목 목록 표시
            if watchlist_keys:
                st.markdown("### 📋 관심종목 목록")
                
                watchlist_symbols = []
                for key in watchlist_keys:
                    symbol = key.decode('utf-8').replace('watchlist_data:', '')
                    try:
                        data = redis_client.redis_client.get(key)
                        if data:
                            symbol_data = json.loads(data)
                            metadata = symbol_data.get('metadata', {})
                            
                            watchlist_symbols.append({
                                'Symbol': symbol,
                                'Name': metadata.get('name', 'N/A'),
                                'Sector': metadata.get('sector', 'N/A'),
                                'Data Points': metadata.get('data_points', 0),
                                'Last Update': metadata.get('last_update', 'N/A')
                            })
                    except:
                        watchlist_symbols.append({
                            'Symbol': symbol,
                            'Name': 'Error',
                            'Sector': 'Error', 
                            'Data Points': 0,
                            'Last Update': 'Error'
                        })
                
                if watchlist_symbols:
                    df_watchlist = pd.DataFrame(watchlist_symbols)
                    st.dataframe(df_watchlist, use_container_width=True, hide_index=True)
                    
                    # 개별 종목 데이터 조회
                    selected_symbol = st.selectbox(
                        "상세 조회할 종목 선택:",
                        options=[item['Symbol'] for item in watchlist_symbols]
                    )
                    
                    if selected_symbol and st.button("📈 종목 데이터 상세보기"):
                        symbol_key = f"watchlist_data:{selected_symbol}"
                        try:
                            symbol_data = redis_client.redis_client.get(symbol_key)
                            if symbol_data:
                                data = json.loads(symbol_data)
                                
                                with st.expander(f"📊 {selected_symbol} 상세 데이터", expanded=True):
                                    # 메타데이터 표시
                                    metadata = data.get('metadata', {})
                                    st.write("**메타데이터:**")
                                    st.json(metadata)
                                    
                                    # 히스토리컬 데이터 표시
                                    historical = data.get('historical_data', [])
                                    if historical:
                                        st.write(f"**히스토리컬 데이터 ({len(historical)}일):**")
                                        df_historical = pd.DataFrame(historical)
                                        st.dataframe(df_historical, use_container_width=True)
                        except Exception as e:
                            st.error(f"데이터 조회 오류: {e}")
            else:
                st.info("관심종목 데이터가 없습니다. 사이드바의 동기화 버튼을 사용해주세요.")
        
        except Exception as e:
            st.error(f"관심종목 현황 조회 오류: {e}")
        
        st.markdown("---")
        
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
