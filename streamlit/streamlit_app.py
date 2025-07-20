#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
import sys
import os

# 프로젝트 경로 추가
sys.path.append('/app/common')
from technical_scanner import TechnicalScanner
from database import DuckDBManager

# 페이지 설정
st.set_page_config(
    page_title="📈 Stock Watchlist Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 사이드바
st.sidebar.title("📊 관심종목 대시보드")
st.sidebar.markdown("---")

# 날짜 선택
selected_date = st.sidebar.date_input(
    "조회 날짜 선택",
    value=date.today(),
    max_value=date.today()
)

# 조건 선택
condition_options = {
    'all': '전체',
    'bollinger_upper_touch': '볼린저 밴드 상단 터치'
}
selected_condition = st.sidebar.selectbox(
    "스캔 조건",
    options=list(condition_options.keys()),
    format_func=lambda x: condition_options[x]
)

# 새로고침 버튼
if st.sidebar.button("🔄 데이터 새로고침"):
    st.rerun()

# 메인 페이지
st.title("📈 일별 관심종목 대시보드")
st.markdown(f"**조회 날짜:** {selected_date} | **스캔 조건:** {condition_options[selected_condition]}")

try:
    # 기술적 스캐너 초기화
    scanner = TechnicalScanner(db_path="/data/duckdb/stock_data.db")
    
    # 선택된 날짜의 관심종목 조회
    condition_filter = None if selected_condition == 'all' else selected_condition
    watchlist = scanner.get_daily_watchlist(
        scan_date=selected_date,
        condition_type=condition_filter
    )
    
    # 통계 요약
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("총 관심종목", len(watchlist))
    
    with col2:
        tier_1_count = len([w for w in watchlist if w['market_cap_tier'] == 1])
        st.metric("대형주", tier_1_count)
    
    with col3:
        tier_2_count = len([w for w in watchlist if w['market_cap_tier'] == 2])
        st.metric("중형주", tier_2_count)
    
    with col4:
        tier_3_count = len([w for w in watchlist if w['market_cap_tier'] == 3])
        st.metric("소형주", tier_3_count)
    
    # 관심종목 목록
    if watchlist:
        st.markdown("## 📋 관심종목 목록")
        
        # 데이터프레임 생성
        df = pd.DataFrame(watchlist)
        
        # 시가총액 티어 이름 변환
        tier_names = {1: "대형주", 2: "중형주", 3: "소형주"}
        df['tier_name'] = df['market_cap_tier'].map(tier_names)
        
        # 조건 값 포맷팅
        if 'condition_value' in df.columns:
            df['signal_strength'] = df['condition_value'].apply(lambda x: f"{x:.3f}" if x else "N/A")
        
        # 표시할 컬럼 선택
        display_df = df[[
            'symbol', 'name', 'sector', 'market_cap', 'tier_name', 
            'condition_type', 'signal_strength'
        ]].copy()
        
        display_df.columns = [
            '종목코드', '종목명', '섹터', '시가총액', '티어', 
            '신호 유형', '신호 강도'
        ]
        
        # 필터 옵션
        col1, col2 = st.columns(2)
        
        with col1:
            selected_sectors = st.multiselect(
                "섹터 필터",
                options=df['sector'].unique(),
                default=[]
            )
        
        with col2:
            selected_tiers = st.multiselect(
                "티어 필터",
                options=[1, 2, 3],
                default=[],
                format_func=lambda x: tier_names[x]
            )
        
        # 필터 적용
        filtered_df = display_df.copy()
        if selected_sectors:
            mask = df['sector'].isin(selected_sectors)
            filtered_df = display_df[mask]
        
        if selected_tiers:
            mask = df['market_cap_tier'].isin(selected_tiers)
            if selected_sectors:
                mask = mask & df['sector'].isin(selected_sectors)
            filtered_df = display_df[mask]
        
        # 테이블 표시
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True
        )
        
        # 섹터별 분포 차트
        if len(df) > 0:
            st.markdown("## 📊 섹터별 분포")
            
            sector_counts = df['sector'].value_counts()
            
            fig = px.pie(
                values=sector_counts.values,
                names=sector_counts.index,
                title="섹터별 관심종목 분포"
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info(f"📅 {selected_date}에 해당하는 관심종목이 없습니다.")
    
    # 최근 7일 통계
    st.markdown("## 📈 최근 7일 통계")
    
    summary = scanner.get_watchlist_summary(days=7)
    
    # 일별 관심종목 수 차트
    if summary['daily_counts']:
        daily_df = pd.DataFrame(summary['daily_counts'])
        daily_df['date'] = pd.to_datetime(daily_df['date'])
        
        fig = px.bar(
            daily_df,
            x='date',
            y='count',
            color='condition_type',
            title="일별 관심종목 수",
            labels={'count': '관심종목 수', 'date': '날짜'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # 인기 섹터
    if summary['sector_stats']:
        st.markdown("### 🔥 인기 섹터 (최근 7일)")
        
        sector_df = pd.DataFrame(summary['sector_stats'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                sector_df.head(10),
                x='frequency',
                y='sector',
                orientation='h',
                title="섹터별 등장 횟수",
                labels={'frequency': '등장 횟수', 'sector': '섹터'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(
                sector_df[['sector', 'frequency', 'avg_signal_strength']],
                column_config={
                    'sector': '섹터',
                    'frequency': '등장 횟수',
                    'avg_signal_strength': st.column_config.NumberColumn(
                        '평균 신호 강도',
                        format="%.3f"
                    )
                },
                hide_index=True,
                use_container_width=True
            )
    
    # 상위 종목
    if summary['top_symbols']:
        st.markdown("### ⭐ 상위 종목 (최근 7일)")
        
        top_df = pd.DataFrame(summary['top_symbols'])
        
        st.dataframe(
            top_df[['symbol', 'name', 'appearances', 'avg_signal_strength', 'max_signal_strength']],
            column_config={
                'symbol': '종목코드',
                'name': '종목명',
                'appearances': '등장 횟수',
                'avg_signal_strength': st.column_config.NumberColumn(
                    '평균 신호 강도',
                    format="%.3f"
                ),
                'max_signal_strength': st.column_config.NumberColumn(
                    '최대 신호 강도',
                    format="%.3f"
                )
            },
            hide_index=True,
            use_container_width=True
        )

except Exception as e:
    st.error(f"❌ 데이터 로딩 중 오류 발생: {str(e)}")
    st.info("데이터베이스 연결을 확인해주세요.")

# 푸터
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <small>📈 Stock Watchlist Dashboard | 기술적 지표 기반 관심종목 스캔</small>
    </div>
    """,
    unsafe_allow_html=True
)
