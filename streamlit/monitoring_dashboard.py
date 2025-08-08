#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
주식 데이터 수집 모니터링 대시보드 - Streamlit 기반
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import json
import requests
from datetime import datetime, timedelta
import os
import sys

# 페이지 설정
st.set_page_config(
    page_title="주식 데이터 수집 모니터링",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 사이드바 설정
st.sidebar.title("📊 모니터링 대시보드")
st.sidebar.markdown("---")

# 모니터링 모드 선택
monitoring_mode = st.sidebar.selectbox(
    "모니터링 모드",
    ["실시간 모니터링", "성능 분석", "시스템 상태", "로그 분석"]
)

# 새로고침 간격
refresh_interval = st.sidebar.selectbox(
    "새로고침 간격",
    [5, 10, 30, 60],
    index=1
)

# 자동 새로고침 설정
auto_refresh = st.sidebar.checkbox("자동 새로고침", value=True)

class MonitoringDashboard:
    """모니터링 대시보드 클래스"""
    
    def __init__(self, data_path="/home/grey1/stock-kafka3/data"):
        self.data_path = data_path
        self.log_path = "/home/grey1/stock-kafka3/logs"
    
    def get_system_status(self):
        """시스템 상태 확인"""
        try:
            import psutil
            
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 메모리 사용률
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used_gb = memory.used / (1024**3)
            memory_total_gb = memory.total / (1024**3)
            
            # 디스크 사용률
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            disk_used_gb = disk.used / (1024**3)
            disk_total_gb = disk.total / (1024**3)
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_used_gb': memory_used_gb,
                'memory_total_gb': memory_total_gb,
                'disk_percent': disk_percent,
                'disk_used_gb': disk_used_gb,
                'disk_total_gb': disk_total_gb,
                'timestamp': datetime.now()
            }
        except Exception as e:
            st.error(f"시스템 상태 조회 실패: {e}")
            return {}
    
    def get_data_files_info(self):
        """PostgreSQL 테이블 정보 조회"""
        try:
            from common.database import PostgreSQLManager
            tables_info = []
            
            db_manager = PostgreSQLManager()
            
            # PostgreSQL에서 테이블 정보 조회
            query = """
            SELECT 
                table_name,
                pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size,
                pg_total_relation_size(quote_ident(table_name)) as size_bytes,
                (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as columns_count
            FROM information_schema.tables t
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC
            """
            
            result = db_manager.execute_query(query)
            
            for _, row in result.iterrows():
                # 각 테이블의 레코드 수 조회
                count_query = f"SELECT COUNT(*) FROM {row['table_name']}"
                count_result = db_manager.execute_query(count_query)
                record_count = count_result.iloc[0, 0] if not count_result.empty else 0
                
                tables_info.append({
                    'filename': row['table_name'],
                    'path': f"PostgreSQL.{row['table_name']}",
                    'size_mb': row['size_bytes'] / (1024*1024),
                    'modified': datetime.now(),  # PostgreSQL은 테이블 수정 시간이 따로 없음
                    'type': 'TABLE',
                    'records': record_count,
                    'columns': row['columns_count']
                })
            
            db_manager.close()
            return sorted(tables_info, key=lambda x: x['size_mb'], reverse=True)
        
        except Exception as e:
            st.error(f"PostgreSQL 테이블 정보 조회 실패: {e}")
            return []
    
    def get_log_files_info(self):
        """로그 파일 정보 조회"""
        try:
            log_info = []
            
            if os.path.exists(self.log_path):
                for root, dirs, files in os.walk(self.log_path):
                    for file in files:
                        if file.endswith('.log'):
                            file_path = os.path.join(root, file)
                            stat = os.stat(file_path)
                            
                            log_info.append({
                                'filename': file,
                                'path': file_path,
                                'size_mb': stat.st_size / (1024*1024),
                                'modified': datetime.fromtimestamp(stat.st_mtime)
                            })
            
            return sorted(log_info, key=lambda x: x['modified'], reverse=True)
        
        except Exception as e:
            st.warning(f"로그 파일 정보 조회 실패: {e}")
            return []
    
    def read_recent_logs(self, log_file, lines=100):
        """최근 로그 읽기"""
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f:
                    all_lines = f.readlines()
                    return all_lines[-lines:] if len(all_lines) > lines else all_lines
            return []
        except Exception as e:
            st.error(f"로그 읽기 실패: {e}")
            return []
    
    def analyze_table_data(self, table_name):
        """PostgreSQL 테이블 데이터 분석"""
        try:
            from common.database import PostgreSQLManager
            db_manager = PostgreSQLManager()
            
            # 테이블 기본 정보 조회
            info_query = f"""
            SELECT 
                COUNT(*) as row_count,
                (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{table_name}') as column_count
            FROM {table_name}
            """
            info_result = db_manager.execute_query(info_query)
            
            # 컬럼 정보 조회
            columns_query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            columns_result = db_manager.execute_query(columns_query)
            
            # 샘플 데이터 조회
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            sample_result = db_manager.execute_query(sample_query)
            
            analysis = {
                'rows': info_result.iloc[0]['row_count'] if not info_result.empty else 0,
                'columns': info_result.iloc[0]['column_count'] if not info_result.empty else 0,
                'column_info': columns_result.to_dict('records') if not columns_result.empty else [],
                'sample_data': sample_result.to_dict('records') if not sample_result.empty else []
            }
            
            # 날짜 컬럼이 있으면 날짜 범위 분석
            date_columns = [col['column_name'] for col in analysis['column_info'] 
                          if 'date' in col['column_name'].lower() or 'time' in col['column_name'].lower()]
            
            if date_columns:
                for col in date_columns:
                    try:
                        date_range_query = f"SELECT MIN({col}) as min_date, MAX({col}) as max_date FROM {table_name}"
                        date_result = db_manager.execute_query(date_range_query)
                        if not date_result.empty:
                            analysis[f'{col}_range'] = {
                                'start': date_result.iloc[0]['min_date'],
                                'end': date_result.iloc[0]['max_date']
                            }
                    except:
                        pass
            
            db_manager.close()
            return analysis
        
        except Exception as e:
            st.error(f"테이블 분석 실패: {e}")
            return {}

def render_realtime_monitoring(dashboard):
    """실시간 모니터링 렌더링"""
    st.title("🔴 실시간 모니터링")
    
    # 시스템 상태 가져오기
    system_status = dashboard.get_system_status()
    
    if system_status:
        # 메트릭 컨테이너
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            cpu_color = "🟢" if system_status['cpu_percent'] < 70 else "🟡" if system_status['cpu_percent'] < 90 else "🔴"
            st.metric(
                "CPU 사용률",
                f"{system_status['cpu_percent']:.1f}%",
                delta=None
            )
            st.write(f"{cpu_color} 상태")
        
        with col2:
            memory_color = "🟢" if system_status['memory_percent'] < 70 else "🟡" if system_status['memory_percent'] < 90 else "🔴"
            st.metric(
                "메모리 사용률",
                f"{system_status['memory_percent']:.1f}%",
                delta=f"{system_status['memory_used_gb']:.1f}GB / {system_status['memory_total_gb']:.1f}GB"
            )
            st.write(f"{memory_color} 상태")
        
        with col3:
            disk_color = "🟢" if system_status['disk_percent'] < 80 else "🟡" if system_status['disk_percent'] < 90 else "🔴"
            st.metric(
                "디스크 사용률",
                f"{system_status['disk_percent']:.1f}%",
                delta=f"{system_status['disk_used_gb']:.1f}GB / {system_status['disk_total_gb']:.1f}GB"
            )
            st.write(f"{disk_color} 상태")
        
        with col4:
            st.metric(
                "마지막 업데이트",
                system_status['timestamp'].strftime("%H:%M:%S"),
                delta=None
            )
    
    # PostgreSQL 테이블 현황
    st.subheader("� PostgreSQL 테이블 현황")
    
    files_info = dashboard.get_data_files_info()
    if files_info:
        df_files = pd.DataFrame(files_info)
        
        # 테이블 타입별 통계
        col1, col2 = st.columns(2)
        
        with col1:
            # 테이블별 레코드 수
            fig = px.bar(
                df_files.head(10),
                x='filename',
                y='records',
                title='테이블별 레코드 수',
                color='size_mb'
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # 테이블 크기별 분포
            fig = px.pie(
                df_files,
                values='size_mb',
                names='filename',
                title='테이블 크기 분포 (MB)'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # PostgreSQL 테이블 상세 정보
        st.write("**PostgreSQL 테이블 상세 정보:**")
        display_df = df_files[['filename', 'size_mb', 'records', 'columns']].copy()
        display_df.columns = ['테이블명', '크기(MB)', '레코드수', '컬럼수']
        display_df['크기(MB)'] = display_df['크기(MB)'].round(2)
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("PostgreSQL 테이블 정보를 가져올 수 없습니다.")

def render_performance_analysis(dashboard):
    """성능 분석 렌더링"""
    st.title("⚡ 성능 분석")
    
    # PostgreSQL 테이블 선택
    tables_info = dashboard.get_data_files_info()
    
    if tables_info:
        selected_table = st.selectbox(
            "분석할 PostgreSQL 테이블 선택:",
            options=[t['filename'] for t in tables_info],
        )
        
        if selected_table:
            st.subheader(f"📊 테이블 분석: {selected_table}")
            
            # 테이블 분석 실행
            analysis = dashboard.analyze_table_data(selected_table)
            
            if analysis:
                # 기본 정보
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("총 행 수", f"{analysis['rows']:,}")
                
                with col2:
                    st.metric("총 열 수", analysis['columns'])
                
                with col3:
                    null_total = sum(analysis['null_counts'].values())
                    st.metric("결측값 수", f"{null_total:,}")
                
                # 컬럼 정보
                st.subheader("📋 컬럼 정보")
                
                col_info = []
                for col in analysis['column_names']:
                    col_info.append({
                        '컬럼명': col,
                        '데이터타입': str(analysis['data_types'][col]),
                        '결측값': analysis['null_counts'][col]
                    })
                
                st.dataframe(pd.DataFrame(col_info), use_container_width=True, hide_index=True)
                
                # 샘플 데이터
                if analysis['sample_data']:
                    st.subheader("📄 샘플 데이터 (상위 5행)")
                    st.dataframe(pd.DataFrame(analysis['sample_data']), use_container_width=True, hide_index=True)
                
                # 날짜 범위 정보
                date_ranges = {k: v for k, v in analysis.items() if k.endswith('_range')}
                if date_ranges:
                    st.subheader("📅 날짜 범위 정보")
                    for col_range, range_info in date_ranges.items():
                        col_name = col_range.replace('_range', '')
                        st.write(f"**{col_name}**: {range_info['start']} ~ {range_info['end']}")
    else:
        st.info("분석할 CSV 파일이 없습니다.")

def render_system_status(dashboard):
    """시스템 상태 렌더링"""
    st.title("🖥️ 시스템 상태")
    
    # 시스템 정보
    system_status = dashboard.get_system_status()
    
    if system_status:
        # 리소스 사용률
        st.subheader("📊 시스템 리소스")
        
        # CPU, 메모리, 디스크 사용률 차트
        categories = ['CPU', 'Memory', 'Disk']
        values = [
            system_status['cpu_percent'],
            system_status['memory_percent'],
            system_status['disk_percent']
        ]
        colors = ['red' if v > 90 else 'orange' if v > 70 else 'green' for v in values]
        
        fig = go.Figure(data=go.Bar(
            x=categories,
            y=values,
            marker_color=colors,
            text=[f"{v:.1f}%" for v in values],
            textposition='auto'
        ))
        
        fig.update_layout(
            title="시스템 리소스 사용률",
            yaxis_title="사용률 (%)",
            yaxis=dict(range=[0, 100])
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # 디렉토리 구조
    st.subheader("📁 프로젝트 구조")
    
    project_structure = {
        "data/": "데이터 파일 저장소",
        "logs/": "로그 파일 저장소", 
        "dags/": "Airflow DAG 파일들",
        "streamlit/": "Streamlit 앱 파일들",
        "common/": "공통 유틸리티 모듈들",
        "config/": "설정 파일들"
    }
    
    for folder, description in project_structure.items():
        folder_path = f"/home/grey1/stock-kafka3/{folder}"
        exists = "🟢" if os.path.exists(folder_path) else "🔴"
        st.write(f"{exists} **{folder}** - {description}")

def render_log_analysis(dashboard):
    """로그 분석 렌더링"""
    st.title("📋 로그 분석")
    
    # 로그 파일 목록
    log_files = dashboard.get_log_files_info()
    
    if log_files:
        # 로그 파일 선택
        selected_log = st.selectbox(
            "로그 파일 선택:",
            options=[f['path'] for f in log_files],
            format_func=lambda x: os.path.basename(x)
        )
        
        if selected_log:
            # 읽을 라인 수 선택
            num_lines = st.slider("읽을 라인 수", 10, 1000, 100)
            
            # 로그 내용 읽기
            log_lines = dashboard.read_recent_logs(selected_log, num_lines)
            
            if log_lines:
                st.subheader(f"📄 로그 내용: {os.path.basename(selected_log)}")
                
                # 로그 레벨별 필터링
                log_level_filter = st.selectbox(
                    "로그 레벨 필터",
                    ["전체", "ERROR", "WARNING", "INFO", "DEBUG"]
                )
                
                filtered_lines = log_lines
                if log_level_filter != "전체":
                    filtered_lines = [line for line in log_lines if log_level_filter in line]
                
                # 로그 내용 표시
                st.text_area(
                    "로그 내용",
                    value=''.join(filtered_lines),
                    height=400
                )
                
                # 로그 통계
                st.subheader("📊 로그 통계")
                
                error_count = sum(1 for line in log_lines if 'ERROR' in line)
                warning_count = sum(1 for line in log_lines if 'WARNING' in line)
                info_count = sum(1 for line in log_lines if 'INFO' in line)
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("ERROR", error_count)
                
                with col2:
                    st.metric("WARNING", warning_count)
                
                with col3:
                    st.metric("INFO", info_count)
            else:
                st.info("로그 내용이 없습니다.")
    else:
        st.info("로그 파일이 없습니다.")

# 메인 앱
def main():
    # 대시보드 인스턴스 생성
    dashboard = MonitoringDashboard()
    
    # 모니터링 모드에 따른 렌더링
    if monitoring_mode == "실시간 모니터링":
        render_realtime_monitoring(dashboard)
    elif monitoring_mode == "성능 분석":
        render_performance_analysis(dashboard)
    elif monitoring_mode == "시스템 상태":
        render_system_status(dashboard)
    elif monitoring_mode == "로그 분석":
        render_log_analysis(dashboard)
    
    # 자동 새로고침
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()
