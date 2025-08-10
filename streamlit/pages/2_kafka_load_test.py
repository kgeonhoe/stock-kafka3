#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 부하테스트 실시간 대시보드 (Streamlit 페이지)

기능:
 - 테스트 파라미터 입력 (duration, workers 등)
 - 테스트 시작/중단
 - 실시간 TPS / 누적 카운터 / 라인차트
 - 완료 후 Summary 출력

주의:
 - scripts.kafka_load_tester.KafkaLoadTester 에 의존
 - 다중 실행 시 이전 테스트 객체를 폐기하고 새로 생성
"""

import streamlit as st
import time
import threading
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional
import sys
import os

# ---- Import fallback 처리 ----
_import_error = None
try:
    from scripts.kafka_load_tester import LoadTestConfig, KafkaLoadTester, SLOCriteria
except Exception as e:
    _import_error = e
    # 상위 루트 추가 후 재시도
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if ROOT_DIR not in sys.path:
        sys.path.insert(0, ROOT_DIR)
    try:
        from scripts.kafka_load_tester import LoadTestConfig, KafkaLoadTester, SLOCriteria
        _import_error = None
    except Exception as e2:  # pragma: no cover
        _import_error = e2

if _import_error:
    st.error(f"kafka_load_tester import 실패: {_import_error}")
    st.stop()

st.set_page_config(page_title="Kafka Load Test", page_icon="📈", layout="wide")
st.title("📈 Kafka 부하테스트")

# ---------------- 가이드 / 추천 설정 표시 ----------------
with st.expander("📘 부하테스트 가이드 / 추천값", expanded=False):
    st.markdown(
        """
### 1. 목적
Kafka 인프라가 (지연·에러·자원사용) SLO를 만족하면서 처리할 수 있는 최대 안정 처리량(capacity)을 찾고 병목 지점을 식별하기 위함입니다.

### 2. 핵심 지표 (테스트 중 추적 권장)
- Throughput: msgs/sec, MB/sec (Producer/Consumer 모두)
- End-to-End Latency: p50 / p95 / p99 (추가 구현 예정)
- Producer Ack Latency / Error Rate
- Consumer Lag (파트션별 누적 지연)
- Error 유형: timeout, NOT_ENOUGH_REPLICAS, 재시도, rebalance 빈도
- 자원 사용: Broker CPU / 메모리 / 네트워크 / 디스크 flush time
- 파티션 스큐: (최대 파티션 처리량) / (평균 처리량)

### 3. 권장 절차 (Ramp-Up)
1) 메시지 크기, 형식 고정 (예: 300~800B JSON)
2) 목표치의 20% → 40% → 60% → 80% → 100% 순차 상승 (단계당 3~5분 유지)
3) 임계 징후 (Latency 급등, Lag 누적, Error Rate 상승) 관찰 시점 기록
4) 병목 유형별 튜닝 (배치/linger/압축/파티션/스레드) 후 재실행 비교
5) 안정 구간과 붕괴(Degradation) 구간 경계 명확화

### 4. 프로듀서 추천 기본값 (소형 단일 브로커, 메시지 <1KB 기준)
| 항목 | 값 | 비고 |
|------|----|------|
| linger.ms | 5~10 (지연 덜 중요하면 15~20) | 배치 결집 ↑ Throughput ↑ |
| batch.size | 65536 (64KB) → 필요시 131072 | 과도하면 메모리 ↑ |
| compression.type | lz4 (균형) / zstd (CPU 허용) | gzip 은 CPU 비용↑ |
| acks | all | 데이터 내구성 확보 |
| enable.idempotence | true | 중복 방지 |
| max.in.flight.requests.per.connection | 5 | 지연 민감 시 1 |
| buffer.memory | 기본 32MB → 64MB | 워커 많을 때 확장 |

### 5. 토픽 / 컨슈머 설정 가이드
- 파티션: 초기 6~12 (확장 여지 고려). 처리량 목표 / (단일 파티션 처리 한계)로 역산
- auto.offset.reset: 최신 run 정밀 측정은 latest 권장 (백로그 영향 제거)
- Consumer fetch.min.bytes: 8KB~32KB (Throughput 우선 시 ↑, 지연 우선 시 ↓)
- fetch.max.wait.ms: 20~50ms

### 6. 단계별 부하 예시 (목표 10K msg/s)
| 단계 | 목표 TPS | 비고 |
|------|----------|------|
| 1 | 2K | 안정 베이스라인 |
| 2 | 4K | 지연/에러 추세 확인 |
| 3 | 6K | Lag 발생 여부 |
| 4 | 8K | 자원 여유 한계 접근 |
| 5 | 10K | 목표 SLO 유지 여부 |
| 6 | 11~12K | 붕괴 구간 탐지 |

### 7. 튜닝 우선순위 로직
1) p95 Latency 증가 + CPU 여유 → linger / batch 확장, 더 강한 압축
2) CPU 포화 + Latency 증가 → 압축 완화(lz4) / 프로듀서 워커·파티션 재조정
3) 네트워크 포화 → 압축 강도 ↑ 또는 메시지 묶기 (aggregation)
4) Lag 증가 + Broker 여유 → consumer fetch.min.bytes ↑, polling 효율화
5) Rebalance 잦음 → consumer 수/세션 설정 재조정

### 8. 현재 페이지 개선 예정(선택 구현)
- run_id 기반 필터 (백로그/과거 메시지 제외)
- End-to-End Latency 측정 (producer timestamp 삽입 → consumer 수신 시 차이)
- Latency p50/p95/p99 그래프
- 결과 JSON/CSV 다운로드 / 시나리오 자동 실행 매트릭스
- earliest/latest 선택 UI

### 9. 빠른 Baseline (단일 브로커 5K msg/s 목표)
- Topic: partitions=8, replication.factor=1
- Producer: linger.ms=8, batch.size=65536, compression=lz4, acks=all, idempotence=true
- Consumer: auto.offset.reset=latest, fetch.min.bytes=8KB, fetch.max.wait.ms=40
- Ramp: 1K → 3K → 5K → 6K (붕괴 징후 기록)

위 설정은 시작점일 뿐이며, 목표 SLO(예: p95 < 300ms, 에러율 <0.1%, Broker CPU <70%)을 만족하는 최대 TPS 지점이 "현재 최적"입니다.
        """
    )

# ---------------- 세션 상태 초기화 ----------------

def _init_state():
    defaults = {
        'tester': None,
        'test_thread': None,
        'load_test_running': False,
        'load_test_summary': None,
        'last_metrics': None,
        'last_refresh': 0.0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

# ---------------- 사이드바 설정 폼 ----------------
with st.sidebar:
    st.subheader("⚙️ 테스트 설정")
    duration_minutes = st.number_input("Duration (분)", 0.1, 120.0, 1.0, 0.5, help="총 테스트 시간")
    kafka_producers = st.number_input("Kafka Producers", 1, 200, 5)
    kafka_consumers = st.number_input("Kafka Consumers", 0, 200, 2)
    api_workers = st.number_input("API Workers", 0, 100, 2)
    messages_per_worker = st.number_input("Messages per Producer Worker", 10, 1_000_000, 500, step=50)
    topics_raw = st.text_input("Topics (공백 구분)", "realtime-stock yfinance-stock-data")
    snapshot_interval = st.slider("스냅샷 주기(초)", 0.2, 5.0, 1.0, 0.2)
    auto_refresh_sec = st.slider("화면 갱신 주기(초)", 0.5, 10.0, 2.0, 0.5)

    with st.expander("SLO 기준 설정", expanded=False):
        col_slo1, col_slo2 = st.columns(2)
        with col_slo1:
            slo_min_success = st.number_input("성공률 ≥ (%)", 90.0, 100.0, 99.5, 0.1, help="최소 성공률")
            slo_max_cpu = st.number_input("평균 CPU ≤ (%)", 10.0, 100.0, 70.0, 1.0)
            slo_min_thr = st.number_input("처리량 ≥ (msg/s)", 0.0, 1_000_000.0, 0.0, 100.0)
        with col_slo2:
            slo_max_errors = st.number_input("총 오류 ≤", 0, 1_000_000, 0, 1)
            slo_gap_pct = st.number_input("편차 ≤ (%)", 0.0, 100.0, 5.0, 0.5, help="Consumer/Producer 누적 편차")
        use_slo = st.checkbox("SLO 평가 활성화", value=True)

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        start_clicked = st.button("🚀 테스트 시작", type="primary", disabled=st.session_state.load_test_running)
    with col_btn2:
        stop_clicked = st.button("🛑 중단", disabled=not st.session_state.load_test_running)

# ---------------- 테스트 시작 로직 ----------------

def _start_test():
    # 기존 스레드 정리 (가능하면 soft stop)
    if st.session_state.tester and st.session_state.load_test_running:
        try:
            st.session_state.tester.is_running = False
        except Exception:
            pass
    topics = [t.strip() for t in topics_raw.split() if t.strip()]
    config = LoadTestConfig(
        duration_minutes=float(duration_minutes),
        kafka_producer_workers=int(kafka_producers),
        kafka_consumer_workers=int(kafka_consumers),
        api_call_workers=int(api_workers),
        messages_per_worker=int(messages_per_worker),
        topics=topics,
    )
    slo_obj = None
    if 'SLOCriteria' in globals() and use_slo:
        try:
            slo_obj = SLOCriteria(
                min_success_rate=float(slo_min_success),
                max_error_count=int(slo_max_errors),
                max_avg_cpu_usage=float(slo_max_cpu),
                max_consumer_producer_gap_pct=float(slo_gap_pct),
                min_overall_throughput=float(slo_min_thr)
            )
        except Exception:
            slo_obj = None
    tester = KafkaLoadTester(config, slo_obj)
    tester.snapshot_interval_sec = float(snapshot_interval)
    st.session_state.tester = tester
    st.session_state.load_test_summary = None
    st.session_state.load_test_running = True
    st.session_state.last_metrics = None

    def _run():
        try:
            summary = tester.run_load_test()
            st.session_state.load_test_summary = summary
        finally:
            st.session_state.load_test_running = False

    th = threading.Thread(target=_run, daemon=True, name="load_test_runner")
    # Streamlit 백그라운드 스레드 컨텍스트 등록 (missing ScriptRunContext 경고 방지)
    try:  # Streamlit 버전별 안전 처리
        from streamlit.runtime.scriptrunner import add_script_run_ctx
        add_script_run_ctx(th)
    except Exception:
        pass
    st.session_state.test_thread = th
    th.start()


def _stop_test():
    if st.session_state.tester:
        st.session_state.tester.is_running = False


if start_clicked:
    _start_test()
if stop_clicked:
    _stop_test()

# ---------------- 실시간 메트릭 표시 ----------------
placeholder_charts = st.empty()


def _render_live():
    tester: Optional[KafkaLoadTester] = st.session_state.tester
    if not tester:
        st.info("사이드바에서 설정 후 '테스트 시작'을 눌러주세요.")
        return
    metrics = tester.get_live_metrics()
    st.session_state.last_metrics = metrics

    # 상단 메트릭
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Producer 누적", f"{metrics['producer_sent_total']:,}", f"TPS {metrics['producer_tps']:.1f}")
    col2.metric("Consumer 누적", f"{metrics['consumer_received_total']:,}", f"TPS {metrics['consumer_tps']:.1f}")
    col3.metric("API 성공", f"{metrics['api_success_total']:,}", f"TPS {metrics['api_tps']:.1f}")
    elapsed = metrics['snapshots'][-1]['elapsed_sec'] if metrics['snapshots'] else 0
    col4.metric("경과(초)", f"{elapsed:.1f}")
    # run_id 표시
    if hasattr(tester, 'run_id') and tester.run_id:
        st.caption(f"run_id: {tester.run_id}")

    # 차트 데이터 준비
    snaps = metrics['snapshots']
    if snaps:
        df = pd.DataFrame(snaps)
        # 누적 카운터 그래프 (go.Figure 직접 구성 - RecursionError 회피)
        cum_fig = go.Figure()
        cum_fig.add_trace(go.Scatter(x=df['elapsed_sec'], y=df['producer_sent'], name='producer_sent', mode='lines'))
        cum_fig.add_trace(go.Scatter(x=df['elapsed_sec'], y=df['consumer_received'], name='consumer_received', mode='lines'))
        cum_fig.add_trace(go.Scatter(x=df['elapsed_sec'], y=df['api_success'], name='api_success', mode='lines'))
        cum_fig.update_layout(title='누적 카운터', xaxis_title='경과(sec)', yaxis_title='누적')
        placeholder_charts.plotly_chart(cum_fig, use_container_width=True)

        # TPS 계산
        if len(df) > 1:
            elapsed_diff = df['elapsed_sec'].diff()
            # 0 또는 음수 방지
            elapsed_diff[elapsed_diff <= 0] = float('nan')
            tps_df = pd.DataFrame({
                'elapsed_sec': df['elapsed_sec'],
                'producer_tps': df['producer_sent'].diff() / elapsed_diff,
                'consumer_tps': df['consumer_received'].diff() / elapsed_diff,
                'api_tps': df['api_success'].diff() / elapsed_diff,
            }).dropna()
            if not tps_df.empty:
                tps_fig = go.Figure()
                tps_fig.add_trace(go.Scatter(x=tps_df['elapsed_sec'], y=tps_df['producer_tps'], name='producer_tps', mode='lines'))
                tps_fig.add_trace(go.Scatter(x=tps_df['elapsed_sec'], y=tps_df['consumer_tps'], name='consumer_tps', mode='lines'))
                tps_fig.add_trace(go.Scatter(x=tps_df['elapsed_sec'], y=tps_df['api_tps'], name='api_tps', mode='lines'))
                tps_fig.update_layout(title='초당 처리량 (TPS)', xaxis_title='경과(sec)', yaxis_title='TPS')
                st.plotly_chart(tps_fig, use_container_width=True)
            else:
                st.info("TPS 계산 가능한 데이터 없음")
        else:
            st.info("TPS 계산을 위한 데이터가 충분하지 않습니다.")
    else:
        st.info("스냅샷 수집 대기 중...")

    # 최근 에러 로그 표시
    if metrics.get('recent_errors'):
        with st.expander("최근 에러 (최근 50개)"):
            for e in metrics['recent_errors'][-50:]:
                st.code(e)


def _render_summary():
    summary = st.session_state.load_test_summary
    if not summary:
        return
    st.success("✅ 테스트 완료")
    basic_cols = st.columns(5)
    basic_cols[0].metric("총 메시지", f"{summary['total_messages']:,}")
    basic_cols[1].metric("성공", f"{summary['total_success']:,}")
    basic_cols[2].metric("실패", f"{summary['total_errors']:,}")
    basic_cols[3].metric("성공률", f"{summary['success_rate']:.1f}%")
    basic_cols[4].metric("전체 처리량", f"{summary['overall_throughput']:.2f} msg/s")
    if 'run_id' in summary:
        st.caption(f"run_id: {summary['run_id']}")
    if 'latency_stats' in summary and summary['latency_stats']['count']:
        ls = summary['latency_stats']
        st.write(f"Latency ms: avg {ls['avg_ms']:.2f} | p50 {ls['p50_ms']:.2f} | p95 {ls['p95_ms']:.2f} | p99 {ls['p99_ms']:.2f} | max {ls['max_ms']:.2f}")
    if 'skipped_messages' in summary:
        st.write(f"(run_id 불일치로 무시된 consumer 메시지: {summary['skipped_messages']:,})")

    with st.expander("Producer/Consumer/API 요약", expanded=True):
        colp, colc, cola = st.columns(3)
        p = summary['producer_stats']; c = summary['consumer_stats']; a = summary['api_stats']
        colp.write(f"**Producer**\n\n워커: {p['worker_count']}\n\n총 전송: {p['total_sent']:,}\n\n평균 TPS: {p['avg_throughput']:.2f}")
        colc.write(f"**Consumer**\n\n워커: {c['worker_count']}\n\n총 수신: {c['total_consumed']:,}\n\n평균 TPS: {c['avg_throughput']:.2f}")
        cola.write(f"**API**\n\n워커: {a['worker_count']}\n\n총 성공: {a['total_calls']:,}\n\n평균 TPS: {a['avg_throughput']:.2f}")

    if summary.get('slo'):
        slo = summary['slo']
        st.subheader("SLO 평가")
        pass_badge = "✅ PASS" if slo['passed'] else "❌ FAIL"
        st.write(f"종합 결과: {pass_badge}")
        # 체크 테이블
        check_rows = []
        for chk in slo['checks']:
            check_rows.append({
                '항목': chk['name'],
                '결과': 'PASS' if chk['passed'] else 'FAIL',
                'Actual': chk['actual'],
                'Expected': chk['expected']
            })
        st.dataframe(pd.DataFrame(check_rows), use_container_width=True, hide_index=True)

    worker_df = pd.DataFrame(summary['worker_results'])
    st.dataframe(worker_df, use_container_width=True, hide_index=True)

    with st.expander("원시 Summary JSON"):
        st.json(summary, expanded=False)


# ---------------- 렌더링 흐름 ----------------
if st.session_state.load_test_running:
    _render_live()
    time.sleep(auto_refresh_sec)
    st.rerun()
else:
    if st.session_state.load_test_summary:
        _render_summary()
    else:
        st.info("테스트 대기 상태입니다. 좌측에서 설정 후 시작하세요.")
