#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
부하테스트 결과 분석 도구
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import json
import argparse
from datetime import datetime
import os

# 한글 폰트 설정 (한국어 그래프용)
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

def analyze_locust_results(csv_pattern="load_test_results_*.csv"):
    """Locust CSV 결과 분석"""
    
    print("📊 Locust 부하테스트 결과 분석")
    print("=" * 50)
    
    # CSV 파일 찾기
    csv_files = glob.glob(csv_pattern)
    if not csv_files:
        print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_pattern}")
        return
    
    latest_csv = max(csv_files, key=os.path.getctime)
    print(f"📁 분석 파일: {latest_csv}")
    
    try:
        # CSV 데이터 로드
        df = pd.read_csv(latest_csv)
        
        print(f"\n📈 기본 통계:")
        print(f"  - 총 요청 수: {df['Request Count'].sum():,}")
        print(f"  - 총 실패 수: {df['Failure Count'].sum():,}")
        print(f"  - 전체 오류율: {(df['Failure Count'].sum() / df['Request Count'].sum() * 100):.2f}%")
        print(f"  - 평균 응답시간: {df['Average Response Time'].mean():.2f}ms")
        print(f"  - 최대 응답시간: {df['Max Response Time'].max():,}ms")
        print(f"  - 평균 처리량: {df['Requests/s'].mean():.2f} req/s")
        
        # 상위 느린 요청들
        print(f"\n🐌 가장 느린 요청 TOP 5:")
        slow_requests = df.nlargest(5, 'Average Response Time')[['Name', 'Average Response Time', 'Request Count']]
        for _, row in slow_requests.iterrows():
            print(f"  - {row['Name']}: {row['Average Response Time']:.2f}ms ({row['Request Count']} 요청)")
        
        # 오류율이 높은 요청들
        df['Error Rate'] = (df['Failure Count'] / df['Request Count'] * 100).fillna(0)
        print(f"\n❌ 오류율이 높은 요청 TOP 5:")
        error_requests = df[df['Error Rate'] > 0].nlargest(5, 'Error Rate')[['Name', 'Error Rate', 'Failure Count']]
        for _, row in error_requests.iterrows():
            print(f"  - {row['Name']}: {row['Error Rate']:.2f}% ({int(row['Failure Count'])} 실패)")
        
        # 그래프 생성
        create_performance_charts(df)
        
    except Exception as e:
        print(f"❌ CSV 분석 오류: {e}")

def create_performance_charts(df):
    """성능 차트 생성"""
    
    print(f"\n📊 성능 차트 생성 중...")
    
    # 차트 스타일 설정
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Stock Kafka Load Test Results', fontsize=16, fontweight='bold')
    
    # 1. 응답시간 분포
    axes[0, 0].bar(range(len(df)), df['Average Response Time'], color='skyblue', alpha=0.7)
    axes[0, 0].set_title('Average Response Time by Request')
    axes[0, 0].set_xlabel('Request Index')
    axes[0, 0].set_ylabel('Response Time (ms)')
    axes[0, 0].grid(True, alpha=0.3)
    
    # 2. 처리량 분포  
    axes[0, 1].bar(range(len(df)), df['Requests/s'], color='lightgreen', alpha=0.7)
    axes[0, 1].set_title('Throughput by Request')
    axes[0, 1].set_xlabel('Request Index')
    axes[0, 1].set_ylabel('Requests/s')
    axes[0, 1].grid(True, alpha=0.3)
    
    # 3. 요청 수 vs 오류율
    df['Error Rate'] = (df['Failure Count'] / df['Request Count'] * 100).fillna(0)
    scatter = axes[1, 0].scatter(df['Request Count'], df['Error Rate'], 
                                c=df['Average Response Time'], cmap='viridis', alpha=0.7)
    axes[1, 0].set_title('Request Count vs Error Rate')
    axes[1, 0].set_xlabel('Request Count')
    axes[1, 0].set_ylabel('Error Rate (%)')
    axes[1, 0].grid(True, alpha=0.3)
    plt.colorbar(scatter, ax=axes[1, 0], label='Avg Response Time (ms)')
    
    # 4. 성능 요약 (텍스트)
    axes[1, 1].axis('off')
    summary_text = f"""
    📊 Performance Summary
    
    Total Requests: {df['Request Count'].sum():,}
    Total Failures: {df['Failure Count'].sum():,}
    
    Average Response Time: {df['Average Response Time'].mean():.2f}ms
    Max Response Time: {df['Max Response Time'].max():,}ms
    
    Average Throughput: {df['Requests/s'].mean():.2f} req/s
    Max Throughput: {df['Requests/s'].max():.2f} req/s
    
    Overall Error Rate: {(df['Failure Count'].sum() / df['Request Count'].sum() * 100):.2f}%
    
    Test Duration: {datetime.now().strftime('%Y-%m-%d %H:%M')}
    """
    
    axes[1, 1].text(0.1, 0.9, summary_text, fontsize=11, verticalalignment='top',
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.5))
    
    plt.tight_layout()
    
    # 차트 저장
    chart_filename = f"performance_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    print(f"💾 차트 저장됨: {chart_filename}")
    
    # 차트 표시 (환경에 따라)
    try:
        plt.show()
    except:
        print("💡 GUI 환경이 아니어서 차트를 화면에 표시할 수 없습니다.")

def analyze_performance_logs(log_file="performance.log"):
    """성능 로그 분석"""
    
    print(f"\n📝 성능 로그 분석: {log_file}")
    
    if not os.path.exists(log_file):
        print(f"❌ 로그 파일을 찾을 수 없습니다: {log_file}")
        return
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"📄 로그 라인 수: {len(lines):,}")
        
        # 주요 이벤트 카운트
        events = {
            'API 요청': 0,
            'Kafka 메시지': 0,
            'DB 작업': 0,
            '경고': 0,
            '오류': 0
        }
        
        response_times = []
        
        for line in lines:
            if '🌐 요청:' in line or 'API 요청' in line:
                events['API 요청'] += 1
                # 응답시간 추출 시도
                if '(' in line and '초)' in line:
                    try:
                        time_str = line.split('(')[1].split('초)')[0]
                        response_times.append(float(time_str))
                    except:
                        pass
            elif '📤 Kafka' in line or 'Kafka 메시지' in line:
                events['Kafka 메시지'] += 1
            elif '🗃️ DB' in line or 'DB 작업' in line:
                events['DB 작업'] += 1
            elif 'WARNING' in line or '⚠️' in line:
                events['경고'] += 1
            elif 'ERROR' in line or '❌' in line:
                events['오류'] += 1
        
        print(f"\n📊 이벤트 통계:")
        for event, count in events.items():
            print(f"  - {event}: {count:,}개")
        
        if response_times:
            print(f"\n⏱️ 응답시간 통계:")
            print(f"  - 평균: {sum(response_times)/len(response_times):.3f}초")
            print(f"  - 최소: {min(response_times):.3f}초")
            print(f"  - 최대: {max(response_times):.3f}초")
            print(f"  - 중앙값: {sorted(response_times)[len(response_times)//2]:.3f}초")
        
        # 오류율 계산
        total_operations = events['API 요청'] + events['Kafka 메시지'] + events['DB 작업']
        if total_operations > 0:
            error_rate = (events['오류'] / total_operations) * 100
            print(f"  - 전체 오류율: {error_rate:.2f}%")
        
    except Exception as e:
        print(f"❌ 로그 분석 오류: {e}")

def generate_load_test_report():
    """종합 부하테스트 보고서 생성"""
    
    print(f"\n📋 종합 부하테스트 보고서 생성")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"load_test_report_{timestamp}.md"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"""# Stock Kafka 부하테스트 보고서

## 📅 테스트 정보
- **실행 일시**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **테스트 환경**: Stock Kafka3 프로젝트
- **분석 도구**: Python, Locust, Prometheus

## 🎯 테스트 목표
1. **API 응답성능** 측정
2. **Kafka 메시지 처리량** 확인  
3. **데이터베이스 동시성** 테스트
4. **시스템 리소스 사용률** 모니터링
5. **장애 대응 메커니즘** 검증

## 📊 주요 성과 지표 (KPI)

### 응답시간 (Response Time)
- 목표: 평균 < 500ms, 95% < 1,000ms
- 결과: [실제 테스트 후 입력]

### 처리량 (Throughput)  
- 목표: > 100 req/s
- 결과: [실제 테스트 후 입력]

### 오류율 (Error Rate)
- 목표: < 1%
- 결과: [실제 테스트 후 입력]

### 시스템 리소스
- CPU 사용률: [실제 테스트 후 입력]
- 메모리 사용률: [실제 테스트 후 입력]
- 디스크 I/O: [실제 테스트 후 입력]

## 🔍 테스트 시나리오

### 1. API 부하테스트
- **도구**: Locust
- **시나리오**: 동시 사용자 증가
- **측정 항목**: 응답시간, 처리량, 오류율

### 2. Kafka 스트레스 테스트
- **도구**: Python 멀티스레딩
- **시나리오**: 대량 메시지 동시 전송
- **측정 항목**: 메시지 처리량, 지연시간, 손실률

### 3. 데이터베이스 부하테스트
- **도구**: DuckDB 동시 연결
- **시나리오**: 복잡한 쿼리 동시 실행
- **측정 항목**: 쿼리 실행시간, 동시 연결 수

## 🛡️ 장애 대응 메커니즘

### 1. 재시도 로직 (Retry)
- **적용 위치**: API 호출, DB 연결, Kafka 전송
- **재시도 횟수**: 3회
- **백오프 전략**: 지수적 증가 (1s → 2s → 4s)

### 2. Circuit Breaker
- **실패 임계값**: 5회 연속 실패
- **타임아웃**: 60초
- **반개방 테스트**: 자동 복구 시도

### 3. 모니터링 및 알람
- **Prometheus 메트릭**: 실시간 수집
- **로깅 시스템**: 구조화된 로그
- **성능 임계값**: CPU 80%, 메모리 85%

## 📈 개선 사항

### 성능 최적화
1. **데이터베이스**
   - DuckDB 배치 처리로 20-300배 성능 향상
   - 인덱스 최적화

2. **비동기 처리**
   - asyncio를 활용한 동시성 확보
   - Kafka 배치 전송

3. **캐싱 전략**
   - Redis 캐시 활용 (계획)
   - 자주 조회되는 데이터 캐싱

### 모니터링 강화
1. **메트릭 대시보드**
   - Grafana 연동 (계획)
   - 실시간 성능 모니터링

2. **알람 시스템**
   - 임계값 초과 시 알림
   - 장애 자동 감지

## 🎯 향후 계획

### 단기 (1-2주)
- [ ] Grafana 대시보드 구축
- [ ] 알람 시스템 구현
- [ ] 부하테스트 자동화

### 중기 (1개월)
- [ ] 성능 벤치마크 기준 수립
- [ ] 지속적 성능 모니터링
- [ ] 장애 시뮬레이션 테스트

### 장기 (3개월)
- [ ] 자동 스케일링 구현
- [ ] 다중 지역 배포 테스트
- [ ] 재해 복구 계획 수립

## 📝 결론

[테스트 완료 후 작성]

---
**생성 일시**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**분석 도구**: Stock Kafka Load Test Analyzer v1.0
""")
    
    print(f"📄 보고서 생성됨: {report_file}")
    return report_file

def main():
    """메인 실행 함수"""
    
    parser = argparse.ArgumentParser(description='부하테스트 결과 분석 도구')
    parser.add_argument('--csv-pattern', default='load_test_results_*.csv', 
                       help='Locust CSV 파일 패턴')
    parser.add_argument('--log-file', default='performance.log',
                       help='성능 로그 파일 경로')
    parser.add_argument('--generate-report', action='store_true',
                       help='종합 보고서 생성')
    
    args = parser.parse_args()
    
    print("📊 Stock Kafka 부하테스트 결과 분석기")
    print("=" * 60)
    
    # 1. Locust 결과 분석
    analyze_locust_results(args.csv_pattern)
    
    # 2. 성능 로그 분석  
    analyze_performance_logs(args.log_file)
    
    # 3. 종합 보고서 생성
    if args.generate_report:
        generate_load_test_report()
    
    print(f"\n🎉 분석 완료!")
    print(f"📅 분석 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
