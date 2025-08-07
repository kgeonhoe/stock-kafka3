#!/bin/bash
# -*- coding: utf-8 -*-

"""
나스닥 최적화 데이터 수집 시스템 실행 스크립트
- 대량 데이터 수집 (5년치)
- HDD 환경 최적화
- 병목 해결
"""

set -e  # 에러 시 즉시 종료

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 로그 함수
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 기본 설정
PROJECT_ROOT="/home/grey1/stock-kafka3"
VENV_PATH="${PROJECT_ROOT}/venv"
DATA_DIR="/data"
DUCKDB_DIR="${DATA_DIR}/duckdb"
PARQUET_DIR="${DATA_DIR}/parquet_storage"

# 함수: 시스템 요구사항 확인
check_system_requirements() {
    log_step "시스템 요구사항 확인 중..."
    
    # 메모리 확인
    available_memory=$(free -g | awk '/^Mem:/{print $7}')
    if [ "$available_memory" -lt 2 ]; then
        log_error "메모리 부족: ${available_memory}GB (최소 2GB 필요)"
        exit 1
    fi
    
    # 디스크 공간 확인 
    available_disk=$(df -BG "$DATA_DIR" 2>/dev/null | awk 'NR==2{print $4}' | sed 's/G//' || echo "50")
    if [ "$available_disk" -lt 50 ]; then
        log_error "디스크 공간 부족: ${available_disk}GB (최소 50GB 필요)"
        exit 1
    fi
    
    log_info "시스템 요구사항 충족 ✓"
    log_info "사용 가능 메모리: ${available_memory}GB"
    log_info "사용 가능 디스크: ${available_disk}GB"
}

# 함수: 디렉토리 구조 생성
setup_directories() {
    log_step "디렉토리 구조 생성 중..."
    
    # 필요한 디렉토리 생성
    mkdir -p "$DUCKDB_DIR"
    mkdir -p "$PARQUET_DIR"/{staging,archive,temp}
    mkdir -p "$DATA_DIR"/{logs,temp}
    
    # 권한 설정
    chmod 755 "$DATA_DIR"
    chmod 755 "$DUCKDB_DIR"
    chmod 755 "$PARQUET_DIR"
    
    log_info "디렉토리 구조 생성 완료 ✓"
}

# 함수: Python 환경 설정
setup_python_environment() {
    log_step "Python 환경 설정 중..."
    
    cd "$PROJECT_ROOT"
    
    # 가상환경 생성 (없는 경우)
    if [ ! -d "$VENV_PATH" ]; then
        log_info "Python 가상환경 생성 중..."
        python3 -m venv "$VENV_PATH"
    fi
    
    # 가상환경 활성화
    source "$VENV_PATH/bin/activate"
    
    # pip 업그레이드
    pip install --upgrade pip
    
    # 필수 패키지 설치
    log_info "필수 패키지 설치 중..."
    pip install -r requirements.txt || {
        log_warn "requirements.txt가 없습니다. 필수 패키지를 직접 설치합니다."
        pip install \
            FinanceDataReader \
            duckdb \
            pandas \
            pyarrow \
            psutil \
            apache-airflow \
            redis
    }
    
    log_info "Python 환경 설정 완료 ✓"
}

# 함수: DuckDB 데이터베이스 초기화
initialize_database() {
    log_step "DuckDB 데이터베이스 초기화 중..."
    
    cd "$PROJECT_ROOT"
    source "$VENV_PATH/bin/activate"
    
    # Python 스크립트로 데이터베이스 초기화
    python3 -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT/common')

from parquet_loader import ParquetDataLoader
from batch_scheduler import DuckDBBatchScheduler, CheckpointConfig
import os

# Parquet 로더로 Append-only 구조 생성
loader = ParquetDataLoader('$PARQUET_DIR')
success = loader.create_append_only_structure('$DUCKDB_DIR/stock_data.db', 'stock_data')

if success:
    print('✅ DuckDB 데이터베이스 초기화 완료')
else:
    print('❌ DuckDB 데이터베이스 초기화 실패')
    exit(1)
"
    
    log_info "DuckDB 데이터베이스 초기화 완료 ✓"
}

# 함수: 샘플 데이터 수집 테스트
test_sample_collection() {
    log_step "샘플 데이터 수집 테스트 중..."
    
    cd "$PROJECT_ROOT"
    source "$VENV_PATH/bin/activate"
    
    # 소량 테스트 데이터 수집
    python3 -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT/common')

from bulk_data_collector import BulkDataCollector

# 테스트용 소량 수집
collector = BulkDataCollector(
    db_path='$DUCKDB_DIR/stock_data.db',
    batch_size=100,
    max_workers=2
)

try:
    # 상위 5개 종목만 테스트
    symbols = collector.get_nasdaq_symbols(limit=5)
    if symbols:
        print(f'테스트 종목: {symbols}')
        success = collector.collect_bulk_data(
            symbols=symbols,
            start_date='2024-01-01',
            batch_symbols=2
        )
        
        if success:
            stats = collector.get_collection_stats()
            print(f'✅ 샘플 수집 완료: {stats}')
        else:
            print('❌ 샘플 수집 실패')
            exit(1)
    else:
        print('❌ 종목 리스트 수집 실패')
        exit(1)
        
finally:
    collector.close()
"
    
    log_info "샘플 데이터 수집 테스트 완료 ✓"
}

# 함수: Airflow DAG 배포
deploy_airflow_dags() {
    log_step "Airflow DAG 배포 중..."
    
    # Airflow DAG 디렉토리 확인
    AIRFLOW_DAGS_DIR="${PROJECT_ROOT}/airflow/dags"
    AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
    
    if [ ! -d "$AIRFLOW_HOME" ]; then
        log_warn "Airflow가 설치되지 않았습니다. DAG 파일만 준비됩니다."
        return 0
    fi
    
    # DAG 파일 복사
    if [ -d "$AIRFLOW_DAGS_DIR" ]; then
        cp "$AIRFLOW_DAGS_DIR"/*.py "$AIRFLOW_HOME/dags/" 2>/dev/null || true
        log_info "Airflow DAG 배포 완료 ✓"
    else
        log_warn "DAG 디렉토리를 찾을 수 없습니다: $AIRFLOW_DAGS_DIR"
    fi
}

# 함수: 성능 모니터링 설정
setup_monitoring() {
    log_step "성능 모니터링 설정 중..."
    
    # 모니터링 스크립트 생성
    cat > "$DATA_DIR/monitor_performance.sh" << 'EOF'
#!/bin/bash
# 성능 모니터링 스크립트

while true; do
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    # 메모리 사용률
    memory_usage=$(free | grep Mem | awk '{printf "%.1f", $3/$2 * 100.0}')
    
    # 디스크 사용률
    disk_usage=$(df /data | tail -1 | awk '{print $5}' | sed 's/%//')
    
    # DuckDB 파일 크기
    db_size=0
    wal_size=0
    if [ -f "/data/duckdb/stock_data.db" ]; then
        db_size=$(stat -c%s "/data/duckdb/stock_data.db" 2>/dev/null || echo 0)
        db_size=$((db_size / 1024 / 1024))  # MB
    fi
    
    if [ -f "/data/duckdb/stock_data.db.wal" ]; then
        wal_size=$(stat -c%s "/data/duckdb/stock_data.db.wal" 2>/dev/null || echo 0)
        wal_size=$((wal_size / 1024 / 1024))  # MB
    fi
    
    # 로그 출력
    echo "[$timestamp] 메모리: ${memory_usage}% | 디스크: ${disk_usage}% | DB: ${db_size}MB | WAL: ${wal_size}MB"
    
    sleep 60  # 1분마다
done
EOF
    
    chmod +x "$DATA_DIR/monitor_performance.sh"
    
    log_info "성능 모니터링 설정 완료 ✓"
    log_info "모니터링 실행: $DATA_DIR/monitor_performance.sh"
}

# 함수: 전체 데이터 수집 실행
run_full_collection() {
    log_step "전체 데이터 수집 시작..."
    
    cd "$PROJECT_ROOT"
    source "$VENV_PATH/bin/activate"
    
    # 백그라운드에서 모니터링 시작
    if [ -f "$DATA_DIR/monitor_performance.sh" ]; then
        "$DATA_DIR/monitor_performance.sh" > "$DATA_DIR/logs/performance.log" 2>&1 &
        MONITOR_PID=$!
        log_info "성능 모니터링 시작됨 (PID: $MONITOR_PID)"
    fi
    
    # 대량 데이터 수집 실행
    python3 -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT/common')

from bulk_data_collector import BulkDataCollector
from parquet_loader import ParquetDataLoader
from batch_scheduler import DuckDBBatchScheduler, CheckpointConfig
import time

print('🚀 나스닥 5년치 대량 데이터 수집 시작...')

# 배치 스케줄러 시작
config = CheckpointConfig(
    interval_seconds=300,
    wal_size_threshold_mb=200,
    memory_threshold_percent=75,
    force_checkpoint_interval=1800
)

scheduler = DuckDBBatchScheduler('$DUCKDB_DIR/stock_data.db', config)
scheduler.start()

# 대량 수집기 초기화
collector = BulkDataCollector(
    db_path='$DUCKDB_DIR/stock_data.db',
    batch_size=2000,
    max_workers=3
)

try:
    # 전체 나스닥 종목 수집
    print('📊 나스닥 종목 리스트 수집 중...')
    symbols = collector.get_nasdaq_symbols()
    
    if not symbols:
        raise Exception('종목 리스트 수집 실패')
    
    print(f'✅ 종목 수집 완료: {len(symbols)}개')
    
    # 5년치 데이터 수집
    print('📈 5년치 데이터 수집 시작...')
    success = collector.collect_bulk_data(
        symbols=symbols,
        start_date='2020-01-01',
        batch_symbols=50
    )
    
    if not success:
        raise Exception('데이터 수집 실패')
    
    # 통계 출력
    stats = collector.get_collection_stats()
    print(f'✅ 데이터 수집 완료:')
    print(f'   종목 수: {stats.get(\"symbol_count\", 0):,}')
    print(f'   총 레코드: {stats.get(\"total_records\", 0):,}')
    print(f'   날짜 범위: {stats.get(\"date_range\", {})}')
    
    # Parquet 로더로 최적화
    print('🔧 데이터 최적화 중...')
    parquet_loader = ParquetDataLoader('$PARQUET_DIR')
    
    # 배치 병합
    parquet_loader.merge_batches_to_main('stock_data', '$DUCKDB_DIR/stock_data.db')
    
    # 저장소 최적화
    parquet_loader.optimize_storage('stock_data', '$DUCKDB_DIR/stock_data.db')
    
    # 완료 플래그 생성
    import json
    from datetime import datetime
    
    completion_info = {
        'completion_time': datetime.now().isoformat(),
        'collection_type': 'bulk_5year_data',
        'final_stats': stats,
        'success': True
    }
    
    with open('/tmp/nasdaq_bulk_collection_complete.flag', 'w') as f:
        json.dump(completion_info, f, indent=2, default=str)
    
    print('🎉 나스닥 5년치 대량 데이터 수집 완료!')
    
except Exception as e:
    print(f'❌ 데이터 수집 실패: {e}')
    exit(1)
    
finally:
    collector.close()
    scheduler.stop()
"
    
    # 모니터링 프로세스 종료
    if [ -n "$MONITOR_PID" ]; then
        kill $MONITOR_PID 2>/dev/null || true
        log_info "성능 모니터링 종료"
    fi
    
    log_info "전체 데이터 수집 완료 ✓"
}

# 함수: 도움말 출력
show_help() {
    echo "나스닥 최적화 데이터 수집 시스템"
    echo ""
    echo "사용법: $0 [옵션]"
    echo ""
    echo "옵션:"
    echo "  setup     - 전체 환경 설정 (디렉토리, Python, DB 초기화)"
    echo "  test      - 샘플 데이터 수집 테스트"
    echo "  full      - 전체 5년치 데이터 수집 실행"
    echo "  monitor   - 성능 모니터링만 실행"
    echo "  clean     - 임시 파일 및 캐시 정리"
    echo "  status    - 현재 상태 확인"
    echo "  help      - 이 도움말 출력"
    echo ""
    echo "예시:"
    echo "  $0 setup    # 처음 설정"
    echo "  $0 test     # 테스트 실행"
    echo "  $0 full     # 전체 수집"
}

# 함수: 상태 확인
check_status() {
    log_step "시스템 상태 확인 중..."
    
    # 데이터베이스 파일 확인
    if [ -f "$DUCKDB_DIR/stock_data.db" ]; then
        db_size=$(stat -c%s "$DUCKDB_DIR/stock_data.db" | awk '{printf "%.1f", $1/1024/1024}')
        log_info "DuckDB 데이터베이스: ${db_size}MB"
    else
        log_warn "DuckDB 데이터베이스가 없습니다"
    fi
    
    # WAL 파일 확인
    if [ -f "$DUCKDB_DIR/stock_data.db.wal" ]; then
        wal_size=$(stat -c%s "$DUCKDB_DIR/stock_data.db.wal" | awk '{printf "%.1f", $1/1024/1024}')
        log_info "WAL 파일: ${wal_size}MB"
    fi
    
    # Parquet 파일 확인
    parquet_count=$(find "$PARQUET_DIR" -name "*.parquet" 2>/dev/null | wc -l)
    log_info "Parquet 파일 수: $parquet_count"
    
    # 완료 플래그 확인
    if [ -f "/tmp/nasdaq_bulk_collection_complete.flag" ]; then
        log_info "대량 수집 완료 플래그 존재 ✓"
    else
        log_warn "대량 수집 완료 플래그 없음"
    fi
    
    # 시스템 리소스
    memory_usage=$(free | grep Mem | awk '{printf "%.1f", $3/$2 * 100.0}')
    disk_usage=$(df "$DATA_DIR" | tail -1 | awk '{print $5}')
    log_info "메모리 사용률: ${memory_usage}%"
    log_info "디스크 사용률: $disk_usage"
}

# 함수: 정리
clean_temp_files() {
    log_step "임시 파일 정리 중..."
    
    # 임시 디렉토리 정리
    rm -rf "$PARQUET_DIR/temp"/*
    rm -rf "$DATA_DIR/temp"/*
    
    # 오래된 로그 파일 정리 (7일 이상)
    find "$DATA_DIR/logs" -name "*.log" -mtime +7 -delete 2>/dev/null || true
    
    # 완료 플래그 정리 (선택적)
    # rm -f /tmp/nasdaq_*_complete.flag
    
    log_info "임시 파일 정리 완료 ✓"
}

# 메인 실행 부분
main() {
    echo "================================================"
    echo "나스닥 최적화 데이터 수집 시스템"
    echo "HDD 환경 최적화 | 5년치 대량 수집"
    echo "================================================"
    echo ""
    
    case "${1:-help}" in
        "setup")
            check_system_requirements
            setup_directories
            setup_python_environment
            initialize_database
            deploy_airflow_dags
            setup_monitoring
            log_info "🎉 전체 환경 설정 완료!"
            ;;
        "test")
            check_system_requirements
            test_sample_collection
            log_info "🎉 샘플 테스트 완료!"
            ;;
        "full")
            check_system_requirements
            run_full_collection
            log_info "🎉 전체 데이터 수집 완료!"
            ;;
        "monitor")
            if [ -f "$DATA_DIR/monitor_performance.sh" ]; then
                log_info "성능 모니터링 시작 (Ctrl+C로 종료)"
                "$DATA_DIR/monitor_performance.sh"
            else
                log_error "모니터링 스크립트가 없습니다. 먼저 'setup'을 실행하세요."
            fi
            ;;
        "status")
            check_status
            ;;
        "clean")
            clean_temp_files
            ;;
        "help"|*)
            show_help
            ;;
    esac
}

# 스크립트 실행
main "$@"
