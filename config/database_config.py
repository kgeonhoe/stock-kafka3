"""
Database Configuration for Stock Kafka Pipeline
DuckDB 데이터베이스 경로 및 설정 관리
"""

import os
from pathlib import Path
import shutil

class DatabaseConfig:
    """데이터베이스 설정 관리"""
    
    # 환경에 따른 기본 경로 설정
    # Docker 환경: /data (docker-compose.yml에서 마운트)
    # 로컬 환경: /home/grey1/stock-kafka3/data
    if os.path.exists('/.dockerenv'):
        # Docker 환경
        BASE_PATH = os.getenv("DATABASE_BASE_PATH", "/data")
    else:
        # 로컬 환경
        BASE_PATH = os.getenv("DATABASE_BASE_PATH", "/home/grey1/stock-kafka3/data")
    
    # DuckDB 설정 - cache_data.db 제거
    DUCKDB_BASE_DIR = f"{BASE_PATH}/duckdb"
    DUCKDB_MAIN_DB = f"{DUCKDB_BASE_DIR}/stock_data.db"
    DUCKDB_ANALYTICS_DB = f"{DUCKDB_BASE_DIR}/analytics_data.db"
    
    # 백업 및 아카이브 설정
    BACKUP_DIR = f"{BASE_PATH}/backups"
    ARCHIVE_DIR = f"{BASE_PATH}/archives"
    
    # DuckDB 성능 설정
    DUCKDB_MEMORY_LIMIT = "4GB"  # 1TB HDD 환경에 맞게 증가
    DUCKDB_THREADS = 4           # CPU 코어에 맞게 조정
    DUCKDB_TEMP_DIR = f"{BASE_PATH}/temp"
    
    # 테이블 이름 정의
    TABLE_NASDAQ_SYMBOLS = "nasdaq_symbols"
    TABLE_STOCK_DATA = "stock_data"
    TABLE_TECHNICAL_INDICATORS = "stock_data_technical_indicators"
    TABLE_PORTFOLIO = "portfolio"
    TABLE_TRADING_SIGNALS = "trading_signals"
    
    # Redis 설정 추가
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
    
    # Redis 키 프리픽스
    REDIS_KEY_PREFIX = {
        'realtime_price': 'rt:price:',      # 실시간 가격
        'technical': 'tech:',               # 기술적 지표 캐시
        'symbol_info': 'symbol:',           # 종목 정보
        'portfolio': 'portfolio:',          # 포트폴리오 상태
        'signal': 'signal:',                # 거래 신호
        'rate_limit': 'rl:',                # API 호출 제한
    }
    
    # Redis TTL 설정 (초 단위)
    REDIS_TTL = {
        'realtime_price': 10,               # 10초
        'technical': 300,                   # 5분
        'symbol_info': 86400,               # 24시간
        'portfolio': 60,                    # 1분
        'signal': 30,                       # 30초
        'rate_limit': 60,                   # 1분
    }
    
    @classmethod
    def ensure_directories(cls):
        """필요한 디렉토리들 생성"""
        directories = [
            cls.DUCKDB_BASE_DIR,
            cls.BACKUP_DIR,
            cls.ARCHIVE_DIR,
            cls.DUCKDB_TEMP_DIR
        ]
        
        for directory in directories:
            try:
                Path(directory).mkdir(parents=True, exist_ok=True)
                print(f"✅ Directory ensured: {directory}")
            except PermissionError as e:
                print(f"⚠️ Permission denied for {directory}: {e}")
                # Docker 환경에서는 권한 문제가 발생할 수 있음
                if not os.path.exists('/.dockerenv'):
                    raise
            except Exception as e:
                print(f"❌ Error creating {directory}: {e}")
                raise
    
    @classmethod
    def get_connection_config(cls):
        """DuckDB 연결 설정 반환"""
        return {
            'database': cls.DUCKDB_MAIN_DB,
            'config': {
                'memory_limit': cls.DUCKDB_MEMORY_LIMIT,
                'threads': cls.DUCKDB_THREADS,
                'temp_directory': cls.DUCKDB_TEMP_DIR,
                'max_memory': '8GB',
                'enable_progress_bar': True,
                'enable_progress_bar_print': False,
                'access_mode': 'READ_WRITE',
                'enable_external_access': True
            }
        }
    
    @classmethod
    def get_disk_usage(cls):
        """디스크 사용량 확인"""
        try:
            total, used, free = shutil.disk_usage(cls.BASE_PATH)
            return {
                'total_gb': round(total / (1024**3), 2),
                'used_gb': round(used / (1024**3), 2),
                'free_gb': round(free / (1024**3), 2),
                'usage_percent': round((used / total) * 100, 1)
            }
        except Exception as e:
            return {'error': str(e)}
    
    @classmethod
    def print_config_info(cls):
        """설정 정보 출력"""
        print("\n🗄️  DuckDB Configuration")
        print("=" * 50)
        print(f"📍 Environment: {'Docker' if os.path.exists('/.dockerenv') else 'Local'}")
        print(f"📍 Base Path: {cls.BASE_PATH}")
        print(f"🗄️  Main DB: {cls.DUCKDB_MAIN_DB}")
        print(f"📊 Analytics DB: {cls.DUCKDB_ANALYTICS_DB}")
        print(f"💾 Memory Limit: {cls.DUCKDB_MEMORY_LIMIT}")
        print(f"🔧 Threads: {cls.DUCKDB_THREADS}")
        print("")
        
        # 디스크 사용량
        usage = cls.get_disk_usage()
        if 'error' not in usage:
            print(f"💽 Disk Usage:")
            print(f"   Total: {usage['total_gb']} GB")
            print(f"   Used:  {usage['used_gb']} GB ({usage['usage_percent']}%)")
            print(f"   Free:  {usage['free_gb']} GB")
        else:
            print(f"❌ Disk usage error: {usage['error']}")
        print("=" * 50)

# 환경변수로도 설정 가능
def get_db_path():
    """환경변수 또는 기본값으로 DB 경로 반환"""
    return os.getenv('DUCKDB_PATH', DatabaseConfig.DUCKDB_MAIN_DB)

def get_cache_db_path():
    """캐시 DB 경로 반환"""
    return os.getenv('DUCKDB_CACHE_PATH', DatabaseConfig.DUCKDB_CACHE_DB)

def get_analytics_db_path():
    """분석 DB 경로 반환"""
    return os.getenv('DUCKDB_ANALYTICS_PATH', DatabaseConfig.DUCKDB_ANALYTICS_DB)

# 빠른 접근을 위한 별칭
DB_CONFIG = DatabaseConfig

if __name__ == "__main__":
    # 설정 테스트
    DatabaseConfig.ensure_directories()
    DatabaseConfig.print_config_info()
