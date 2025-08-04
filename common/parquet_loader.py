#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parquet 기반 고속 데이터 로더
- CSV/Parquet bulk ingest 최적화
- HDD 환경에서 최고 성능 추출
- WAL 없는 append-only 구조
"""

import pandas as pd
import duckdb
import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import json


class ParquetDataLoader:
    """Parquet 기반 고속 데이터 로더"""
    
    def __init__(self, base_path: str = "/data/parquet_storage"):
        """
        초기화
        
        Args:
            base_path: Parquet 파일 저장 기본 경로
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # 로깅 설정
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # 경로 설정
        self.staging_path = self.base_path / "staging"
        self.archive_path = self.base_path / "archive"
        self.temp_path = self.base_path / "temp"
        
        for path in [self.staging_path, self.archive_path, self.temp_path]:
            path.mkdir(exist_ok=True)
        
        self.logger.info(f"Parquet 데이터 로더 초기화: {base_path}")
    
    def save_to_parquet(self, df: pd.DataFrame, 
                       table_name: str, 
                       partition_cols: Optional[List[str]] = None,
                       compression: str = 'snappy') -> str:
        """
        DataFrame을 Parquet 파일로 저장
        
        Args:
            df: 저장할 DataFrame
            table_name: 테이블 이름
            partition_cols: 파티션 컬럼
            compression: 압축 방식
        
        Returns:
            저장된 파일 경로
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{table_name}_{timestamp}.parquet"
            file_path = self.staging_path / filename
            
            # 파티션이 있는 경우
            if partition_cols:
                # 파티션별로 디렉토리 생성하여 저장
                partition_path = self.staging_path / table_name / timestamp
                partition_path.mkdir(parents=True, exist_ok=True)
                
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(
                    table, 
                    root_path=str(partition_path),
                    partition_cols=partition_cols,
                    compression=compression,
                    use_legacy_dataset=False
                )
                
                saved_path = str(partition_path)
                
            else:
                # 단일 파일로 저장
                df.to_parquet(
                    file_path, 
                    compression=compression, 
                    index=False,
                    engine='pyarrow'
                )
                
                saved_path = str(file_path)
            
            self.logger.info(f"Parquet 저장 완료: {saved_path} ({len(df)} 레코드)")
            return saved_path
            
        except Exception as e:
            self.logger.error(f"Parquet 저장 실패: {e}")
            raise
    
    def bulk_load_from_parquet(self, db_path: str, 
                              table_name: str, 
                              parquet_files: List[str],
                              create_table_sql: Optional[str] = None) -> bool:
        """
        Parquet 파일들을 DuckDB로 고속 로드
        
        Args:
            db_path: DuckDB 파일 경로
            table_name: 대상 테이블 이름
            parquet_files: Parquet 파일 경로 리스트
            create_table_sql: 테이블 생성 SQL (없으면 자동 생성)
        
        Returns:
            성공 여부
        """
        try:
            with duckdb.connect(db_path) as conn:
                conn.execute('SET memory_limit="2GB"')
                conn.execute("SET threads=4")
                
                # 테이블 생성 (필요한 경우)
                if create_table_sql:
                    conn.execute(create_table_sql)
                
                total_records = 0
                start_time = datetime.now()
                
                for parquet_file in parquet_files:
                    if os.path.exists(parquet_file):
                        # Parquet 파일에서 직접 읽어서 삽입 (매우 빠름)
                        if os.path.isdir(parquet_file):
                            # 파티션된 디렉토리
                            conn.execute(f"""
                                INSERT INTO {table_name} 
                                SELECT * FROM read_parquet('{parquet_file}/**/*.parquet')
                            """)
                        else:
                            # 단일 파일
                            conn.execute(f"""
                                INSERT INTO {table_name} 
                                SELECT * FROM read_parquet('{parquet_file}')
                            """)
                        
                        # 레코드 수 계산
                        result = conn.execute(f"""
                            SELECT COUNT(*) FROM read_parquet('{parquet_file}/**/*.parquet' if '{parquet_file}'.endswith('/') else '{parquet_file}')
                        """).fetchone()
                        
                        if result:
                            records = result[0]
                            total_records += records
                            self.logger.info(f"로드 완료: {parquet_file} ({records} 레코드)")
                
                # 최종 체크포인트
                conn.execute("CHECKPOINT")
                
                duration = (datetime.now() - start_time).total_seconds()
                self.logger.info(f"전체 로드 완료: {total_records} 레코드, {duration:.2f}초")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Parquet 로드 실패: {e}")
            return False
    
    def create_append_only_structure(self, db_path: str, table_name: str) -> bool:
        """
        Append-only 로그 테이블 구조 생성
        
        Args:
            db_path: DuckDB 파일 경로
            table_name: 기본 테이블 이름
        
        Returns:
            성공 여부
        """
        try:
            with duckdb.connect(db_path) as conn:
                # 1. 메인 테이블 (읽기 전용) - yfinance 호환성을 위한 Adjusted Close 기준
                # 시스템 일관성: yfinance auto_adjust=True와 동일한 방식
                # close = Adjusted Close (분할/배당 반영) - 기존 시스템과 호환
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name}_main (
                        symbol VARCHAR,
                        date DATE,
                        open DOUBLE,            -- FDR adj_open (분할/배당 반영)
                        high DOUBLE,            -- FDR adj_high (분할/배당 반영)
                        low DOUBLE,             -- FDR adj_low (분할/배당 반영)
                        close DOUBLE,           -- FDR adj_close (분할/배당 반영) = yfinance close
                        volume BIGINT,          -- 거래량
                        raw_close DOUBLE,       -- FDR raw close (분할/배당 미반영) - 선택적 보관
                        batch_id VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 2. 로그 테이블 (append-only) - yfinance 호환성을 위한 Adjusted Close 기준
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name}_log (
                        symbol VARCHAR,
                        date DATE,
                        open DOUBLE,            -- FDR adj_open (분할/배당 반영)
                        high DOUBLE,            -- FDR adj_high (분할/배당 반영)
                        low DOUBLE,             -- FDR adj_low (분할/배당 반영)
                        close DOUBLE,           -- FDR adj_close (분할/배당 반영) = yfinance close
                        volume BIGINT,
                        raw_close DOUBLE,       -- FDR raw close (분할/배당 미반영) - 선택적 보관
                        batch_id VARCHAR,
                        operation VARCHAR DEFAULT 'INSERT',
                        log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 3. 배치 메타데이터 테이블
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name}_batches (
                        batch_id VARCHAR PRIMARY KEY,
                        parquet_file VARCHAR,
                        record_count INTEGER,
                        start_date DATE,
                        end_date DATE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        merged_at TIMESTAMP
                    )
                """)
                
                # 4. 통합 뷰 생성 - yfinance 호환성 우선
                # 기존 시스템과의 완벽한 호환성을 위해 close = adjusted close
                conn.execute(f"""
                    CREATE VIEW IF NOT EXISTS {table_name}_view AS
                    SELECT * FROM {table_name}_main
                    UNION ALL
                    SELECT 
                        symbol, date, 
                        open,           -- Adjusted Open (yfinance 호환)
                        high,           -- Adjusted High (yfinance 호환)
                        low,            -- Adjusted Low (yfinance 호환)
                        close,          -- Adjusted Close (yfinance 호환)
                        volume, 
                        raw_close,      -- Raw Close (분석용 추가 정보)
                        batch_id, log_timestamp as created_at
                    FROM {table_name}_log
                    WHERE operation = 'INSERT'
                """)
                
                self.logger.info(f"Append-only 구조 생성 완료: {table_name} (yfinance 호환)")
                self.logger.info("📝 컬럼 의미 (yfinance 일관성):")
                self.logger.info("  - open/high/low/close: Adjusted OHLC (분할/배당 반영) = yfinance 동일")
                self.logger.info("  - raw_close: Raw Close (분할/배당 미반영) - 추가 분석용")
                self.logger.info("  - volume: 거래량")
                return True
                
        except Exception as e:
            self.logger.error(f"Append-only 구조 생성 실패: {e}")
            return False
    
    def append_data_batch(self, df: pd.DataFrame, 
                         table_name: str, 
                         db_path: str,
                         batch_id: Optional[str] = None) -> bool:
        """
        데이터를 append-only 방식으로 추가
        
        Args:
            df: 추가할 데이터
            table_name: 테이블 이름
            db_path: DuckDB 파일 경로
            batch_id: 배치 ID
        
        Returns:
            성공 여부
        """
        try:
            if batch_id is None:
                batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 1. Parquet 파일로 임시 저장
            df_with_batch = df.copy()
            df_with_batch['batch_id'] = batch_id
            
            parquet_file = self.save_to_parquet(df_with_batch, f"{table_name}_batch")
            
            # 2. 로그 테이블에 직접 로드 (WAL 최소화)
            with duckdb.connect(db_path) as conn:
                conn.execute(f"""
                    INSERT INTO {table_name}_log 
                    SELECT *, 'INSERT' as operation, CURRENT_TIMESTAMP as log_timestamp
                    FROM read_parquet('{parquet_file}')
                """)
                
                # 3. 배치 메타데이터 저장
                record_count = len(df)
                start_date = df['date'].min() if 'date' in df.columns else None
                end_date = df['date'].max() if 'date' in df.columns else None
                
                conn.execute(f"""
                    INSERT INTO {table_name}_batches 
                    (batch_id, parquet_file, record_count, start_date, end_date)
                    VALUES (?, ?, ?, ?, ?)
                """, (batch_id, parquet_file, record_count, start_date, end_date))
                
                # 즉시 체크포인트 (WAL flush)
                conn.execute("CHECKPOINT")
            
            self.logger.info(f"배치 추가 완료: {batch_id} ({record_count} 레코드)")
            return True
            
        except Exception as e:
            self.logger.error(f"배치 추가 실패: {e}")
            return False
    
    def merge_batches_to_main(self, table_name: str, 
                             db_path: str, 
                             max_batches: int = 10) -> bool:
        """
        로그 테이블의 배치들을 메인 테이블로 병합
        
        Args:
            table_name: 테이블 이름
            db_path: DuckDB 파일 경로
            max_batches: 한 번에 병합할 최대 배치 수
        
        Returns:
            성공 여부
        """
        try:
            with duckdb.connect(db_path) as conn:
                # 1. 병합할 배치 조회
                unmerged_batches = conn.execute(f"""
                    SELECT batch_id, parquet_file, record_count
                    FROM {table_name}_batches 
                    WHERE merged_at IS NULL 
                    ORDER BY created_at 
                    LIMIT {max_batches}
                """).fetchall()
                
                if not unmerged_batches:
                    self.logger.info("병합할 배치가 없습니다")
                    return True
                
                total_records = 0
                merged_batch_ids = []
                
                # 2. 배치별로 메인 테이블로 이동 - yfinance 호환성 유지
                for batch_id, parquet_file, record_count in unmerged_batches:
                    conn.execute(f"""
                        INSERT INTO {table_name}_main
                        SELECT symbol, date, open, high, low, close, volume, raw_close,
                               batch_id, log_timestamp as created_at
                        FROM {table_name}_log 
                        WHERE batch_id = ? AND operation = 'INSERT'
                    """, (batch_id,))
                    
                    total_records += record_count
                    merged_batch_ids.append(batch_id)
                
                # 3. 병합된 로그 데이터 삭제
                for batch_id in merged_batch_ids:
                    conn.execute(f"""
                        DELETE FROM {table_name}_log 
                        WHERE batch_id = ?
                    """, (batch_id,))
                
                # 4. 배치 메타데이터 업데이트
                conn.execute(f"""
                    UPDATE {table_name}_batches 
                    SET merged_at = CURRENT_TIMESTAMP 
                    WHERE batch_id IN ({','.join(['?'] * len(merged_batch_ids))})
                """, merged_batch_ids)
                
                # 5. 최종 체크포인트
                conn.execute("CHECKPOINT")
                
                self.logger.info(f"배치 병합 완료: {len(merged_batch_ids)}개 배치, {total_records} 레코드")
                return True
                
        except Exception as e:
            self.logger.error(f"배치 병합 실패: {e}")
            return False
    
    def optimize_storage(self, table_name: str, db_path: str) -> bool:
        """
        저장소 최적화 (압축, 인덱스 등)
        
        Args:
            table_name: 테이블 이름
            db_path: DuckDB 파일 경로
        
        Returns:
            성공 여부
        """
        try:
            with duckdb.connect(db_path) as conn:
                # 1. 통계 업데이트
                conn.execute(f"ANALYZE {table_name}_main")
                
                # 2. 중복 제거 (혹시 모를)
                conn.execute(f"""
                    CREATE TABLE {table_name}_main_clean AS 
                    SELECT DISTINCT * FROM {table_name}_main
                """)
                
                conn.execute(f"DROP TABLE {table_name}_main")
                conn.execute(f"ALTER TABLE {table_name}_main_clean RENAME TO {table_name}_main")
                
                # 3. 인덱스 재생성
                conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol_date 
                    ON {table_name}_main(symbol, date)
                """)
                
                # 4. 체크포인트
                conn.execute("CHECKPOINT")
                
                self.logger.info(f"저장소 최적화 완료: {table_name}")
                return True
                
        except Exception as e:
            self.logger.error(f"저장소 최적화 실패: {e}")
            return False
    
    def export_to_parquet_archive(self, table_name: str, 
                                 db_path: str,
                                 partition_by: str = 'date') -> str:
        """
        메인 테이블을 Parquet 아카이브로 내보내기
        
        Args:
            table_name: 테이블 이름
            db_path: DuckDB 파일 경로
            partition_by: 파티션 기준 컬럼
        
        Returns:
            아카이브 경로
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = self.archive_path / f"{table_name}_{timestamp}"
            archive_dir.mkdir(exist_ok=True)
            
            with duckdb.connect(db_path) as conn:
                # Parquet으로 내보내기 (파티션된)
                conn.execute(f"""
                    COPY (SELECT * FROM {table_name}_main) 
                    TO '{archive_dir}' 
                    (FORMAT PARQUET, PARTITION_BY ({partition_by}))
                """)
            
            self.logger.info(f"Parquet 아카이브 생성: {archive_dir}")
            return str(archive_dir)
            
        except Exception as e:
            self.logger.error(f"Parquet 아카이브 실패: {e}")
            raise
    
    def get_storage_stats(self, table_name: str, db_path: str) -> Dict:
        """저장소 통계 조회"""
        try:
            stats = {}
            
            with duckdb.connect(db_path) as conn:
                # 메인 테이블 통계
                main_stats = conn.execute(f"""
                    SELECT COUNT(*) as count, 
                           MIN(date) as min_date, 
                           MAX(date) as max_date,
                           COUNT(DISTINCT symbol) as symbol_count
                    FROM {table_name}_main
                """).fetchone()
                
                # 로그 테이블 통계
                log_stats = conn.execute(f"""
                    SELECT COUNT(*) as count,
                           COUNT(DISTINCT batch_id) as batch_count
                    FROM {table_name}_log
                """).fetchone()
                
                # 배치 메타데이터 통계
                batch_stats = conn.execute(f"""
                    SELECT COUNT(*) as total_batches,
                           COUNT(CASE WHEN merged_at IS NULL THEN 1 END) as unmerged_batches
                    FROM {table_name}_batches
                """).fetchone()
                
                stats = {
                    'main_table': {
                        'record_count': main_stats[0] if main_stats else 0,
                        'date_range': {
                            'start': main_stats[1] if main_stats else None,
                            'end': main_stats[2] if main_stats else None
                        },
                        'symbol_count': main_stats[3] if main_stats else 0
                    },
                    'log_table': {
                        'record_count': log_stats[0] if log_stats else 0,
                        'batch_count': log_stats[1] if log_stats else 0
                    },
                    'batches': {
                        'total': batch_stats[0] if batch_stats else 0,
                        'unmerged': batch_stats[1] if batch_stats else 0
                    }
                }
            
            # 파일 시스템 통계
            if os.path.exists(db_path):
                stats['file_sizes'] = {
                    'database_mb': os.path.getsize(db_path) / (1024 * 1024),
                    'wal_mb': os.path.getsize(db_path + '.wal') / (1024 * 1024) if os.path.exists(db_path + '.wal') else 0
                }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"저장소 통계 조회 실패: {e}")
            return {}


# 사용 예시
if __name__ == "__main__":
    
    loader = ParquetDataLoader("/tmp/parquet_test")
    
    # 테스트 데이터 생성 - yfinance 호환성을 위한 Adjusted Close 기준
    # 시스템 일관성: yfinance auto_adjust=True와 동일한 방식
    # close = Adjusted Close (분할/배당 반영) - 기존 시스템과 완벽 호환
    test_data = pd.DataFrame({
        'symbol': ['AAPL'] * 1000 + ['GOOGL'] * 1000,
        'date': pd.date_range('2023-01-01', periods=2000, freq='1H')[:2000],
        'open': [99.5] * 2000,      # Adjusted Open (분할/배당 반영)
        'high': [119.5] * 2000,     # Adjusted High (분할/배당 반영)  
        'low': [89.5] * 2000,       # Adjusted Low (분할/배당 반영)
        'close': [109.5] * 2000,    # Adjusted Close (분할/배당 반영) = yfinance close
        'volume': [5000] * 2000,    # 거래량
        'raw_close': [110.0] * 2000 # Raw Close (분할/배당 미반영) - 추가 분석용
    })
    
    db_path = "/tmp/test_parquet.db"
    table_name = "stock_data"
    
    try:
        # 1. Append-only 구조 생성
        loader.create_append_only_structure(db_path, table_name)
        
        # 2. 배치 데이터 추가
        loader.append_data_batch(test_data, table_name, db_path)
        
        # 3. 통계 조회
        stats = loader.get_storage_stats(table_name, db_path)
        print(f"저장소 통계: {stats}")
        
        # 4. 배치 병합
        loader.merge_batches_to_main(table_name, db_path)
        
        # 5. 최종 통계
        final_stats = loader.get_storage_stats(table_name, db_path)
        print(f"최종 통계: {final_stats}")
        
    except Exception as e:
        print(f"테스트 실패: {e}")
