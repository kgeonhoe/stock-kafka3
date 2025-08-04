#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DuckDB 배치 처리 및 체크포인트 스케줄러
- HDD 환경 최적화
- WAL 관리 및 주기적 체크포인트
- 메모리 사용량 모니터링
"""

import threading
import time
import duckdb
import psutil
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, Any
import logging
from dataclasses import dataclass
from queue import Queue, Empty
import json


@dataclass
class BatchWriteTask:
    """배치 쓰기 작업 정보"""
    table_name: str
    data: Any  # DataFrame 또는 데이터
    operation: str  # 'insert', 'update', 'delete'
    callback: Optional[Callable] = None
    priority: int = 1  # 1=높음, 2=보통, 3=낮음


@dataclass
class CheckpointConfig:
    """체크포인트 설정"""
    interval_seconds: int = 300  # 5분마다
    wal_size_threshold_mb: int = 100  # WAL 크기 임계값
    memory_threshold_percent: int = 80  # 메모리 사용률 임계값
    auto_checkpoint: bool = True
    force_checkpoint_interval: int = 1800  # 30분마다 강제 체크포인트


class DuckDBBatchScheduler:
    """DuckDB 배치 처리 및 체크포인트 스케줄러"""
    
    def __init__(self, db_path: str, config: Optional[CheckpointConfig] = None):
        """
        초기화
        
        Args:
            db_path: DuckDB 파일 경로
            config: 체크포인트 설정
        """
        self.db_path = db_path
        self.config = config or CheckpointConfig()
        
        # 로깅 설정
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # 스레드 및 큐 관리
        self._write_queue = Queue()
        self._running = False
        self._write_thread = None
        self._checkpoint_thread = None
        
        # 통계 정보
        self._stats = {
            'total_writes': 0,
            'successful_writes': 0,
            'failed_writes': 0,
            'checkpoint_count': 0,
            'last_checkpoint': None,
            'last_write': None
        }
        
        # 연결 관리
        self._write_conn = None
        self._write_lock = threading.Lock()
        
        # WAL 파일 경로
        self._wal_path = db_path + '.wal'
        
        self.logger.info(f"DuckDB 배치 스케줄러 초기화: {db_path}")
    
    def start(self):
        """스케줄러 시작"""
        if self._running:
            self.logger.warning("스케줄러가 이미 실행 중입니다")
            return
        
        self._running = True
        
        # 데이터베이스 연결 및 설정
        self._initialize_connection()
        
        # 쓰기 스레드 시작
        self._write_thread = threading.Thread(target=self._write_worker, daemon=True)
        self._write_thread.start()
        
        # 체크포인트 스레드 시작
        if self.config.auto_checkpoint:
            self._checkpoint_thread = threading.Thread(target=self._checkpoint_worker, daemon=True)
            self._checkpoint_thread.start()
        
        self.logger.info("DuckDB 배치 스케줄러 시작됨")
    
    def stop(self):
        """스케줄러 중지"""
        if not self._running:
            return
        
        self.logger.info("DuckDB 배치 스케줄러 종료 중...")
        
        self._running = False
        
        # 큐 비우기 (미처리 작업 완료)
        self._write_queue.put(None)  # 종료 신호
        
        # 스레드 종료 대기
        if self._write_thread:
            self._write_thread.join(timeout=30)
        
        if self._checkpoint_thread:
            self._checkpoint_thread.join(timeout=30)
        
        # 최종 체크포인트
        self._force_checkpoint()
        
        # 연결 정리
        if self._write_conn:
            self._write_conn.close()
            self._write_conn = None
        
        self.logger.info("DuckDB 배치 스케줄러 종료됨")
    
    def _initialize_connection(self):
        """데이터베이스 연결 초기화 및 최적화"""
        try:
            self._write_conn = duckdb.connect(self.db_path)
            
            # 성능 최적화 설정 - DuckDB는 SET 구문 사용 (큰따옴표 필요)
            self._write_conn.execute('SET memory_limit="1GB"')
            self._write_conn.execute("SET threads=2") 
            
            self.logger.info("DuckDB 연결 및 최적화 설정 완료")
            
        except Exception as e:
            self.logger.error(f"DuckDB 연결 초기화 실패: {e}")
            raise
    
    def submit_write_task(self, task: BatchWriteTask):
        """쓰기 작업 제출"""
        if not self._running:
            raise RuntimeError("스케줄러가 실행되지 않음")
        
        self._write_queue.put(task)
        self.logger.debug(f"쓰기 작업 제출: {task.table_name} ({task.operation})")
    
    def submit_bulk_insert(self, table_name: str, df_data, callback: Optional[Callable] = None):
        """대량 삽입 작업 제출 (편의 메서드)"""
        task = BatchWriteTask(
            table_name=table_name,
            data=df_data,
            operation='bulk_insert',
            callback=callback,
            priority=1
        )
        self.submit_write_task(task)
    
    def _write_worker(self):
        """쓰기 작업 처리 워커"""
        self.logger.info("쓰기 워커 시작")
        
        while self._running:
            try:
                # 작업 대기 (타임아웃으로 주기적 체크)
                task = self._write_queue.get(timeout=5)
                
                if task is None:  # 종료 신호
                    break
                
                # 작업 실행
                self._execute_write_task(task)
                
                # 큐 작업 완료 표시
                self._write_queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                self.logger.error(f"쓰기 워커 오류: {e}")
                self._stats['failed_writes'] += 1
        
        self.logger.info("쓰기 워커 종료")
    
    def _execute_write_task(self, task: BatchWriteTask):
        """개별 쓰기 작업 실행"""
        try:
            with self._write_lock:
                start_time = time.time()
                
                if task.operation == 'bulk_insert':
                    self._execute_bulk_insert(task.table_name, task.data)
                elif task.operation == 'insert':
                    self._execute_insert(task.table_name, task.data)
                else:
                    raise ValueError(f"지원하지 않는 작업: {task.operation}")
                
                # 통계 업데이트
                self._stats['total_writes'] += 1
                self._stats['successful_writes'] += 1
                self._stats['last_write'] = datetime.now()
                
                execution_time = time.time() - start_time
                self.logger.debug(f"쓰기 작업 완료: {task.table_name} ({execution_time:.2f}초)")
                
                # 콜백 실행
                if task.callback:
                    try:
                        task.callback(success=True, execution_time=execution_time)
                    except Exception as e:
                        self.logger.error(f"콜백 실행 실패: {e}")
                
        except Exception as e:
            self.logger.error(f"쓰기 작업 실패 ({task.table_name}): {e}")
            self._stats['failed_writes'] += 1
            
            if task.callback:
                try:
                    task.callback(success=False, error=str(e))
                except:
                    pass
    
    def _execute_bulk_insert(self, table_name: str, df_data):
        """대량 삽입 실행"""
        import pandas as pd
        
        if isinstance(df_data, pd.DataFrame):
            # DataFrame으로 직접 삽입 (DuckDB 최적화)
            self._write_conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_data")
        else:
            raise ValueError("DataFrame이 아닌 데이터는 지원하지 않음")
    
    def _execute_insert(self, table_name: str, data):
        """일반 삽입 실행"""
        # 일반적인 INSERT 구문 실행
        if isinstance(data, dict) and 'query' in data:
            self._write_conn.execute(data['query'], data.get('params', []))
        else:
            raise ValueError("잘못된 삽입 데이터 형식")
    
    def _checkpoint_worker(self):
        """체크포인트 처리 워커"""
        self.logger.info("체크포인트 워커 시작")
        
        last_force_checkpoint = time.time()
        
        while self._running:
            try:
                time.sleep(self.config.interval_seconds)
                
                if not self._running:
                    break
                
                # 체크포인트 필요성 확인
                should_checkpoint = self._should_checkpoint()
                
                # 강제 체크포인트 시간 확인
                current_time = time.time()
                force_checkpoint = (current_time - last_force_checkpoint) >= self.config.force_checkpoint_interval
                
                if should_checkpoint or force_checkpoint:
                    self._execute_checkpoint()
                    
                    if force_checkpoint:
                        last_force_checkpoint = current_time
                        self.logger.info("강제 체크포인트 실행")
                
            except Exception as e:
                self.logger.error(f"체크포인트 워커 오류: {e}")
        
        self.logger.info("체크포인트 워커 종료")
    
    def _should_checkpoint(self) -> bool:
        """체크포인트 필요성 판단"""
        try:
            # 1. WAL 파일 크기 확인
            if os.path.exists(self._wal_path):
                wal_size_mb = os.path.getsize(self._wal_path) / (1024 * 1024)
                if wal_size_mb >= self.config.wal_size_threshold_mb:
                    self.logger.info(f"WAL 크기 임계값 초과: {wal_size_mb:.1f}MB")
                    return True
            
            # 2. 메모리 사용률 확인
            memory_percent = psutil.virtual_memory().percent
            if memory_percent >= self.config.memory_threshold_percent:
                self.logger.info(f"메모리 사용률 임계값 초과: {memory_percent:.1f}%")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"체크포인트 필요성 판단 실패: {e}")
            return True  # 안전을 위해 체크포인트 실행
    
    def _execute_checkpoint(self):
        """체크포인트 실행"""
        try:
            with self._write_lock:
                start_time = time.time()
                
                self._write_conn.execute("CHECKPOINT")
                
                execution_time = time.time() - start_time
                self._stats['checkpoint_count'] += 1
                self._stats['last_checkpoint'] = datetime.now()
                
                self.logger.info(f"체크포인트 완료 ({execution_time:.2f}초)")
                
        except Exception as e:
            self.logger.error(f"체크포인트 실행 실패: {e}")
            raise
    
    def _force_checkpoint(self):
        """강제 체크포인트 (종료 시)"""
        try:
            if self._write_conn:
                self._execute_checkpoint()
                self.logger.info("강제 체크포인트 완료")
        except Exception as e:
            self.logger.error(f"강제 체크포인트 실패: {e}")
    
    def force_checkpoint(self):
        """외부에서 강제 체크포인트 호출"""
        if not self._running:
            raise RuntimeError("스케줄러가 실행되지 않음")
        
        try:
            self._execute_checkpoint()
        except Exception as e:
            self.logger.error(f"수동 체크포인트 실패: {e}")
            raise
    
    def get_stats(self) -> Dict:
        """통계 정보 조회"""
        stats = self._stats.copy()
        
        # 추가 정보
        try:
            if os.path.exists(self._wal_path):
                stats['wal_size_mb'] = os.path.getsize(self._wal_path) / (1024 * 1024)
            else:
                stats['wal_size_mb'] = 0
            
            stats['memory_usage_percent'] = psutil.virtual_memory().percent
            stats['queue_size'] = self._write_queue.qsize()
            stats['is_running'] = self._running
            
        except Exception as e:
            self.logger.error(f"통계 수집 실패: {e}")
        
        return stats
    
    def get_performance_metrics(self) -> Dict:
        """성능 메트릭 조회"""
        try:
            with duckdb.connect(self.db_path) as conn:
                # 데이터베이스 크기
                db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
                
                # WAL 크기
                wal_size_mb = os.path.getsize(self._wal_path) / (1024 * 1024) if os.path.exists(self._wal_path) else 0
                
                return {
                    'database_size_mb': db_size_mb,
                    'wal_size_mb': wal_size_mb,
                    'total_size_mb': db_size_mb + wal_size_mb,
                    'memory_usage_percent': psutil.virtual_memory().percent,
                    'disk_usage_percent': psutil.disk_usage('/').percent,
                    'stats': self.get_stats()
                }
                
        except Exception as e:
            self.logger.error(f"성능 메트릭 조회 실패: {e}")
            return {}


# 사용 예시
if __name__ == "__main__":
    import pandas as pd
    
    # 스케줄러 설정
    config = CheckpointConfig(
        interval_seconds=60,      # 1분마다 체크
        wal_size_threshold_mb=50, # 50MB WAL 임계값
        memory_threshold_percent=70,
        force_checkpoint_interval=600  # 10분마다 강제 체크포인트
    )
    
    scheduler = DuckDBBatchScheduler("/tmp/test_batch.db", config)
    
    try:
        scheduler.start()
        
        # 테스트 데이터 삽입
        test_data = pd.DataFrame({
            'id': range(1000),
            'value': range(1000),
            'timestamp': pd.date_range('2023-01-01', periods=1000, freq='1H')
        })
        
        # 테이블 생성
        with duckdb.connect("/tmp/test_batch.db") as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER,
                    value INTEGER,
                    timestamp TIMESTAMP
                )
            """)
        
        # 배치 삽입 테스트
        def callback(success, **kwargs):
            if success:
                print(f"삽입 성공: {kwargs.get('execution_time', 0):.2f}초")
            else:
                print(f"삽입 실패: {kwargs.get('error', 'Unknown')}")
        
        scheduler.submit_bulk_insert('test_table', test_data, callback)
        
        # 통계 출력
        time.sleep(5)
        stats = scheduler.get_stats()
        print(f"통계: {stats}")
        
        metrics = scheduler.get_performance_metrics()
        print(f"성능 메트릭: {metrics}")
        
    finally:
        scheduler.stop()
