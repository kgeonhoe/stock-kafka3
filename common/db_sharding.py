#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
from typing import List, Dict, Any
from database import DuckDBManager

class DBShardManager:
    """
    데이터베이스 샤딩 관리 클래스
    - stock_data를 5개 DB로 분산
    - watchlist는 별도 DB로 관리
    """
    
    def __init__(self, base_path: str = "/data/duckdb"):
        """
        DB 샤드 매니저 초기화
        
        Args:
            base_path: 데이터베이스 파일 기본 경로
        """
        self.base_path = base_path
        self.shard_count = 5  # 5개 샤드
        
        # 샤드 DB 경로들
        self.shard_paths = {
            i: os.path.join(base_path, f"stock_data_shard_{i}.db")
            for i in range(1, self.shard_count + 1)
        }
        
        # 특별 용도 DB들
        self.watchlist_path = os.path.join(base_path, "watchlist.db")
        self.metadata_path = os.path.join(base_path, "metadata.db")  # 샤드 메타데이터
        
        # DB 매니저들 (지연 초기화)
        self._shard_managers = {}
        self._watchlist_manager = None
        self._metadata_manager = None
    
    def get_shard_number(self, symbol: str) -> int:
        """
        심볼을 해시하여 샤드 번호 반환 (1~5)
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            샤드 번호 (1~5)
        """
        # MD5 해시 사용 (안정적이고 고른 분산)
        hash_value = hashlib.md5(symbol.encode('utf-8')).hexdigest()
        # 16진수를 정수로 변환 후 샤드 수로 나눈 나머지 + 1
        return (int(hash_value, 16) % self.shard_count) + 1
    
    def get_shard_manager(self, shard_number: int) -> DuckDBManager:
        """
        샤드 DB 매니저 반환 (지연 초기화)
        
        Args:
            shard_number: 샤드 번호 (1~5)
            
        Returns:
            DuckDBManager 인스턴스
        """
        if shard_number not in self._shard_managers:
            db_path = self.shard_paths[shard_number]
            self._shard_managers[shard_number] = DuckDBManager(db_path)
        
        return self._shard_managers[shard_number]
    
    def get_watchlist_manager(self) -> DuckDBManager:
        """
        워치리스트 DB 매니저 반환 (지연 초기화)
        
        Returns:
            DuckDBManager 인스턴스
        """
        if self._watchlist_manager is None:
            self._watchlist_manager = DuckDBManager(self.watchlist_path)
        
        return self._watchlist_manager
    
    def get_metadata_manager(self) -> DuckDBManager:
        """
        메타데이터 DB 매니저 반환 (지연 초기화)
        
        Returns:
            DuckDBManager 인스턴스
        """
        if self._metadata_manager is None:
            self._metadata_manager = DuckDBManager(self.metadata_path)
        
        return self._metadata_manager
    
    def save_stock_data_sharded(self, stock_data: Dict[str, Any]) -> bool:
        """
        주식 데이터를 적절한 샤드에 저장
        
        Args:
            stock_data: 주식 데이터 딕셔너리
            
        Returns:
            저장 성공 여부
        """
        symbol = stock_data['symbol']
        shard_number = self.get_shard_number(symbol)
        shard_manager = self.get_shard_manager(shard_number)
        
        try:
            shard_manager.save_stock_data(stock_data)
            
            # 메타데이터에 샤드 정보 기록
            self._record_symbol_shard(symbol, shard_number)
            
            return True
        except Exception as e:
            print(f"❌ 샤드 저장 실패 ({symbol} -> 샤드 {shard_number}): {e}")
            return False
    
    def save_stock_data_batch_sharded(self, batch_data: List[Dict[str, Any]]) -> int:
        """
        배치 데이터를 샤드별로 분산 저장
        
        Args:
            batch_data: 주식 데이터 배치
            
        Returns:
            저장된 레코드 수
        """
        # 샤드별로 데이터 분류
        shard_batches = {i: [] for i in range(1, self.shard_count + 1)}
        
        for stock_data in batch_data:
            symbol = stock_data['symbol']
            shard_number = self.get_shard_number(symbol)
            shard_batches[shard_number].append(stock_data)
        
        total_saved = 0
        
        # 각 샤드에 배치 저장
        for shard_number, shard_data in shard_batches.items():
            if shard_data:  # 데이터가 있는 샤드만 처리
                try:
                    shard_manager = self.get_shard_manager(shard_number)
                    saved_count = shard_manager.save_stock_data_batch(shard_data)
                    total_saved += saved_count
                    
                    print(f"📊 샤드 {shard_number}: {len(shard_data)}개 데이터 저장 완료")
                    
                    # 메타데이터 기록
                    for stock_data in shard_data:
                        self._record_symbol_shard(stock_data['symbol'], shard_number)
                        
                except Exception as e:
                    print(f"❌ 샤드 {shard_number} 배치 저장 실패: {e}")
        
        return total_saved
    
    def _record_symbol_shard(self, symbol: str, shard_number: int):
        """
        심볼의 샤드 정보를 메타데이터에 기록
        
        Args:
            symbol: 종목 심볼
            shard_number: 샤드 번호
        """
        try:
            metadata_manager = self.get_metadata_manager()
            
            # 테이블 생성 (존재하지 않는 경우)
            metadata_manager.execute_query("""
                CREATE TABLE IF NOT EXISTS symbol_shards (
                    symbol VARCHAR PRIMARY KEY,
                    shard_number INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # UPSERT (INSERT OR REPLACE)
            metadata_manager.execute_query("""
                INSERT OR REPLACE INTO symbol_shards (symbol, shard_number, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (symbol, shard_number))
            
        except Exception as e:
            print(f"⚠️ 메타데이터 기록 오류 ({symbol} -> 샤드 {shard_number}): {e}")
    
    def get_symbol_shard(self, symbol: str) -> int:
        """
        심볼이 저장된 샤드 번호 반환 (메타데이터에서 조회)
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            샤드 번호 (없으면 해시로 계산)
        """
        try:
            metadata_manager = self.get_metadata_manager()
            result = metadata_manager.execute_query(
                "SELECT shard_number FROM symbol_shards WHERE symbol = ?",
                (symbol,)
            )
            
            if not result.empty:
                return result.iloc[0]['shard_number']
            else:
                # 메타데이터에 없으면 해시로 계산
                return self.get_shard_number(symbol)
                
        except Exception as e:
            print(f"⚠️ 샤드 조회 오류 ({symbol}): {e}")
            # 오류 시 해시로 계산
            return self.get_shard_number(symbol)
    
    def get_all_symbols_from_shards(self) -> List[str]:
        """
        모든 샤드에서 심볼 목록 수집
        
        Returns:
            전체 심볼 목록
        """
        all_symbols = []
        
        for shard_number in range(1, self.shard_count + 1):
            try:
                shard_manager = self.get_shard_manager(shard_number)
                symbols = shard_manager.get_active_symbols()
                all_symbols.extend(symbols)
                print(f"📋 샤드 {shard_number}: {len(symbols)}개 심볼")
            except Exception as e:
                print(f"⚠️ 샤드 {shard_number} 심볼 조회 오류: {e}")
        
        # 중복 제거
        unique_symbols = list(set(all_symbols))
        print(f"🎯 전체 고유 심볼: {len(unique_symbols)}개")
        
        return unique_symbols
    
    def get_shard_statistics(self) -> Dict[str, Any]:
        """
        각 샤드의 통계 정보 반환
        
        Returns:
            샤드별 통계 딕셔너리
        """
        stats = {}
        
        for shard_number in range(1, self.shard_count + 1):
            try:
                shard_manager = self.get_shard_manager(shard_number)
                
                # 심볼 수
                symbols = shard_manager.get_active_symbols()
                symbol_count = len(symbols)
                
                # 총 레코드 수
                record_result = shard_manager.execute_query("SELECT COUNT(*) as count FROM stock_data")
                record_count = record_result.iloc[0]['count'] if not record_result.empty else 0
                
                stats[f"shard_{shard_number}"] = {
                    'symbol_count': symbol_count,
                    'record_count': record_count,
                    'db_path': self.shard_paths[shard_number]
                }
                
            except Exception as e:
                stats[f"shard_{shard_number}"] = {
                    'error': str(e),
                    'db_path': self.shard_paths[shard_number]
                }
        
        return stats
    
    def test_hash_distribution(self, symbols: List[str]) -> Dict[int, int]:
        """
        해시 함수의 분산 품질 테스트
        
        Args:
            symbols: 테스트할 심볼 목록
            
        Returns:
            샤드별 심볼 수 딕셔너리
        """
        distribution = {i: 0 for i in range(1, self.shard_count + 1)}
        
        for symbol in symbols:
            shard_number = self.get_shard_number(symbol)
            distribution[shard_number] += 1
        
        print(f"🔍 해시 분산 테스트 결과 ({len(symbols)}개 심볼):")
        for shard_number, count in distribution.items():
            percentage = (count / len(symbols)) * 100
            print(f"  샤드 {shard_number}: {count}개 ({percentage:.1f}%)")
        
        return distribution
    
    def close_all(self):
        """모든 DB 연결 정리"""
        # 샤드 매니저들 정리
        for manager in self._shard_managers.values():
            try:
                manager.close()
            except:
                pass
        
        # 특별 매니저들 정리
        if self._watchlist_manager:
            try:
                self._watchlist_manager.close()
            except:
                pass
        
        if self._metadata_manager:
            try:
                self._metadata_manager.close()
            except:
                pass
        
        print("🧹 모든 샤드 DB 연결 정리 완료")

# 테스트 함수
def test_sharding():
    """샤딩 시스템 테스트"""
    print("🧪 DB 샤딩 시스템 테스트 시작")
    
    # 테스트용 심볼들
    test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'ADBE', 'CRM']
    
    # 샤드 매니저 생성
    shard_manager = DBShardManager("/tmp/test_shards")
    
    # 해시 분산 테스트
    shard_manager.test_hash_distribution(test_symbols)
    
    # 각 심볼의 샤드 확인
    print("\n📍 심볼별 샤드 배정:")
    for symbol in test_symbols:
        shard_number = shard_manager.get_shard_number(symbol)
        print(f"  {symbol} -> 샤드 {shard_number}")
    
    # 정리
    shard_manager.close_all()
    print("✅ 샤딩 테스트 완료")

if __name__ == "__main__":
    test_sharding()
