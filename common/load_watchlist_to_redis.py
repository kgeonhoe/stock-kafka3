#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
관심종목 데이터를 DuckDB에서 Redis로 로딩하는 스크립트
"""

import sys
import os
from datetime import datetime, timedelta, date
import json

# 프로젝트 경로 추가
sys.path.append('/opt/airflow/common')

from database import DuckDBManager
from redis_client import RedisClient

class WatchlistDataLoader:
    """관심종목 데이터를 Redis에 로딩하는 클래스"""
    
    def __init__(self):
        self.db = DuckDBManager('/data/duckdb/stock_data_replica.db')
        self.redis = RedisClient()
    
    def load_watchlist_to_redis(self, days_back: int = 30):
        """
        최근 관심종목들의 히스토리컬 데이터를 Redis에 로딩
        
        Args:
            days_back: 과거 몇 일의 데이터를 로딩할지
        """
        print(f"🚀 관심종목 데이터를 Redis에 로딩 시작 (과거 {days_back}일)")
        
        try:
            # 최근 관심종목 목록 조회
            watchlist_query = """
                SELECT DISTINCT 
                    symbol,
                    name,
                    sector,
                    market_cap,
                    market_cap_tier
                FROM daily_watchlist 
                WHERE scan_date >= ?
                ORDER BY scan_date DESC, market_cap DESC
            """
            
            cutoff_date = date.today() - timedelta(days=7)  # 최근 7일 관심종목
            watchlist_df = self.db.execute_query(watchlist_query, (cutoff_date,))
            
            if watchlist_df.empty:
                print("⚠️ 관심종목이 없습니다.")
                return
            
            print(f"📋 총 {len(watchlist_df)}개 관심종목 발견")
            
            processed_count = 0
            
            # 각 관심종목에 대해 히스토리컬 데이터 로딩
            for _, watchlist_item in watchlist_df.iterrows():
                symbol = watchlist_item['symbol']
                
                try:
                    # 히스토리컬 데이터 조회 (과거 30일)
                    historical_query = """
                        SELECT 
                            date,
                            open,
                            high,
                            low,
                            close,
                            volume
                        FROM stock_data 
                        WHERE symbol = ? 
                        AND date >= ?
                        ORDER BY date DESC
                        LIMIT ?
                    """
                    
                    historical_date = date.today() - timedelta(days=days_back)
                    historical_df = self.db.execute_query(
                        historical_query, 
                        (symbol, historical_date, days_back)
                    )
                    
                    if not historical_df.empty:
                        # 데이터를 리스트로 변환
                        historical_data = []
                        for _, row in historical_df.iterrows():
                            historical_data.append({
                                'date': str(row['date']),
                                'open': float(row['open']),
                                'high': float(row['high']),
                                'low': float(row['low']),
                                'close': float(row['close']),
                                'volume': int(row['volume'])
                            })
                        
                        # 메타데이터 준비
                        metadata = {
                            'name': watchlist_item['name'],
                            'sector': watchlist_item['sector'],
                            'market_cap': float(watchlist_item['market_cap']) if watchlist_item['market_cap'] else 0,
                            'market_cap_tier': int(watchlist_item['market_cap_tier']),
                            'data_points': len(historical_data),
                            'date_range': {
                                'start': str(historical_df['date'].min()),
                                'end': str(historical_df['date'].max())
                            }
                        }
                        
                        # Redis에 저장
                        success = self.redis.set_watchlist_data(
                            symbol=symbol,
                            historical_data=historical_data,
                            metadata=metadata
                        )
                        
                        if success:
                            processed_count += 1
                            print(f"✅ {symbol} ({watchlist_item['name']}): {len(historical_data)}일 데이터 저장")
                        else:
                            print(f"❌ {symbol}: Redis 저장 실패")
                    else:
                        print(f"⚠️ {symbol}: 히스토리컬 데이터 없음")
                        
                except Exception as symbol_error:
                    print(f"💥 {symbol} 처리 중 오류: {symbol_error}")
                    continue
            
            print(f"\n🎉 완료! {processed_count}/{len(watchlist_df)}개 관심종목 데이터 Redis 로딩 완료")
            
            # Redis 상태 확인
            self.print_redis_status()
            
        except Exception as e:
            print(f"❌ 관심종목 데이터 로딩 중 오류: {e}")
        
        finally:
            self.db.close()
    
    def print_redis_status(self):
        """Redis 상태 출력"""
        try:
            # 관심종목 데이터 키 수 확인
            watchlist_keys = self.redis.redis_client.keys("watchlist_data:*")
            signal_keys = self.redis.redis_client.keys("signal_trigger:*")
            analysis_keys = self.redis.redis_client.keys("realtime_analysis:*")
            
            print(f"\n📊 Redis 상태:")
            print(f"   🎯 관심종목 데이터: {len(watchlist_keys)}개")
            print(f"   📈 활성 신호: {len(signal_keys)}개")
            print(f"   🔍 실시간 분석: {len(analysis_keys)}개")
            
            # 메모리 사용량
            info = self.redis.redis_client.info()
            print(f"   💾 Redis 메모리: {info.get('used_memory_human', 'N/A')}")
            
        except Exception as e:
            print(f"⚠️ Redis 상태 확인 오류: {e}")

    def smart_incremental_update(self, force_full_reload: bool = False):
        """
        스마트 증분 업데이트: 변경된 데이터만 효율적으로 업데이트
        
        Args:
            force_full_reload: 강제로 전체 재로딩할지 여부
        """
        print("🧠 스마트 증분 업데이트 시작...")
        
        try:
            # 1. Redis에서 마지막 업데이트 정보 확인
            last_update_info = self.get_last_update_info()
            current_time = datetime.now()
            
            if force_full_reload or not last_update_info:
                print("🔄 전체 데이터 재로딩 모드")
                return self.load_watchlist_to_redis(days_back=30)
            
            last_update_date = datetime.fromisoformat(last_update_info['timestamp'])
            days_since_update = (current_time - last_update_date).days
            
            print(f"📅 마지막 업데이트: {last_update_date.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"⏰ 경과 시간: {days_since_update}일")
            
            # 2. 업데이트 필요성 판단
            if days_since_update == 0:
                print("✅ 오늘 이미 업데이트됨 - 신규 종목만 확인")
                return self.update_new_symbols_only()
            elif days_since_update >= 7:
                print("🔄 일주일 이상 경과 - 전체 재로딩")
                return self.load_watchlist_to_redis(days_back=30)
            else:
                print(f"⚡ 증분 업데이트 모드 - 최근 {days_since_update + 1}일 데이터")
                return self.incremental_update(days_since_update + 1)
                
        except Exception as e:
            print(f"❌ 스마트 업데이트 중 오류: {e}")
            print("🔄 안전 모드: 전체 재로딩 실행")
            return self.load_watchlist_to_redis(days_back=30)

    def get_last_update_info(self) -> dict:
        """Redis에서 마지막 업데이트 정보 조회"""
        try:
            update_info = self.redis.redis_client.get("watchlist_last_update")
            if update_info:
                return json.loads(update_info)
            return None
        except:
            return None

    def set_last_update_info(self, processed_symbols: int, total_symbols: int, mode: str = "incremental"):
        """마지막 업데이트 정보 저장"""
        update_info = {
            'timestamp': datetime.now().isoformat(),
            'processed_symbols': processed_symbols,
            'total_symbols': total_symbols,
            'success_rate': processed_symbols / total_symbols if total_symbols > 0 else 0,
            'mode': mode
        }
        
        self.redis.redis_client.setex(
            "watchlist_last_update", 
            86400 * 7,  # 7일 TTL
            json.dumps(update_info)
        )

    def incremental_update(self, days_to_update: int):
        """증분 업데이트: 최근 N일 데이터만 업데이트"""
        print(f"⚡ 증분 업데이트 모드: 최근 {days_to_update}일")
        
        try:
            # 1. 기존 관심종목 목록 조회 (Redis에서)
            existing_symbols = self.get_existing_watchlist_symbols()
            
            # 2. 현재 관심종목 목록 조회 (DB에서)
            current_symbols = self.get_current_watchlist_symbols()
            
            # 3. 변경 사항 분석
            new_symbols = set(current_symbols) - set(existing_symbols)
            removed_symbols = set(existing_symbols) - set(current_symbols)
            unchanged_symbols = set(existing_symbols) & set(current_symbols)
            
            print(f"📊 변경 사항 분석:")
            print(f"   🆕 신규 추가: {len(new_symbols)}개")
            print(f"   🗑️ 제거된 종목: {len(removed_symbols)}개") 
            print(f"   🔄 업데이트 대상: {len(unchanged_symbols)}개")
            
            processed_count = 0
            
            # 4. 제거된 종목 처리
            for symbol in removed_symbols:
                self.redis.redis_client.delete(f"watchlist_data:{symbol}")
                print(f"🗑️ {symbol} 제거됨")
            
            # 5. 신규 종목 전체 데이터 로딩
            for symbol in new_symbols:
                if self.load_single_symbol_full(symbol):
                    processed_count += 1
                    print(f"🆕 {symbol} 신규 추가 (30일 데이터)")
            
            # 6. 기존 종목 증분 업데이트 (최근 N일만)
            for symbol in unchanged_symbols:
                if self.update_single_symbol_incremental(symbol, days_to_update):
                    processed_count += 1
                    print(f"⚡ {symbol} 증분 업데이트 ({days_to_update}일)")
            
            total_symbols = len(new_symbols) + len(unchanged_symbols)
            print(f"\n🎉 증분 업데이트 완료! {processed_count}/{total_symbols}개 성공")
            
            # 7. 업데이트 정보 저장
            self.set_last_update_info(processed_count, total_symbols, "incremental")
            self.print_redis_status()
            
            return True
            
        except Exception as e:
            print(f"❌ 증분 업데이트 중 오류: {e}")
            return False

    def get_existing_watchlist_symbols(self) -> list:
        """Redis에서 기존 관심종목 심볼 목록 조회"""
        try:
            keys = self.redis.redis_client.keys("watchlist_data:*")
            return [key.decode('utf-8').split(':')[1] for key in keys]
        except:
            return []

    def get_current_watchlist_symbols(self) -> list:
        """DB에서 현재 관심종목 심볼 목록 조회"""
        try:
            query = """
                SELECT DISTINCT symbol 
                FROM daily_watchlist 
                WHERE scan_date >= ?
            """
            cutoff_date = date.today() - timedelta(days=7)
            df = self.db.execute_query(query, (cutoff_date,))
            return df['symbol'].tolist() if not df.empty else []
        except:
            return []

    def update_single_symbol_incremental(self, symbol: str, days_back: int) -> bool:
        """단일 종목 증분 업데이트"""
        try:
            # 기존 데이터 조회
            existing_data = self.redis.get_watchlist_data(symbol)
            if not existing_data:
                # 기존 데이터 없으면 전체 로딩
                return self.load_single_symbol_full(symbol)
            
            # 최신 날짜 확인
            historical_data = existing_data.get('historical_data', [])
            if not historical_data:
                return self.load_single_symbol_full(symbol)
            
            latest_date_str = max(item['date'] for item in historical_data)
            latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d').date()
            
            # 최신 날짜 이후 데이터만 조회
            new_data_query = """
                SELECT date, open, high, low, close, volume
                FROM stock_data 
                WHERE symbol = ? AND date > ?
                ORDER BY date DESC
            """
            
            new_df = self.db.execute_query(new_data_query, (symbol, latest_date))
            
            if new_df.empty:
                print(f"   📅 {symbol}: 새로운 데이터 없음")
                return True
            
            # 새 데이터 추가
            new_records = []
            for _, row in new_df.iterrows():
                new_records.append({
                    'date': str(row['date']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row['volume'])
                })
            
            # 기존 데이터에 새 데이터 병합 (중복 제거)
            all_data = historical_data + new_records
            
            # 날짜 기준 정렬 및 중복 제거
            unique_data = {}
            for item in all_data:
                unique_data[item['date']] = item
            
            updated_data = sorted(unique_data.values(), key=lambda x: x['date'], reverse=True)
            
            # 최근 30일만 유지
            if len(updated_data) > 30:
                updated_data = updated_data[:30]
            
            # 메타데이터 업데이트
            metadata = existing_data.get('metadata', {})
            metadata.update({
                'data_points': len(updated_data),
                'last_update': datetime.now().isoformat(),
                'new_records_added': len(new_records)
            })
            
            # Redis 업데이트
            return self.redis.set_watchlist_data(symbol, updated_data, metadata)
            
        except Exception as e:
            print(f"❌ {symbol} 증분 업데이트 오류: {e}")
            return False

    def load_single_symbol_full(self, symbol: str) -> bool:
        """단일 종목 전체 데이터 로딩"""
        try:
            # 메타데이터 조회
            meta_query = """
                SELECT DISTINCT name, sector, market_cap, market_cap_tier
                FROM daily_watchlist 
                WHERE symbol = ?
                ORDER BY scan_date DESC
                LIMIT 1
            """
            meta_df = self.db.execute_query(meta_query, (symbol,))
            
            if meta_df.empty:
                print(f"⚠️ {symbol}: 메타데이터 없음")
                return False
            
            # 히스토리컬 데이터 조회 (30일)
            historical_query = """
                SELECT date, open, high, low, close, volume
                FROM stock_data 
                WHERE symbol = ? AND date >= ?
                ORDER BY date DESC
                LIMIT 30
            """
            
            historical_date = date.today() - timedelta(days=30)
            historical_df = self.db.execute_query(historical_query, (symbol, historical_date))
            
            if historical_df.empty:
                print(f"⚠️ {symbol}: 히스토리컬 데이터 없음")
                return False
            
            # 데이터 변환
            historical_data = []
            for _, row in historical_df.iterrows():
                historical_data.append({
                    'date': str(row['date']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row['volume'])
                })
            
            # 메타데이터 준비
            meta_row = meta_df.iloc[0]
            metadata = {
                'name': meta_row['name'],
                'sector': meta_row['sector'],
                'market_cap': float(meta_row['market_cap']) if meta_row['market_cap'] else 0,
                'market_cap_tier': int(meta_row['market_cap_tier']),
                'data_points': len(historical_data),
                'last_update': datetime.now().isoformat(),
                'load_type': 'full'
            }
            
            # Redis 저장
            return self.redis.set_watchlist_data(symbol, historical_data, metadata)
            
        except Exception as e:
            print(f"❌ {symbol} 전체 로딩 오류: {e}")
            return False

    def update_new_symbols_only(self):
        """오늘 이미 업데이트된 경우 - 신규 종목만 확인"""
        print("🔍 신규 종목 확인 모드")
        
        existing_symbols = set(self.get_existing_watchlist_symbols())
        current_symbols = set(self.get_current_watchlist_symbols())
        new_symbols = current_symbols - existing_symbols
        
        if not new_symbols:
            print("✅ 신규 종목 없음 - 업데이트 불필요")
            return True
        
        print(f"🆕 신규 종목 {len(new_symbols)}개 발견")
        
        processed_count = 0
        for symbol in new_symbols:
            if self.load_single_symbol_full(symbol):
                processed_count += 1
                print(f"🆕 {symbol} 추가 완료")
        
        print(f"🎉 신규 종목 추가 완료: {processed_count}/{len(new_symbols)}개")
        self.set_last_update_info(processed_count, len(new_symbols), "new_symbols_only")
        self.print_redis_status()
        return True

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='관심종목 데이터 Redis 로더')
    parser.add_argument('--mode', choices=['smart', 'full', 'incremental'], 
                       default='smart', help='업데이트 모드 선택')
    parser.add_argument('--days', type=int, default=30, 
                       help='로딩할 과거 일수')
    parser.add_argument('--force', action='store_true', 
                       help='강제 전체 재로딩')
    
    try:
        args = parser.parse_args()
    except:
        # argparse 에러 시 기본값 사용
        class DefaultArgs:
            mode = 'smart'
            days = 30
            force = False
        args = DefaultArgs()
    
    print("=" * 60)
    print("📈 Stock Watchlist Data Loader")
    print("=" * 60)
    
    loader = WatchlistDataLoader()
    
    if args.mode == 'smart':
        print("🧠 스마트 모드 선택")
        loader.smart_incremental_update(force_full_reload=args.force)
    elif args.mode == 'full':
        print(f"🔄 전체 로딩 모드 ({args.days}일)")
        loader.load_watchlist_to_redis(days_back=args.days)
        loader.set_last_update_info(1, 1, "full")  # 업데이트 정보 저장
    elif args.mode == 'incremental':
        print(f"⚡ 증분 모드 ({args.days}일)")
        loader.incremental_update(args.days)

if __name__ == "__main__":
    main()
