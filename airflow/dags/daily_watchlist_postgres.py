#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import sys
import os

# 환경에 따라 모듈 경로 설정
if '/opt/airflow/common' not in sys.path:
    sys.path.insert(0, '/opt/airflow/common')
if '/home/grey1/stock-kafka3/common' not in sys.path:
    sys.path.insert(0, '/home/grey1/stock-kafka3/common')

from technical_scanner_postgres import TechnicalScannerPostgreSQL
import pytz

def market_aware_setup(**context):
    """시장 시간대별 스캔 강도 결정"""
    
    # 현재 미국 동부 시간 (NYSE/NASDAQ 기준)
    est = pytz.timezone('US/Eastern')
    current_est = datetime.now(est)
    current_hour = current_est.hour
    current_minute = current_est.minute
    weekday = current_est.weekday()  # 0=월요일, 6=일요일
    
    # 주말은 LOW 강도로 고정
    if weekday >= 5:  # 토요일, 일요일
        scan_intensity = 'LOW'
        conditions = ['bollinger_bands']
        reason = "주말 - 최소 모니터링"
    else:
        # 평일 시간대별 분류
        if 9 <= current_hour < 16:  # 09:30-16:00 정규 거래시간
            if current_hour == 9 and current_minute < 30:
                # 9:00-9:30은 프리마켓 마지막
                scan_intensity = 'MEDIUM'
                conditions = ['bollinger_bands', 'volume_spike']
                reason = "프리마켓 마지막 30분"
            else:
                # 정규 거래시간
                scan_intensity = 'HIGH'
                conditions = ['bollinger_bands', 'rsi_oversold', 'macd_bullish', 'volume_spike']
                reason = "정규 거래시간 - 최고 활동성"
                
        elif 4 <= current_hour < 9 or 16 <= current_hour <= 20:
            # 프리마켓(4:00-9:30) 또는 애프터마켓(16:00-20:00)
            scan_intensity = 'MEDIUM'
            conditions = ['bollinger_bands', 'volume_spike']
            reason = "확장 거래시간 - 중간 모니터링"
            
        else:  # 20:00-04:00 시장 완전 마감
            scan_intensity = 'LOW'
            conditions = ['bollinger_bands']
            reason = "시장 마감 - 최소 모니터링"
    
    print(f"🕐 현재 EST: {current_est.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⚡ 스캔 강도: {scan_intensity}")
    print(f"🎯 활성 조건: {conditions}")
    print(f"💡 이유: {reason}")
    
    # XCom으로 후속 태스크에 전달
    context['task_instance'].xcom_push(key='scan_conditions', value=conditions)
    context['task_instance'].xcom_push(key='scan_intensity', value=scan_intensity)
    context['task_instance'].xcom_push(key='scan_reason', value=reason)
    
    return f"✅ {scan_intensity} 강도 설정: {len(conditions)}개 조건"

def scan_and_update_watchlist(**context):
    """시장 상황에 따른 동적 볼린저 밴드 스캔 (PostgreSQL)"""
    
    # 이전 태스크에서 스캔 조건 가져오기
    scan_conditions = context['task_instance'].xcom_pull(
        task_ids='market_aware_setup', 
        key='scan_conditions'
    ) or ['bollinger_bands']  # 기본값
    
    scan_intensity = context['task_instance'].xcom_pull(
        task_ids='market_aware_setup', 
        key='scan_intensity'
    ) or 'MEDIUM'
    
    # 볼린저 밴드 조건이 포함된 경우에만 실행
    if 'bollinger_bands' not in scan_conditions:
        print(f"⏭️ {scan_intensity} 강도에서 볼린저 밴드 스캔 제외됨")
        context['task_instance'].xcom_push(key='watchlist_count', value=0)
        context['task_instance'].xcom_push(key='scan_date', value=str((datetime.now() - timedelta(days=1)).date()))
        return f"✅ 스캔 스킵 ({scan_intensity})"
    
    # 스캔 날짜 (어제 날짜 사용 - 장마감 후 처리)
    scan_date = (datetime.now() - timedelta(days=1)).date()
    
    try:
        # PostgreSQL 기술적 스캐너 초기화
        scanner = TechnicalScannerPostgreSQL()
        
        # 강도별 스캔 파라미터 조정
        if scan_intensity == 'HIGH':
            # 정규 거래시간 - 더 민감한 조건
            threshold = 0.995  # 볼린저 상단의 99.5% 터치
            limit = 50         # 최대 50개 종목
        elif scan_intensity == 'MEDIUM':
            # 확장 거래시간 - 적당한 조건  
            threshold = 0.99   # 볼린저 상단의 99% 터치
            limit = 30         # 최대 30개 종목
        else:  # LOW
            # 시장 마감 시간 - 보수적 조건
            threshold = 0.985  # 볼린저 상단의 98.5% 터치  
            limit = 20         # 최대 20개 종목
        
        print(f"🎯 {scan_intensity} 강도 스캔: 임계값={threshold:.3f}, 최대={limit}개")
        
        # 볼린저 밴드 상단 터치 종목 스캔
        bb_signals = scanner.scan_bollinger_band_signals(scan_date)
        
        # 강도별 필터링 적용
        filtered_signals = []
        for signal in bb_signals:
            # 임계값 조건 확인 (상단선 대비 비율)
            if signal.get('condition_value', 0) >= threshold:
                filtered_signals.append(signal)
        
        # 상위 N개로 제한
        watchlist_signals = filtered_signals[:limit]
        
        # 필터링된 신호들을 데이터베이스에 저장
        if watchlist_signals:
            try:
                with scanner.db.get_connection() as conn:
                    with conn.cursor() as cur:
                        for signal in watchlist_signals:
                            cur.execute("""
                                INSERT INTO daily_watchlist 
                                (symbol, date, condition_type, condition_value, market_cap_tier)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (symbol, date, condition_type) DO UPDATE SET
                                    condition_value = EXCLUDED.condition_value,
                                    market_cap_tier = EXCLUDED.market_cap_tier
                            """, (
                                signal['symbol'],
                                signal['date'], 
                                signal['condition_type'],
                                signal['condition_value'],
                                signal.get('market_cap_tier', 3)
                            ))
                        conn.commit()
                        print(f"💾 {len(watchlist_signals)}개 신호 DB 저장 완료")
            except Exception as db_error:
                print(f"❌ DB 저장 오류: {db_error}")
                raise
        
        print(f"📈 {scan_date} 볼린저 밴드 상단 터치 종목: {len(watchlist_signals)}개")
        
        # 결과 상세 출력
        for signal in watchlist_signals[:10]:  # 상위 10개만 출력
            print(f"  - {signal['symbol']}: ${signal['close_price']:.2f} (상단선 대비 {signal['condition_value']:.3f})")
        
        # XCom에 결과 저장
        context['task_instance'].xcom_push(key='watchlist_count', value=len(watchlist_signals))
        context['task_instance'].xcom_push(key='scan_date', value=str(scan_date))
        
        scanner.close()
        return f"✅ {scan_intensity} 강도 스캔 완료: {len(watchlist_signals)}개"
        
    except Exception as e:
        print(f"❌ 관심종목 스캔 실패: {str(e)}")
        raise

def scan_rsi_oversold(**context):
    """시장 강도에 따른 RSI 과매도 스캔 (PostgreSQL)"""
    
    # 시장 강도 확인
    scan_conditions = context['task_instance'].xcom_pull(
        task_ids='market_aware_setup', 
        key='scan_conditions'
    ) or []
    
    scan_intensity = context['task_instance'].xcom_pull(
        task_ids='market_aware_setup', 
        key='scan_intensity'
    ) or 'MEDIUM'
    
    # RSI 스캔이 활성화된 경우에만 실행 (HIGH 강도에만)
    if 'rsi_oversold' not in scan_conditions:
        print(f"⏭️ {scan_intensity} 강도에서 RSI 스캔 제외됨")
        context['task_instance'].xcom_push(key='rsi_count', value=0)
        return f"✅ RSI 스캔 스킵 ({scan_intensity})"
    
    scan_date = (datetime.now() - timedelta(days=1)).date()
    
    try:
        scanner = TechnicalScannerPostgreSQL()
        
        # HIGH 강도에서만 실행되므로 더 적극적인 파라미터
        rsi_threshold = 35    # HIGH 강도에서는 RSI 35 이하까지 확장
        limit = 25           # 최대 25개 종목
        
        print(f"🎯 {scan_intensity} RSI 스캔: RSI≤{rsi_threshold}, 최대={limit}개")
        
        # RSI 과매도 신호 스캔
        all_rsi_signals = scanner.scan_rsi_oversold_signals(scan_date)
        
        # 강도별 필터링 적용
        filtered_signals = []
        for signal in all_rsi_signals:
            # RSI 임계값 조건 확인
            if signal.get('rsi', 100) <= rsi_threshold:
                filtered_signals.append(signal)
        
        # 상위 N개로 제한
        rsi_signals = filtered_signals[:limit]
        
        # 필터링된 신호들을 데이터베이스에 저장
        if rsi_signals:
            try:
                with scanner.db.get_connection() as conn:
                    with conn.cursor() as cur:
                        for signal in rsi_signals:
                            cur.execute("""
                                INSERT INTO daily_watchlist 
                                (symbol, date, condition_type, condition_value, market_cap_tier)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (symbol, date, condition_type) DO UPDATE SET
                                    condition_value = EXCLUDED.condition_value,
                                    market_cap_tier = EXCLUDED.market_cap_tier
                            """, (
                                signal['symbol'],
                                signal['date'], 
                                signal['condition_type'],
                                signal['condition_value'],
                                signal.get('market_cap_tier', 3)
                            ))
                        conn.commit()
                        print(f"💾 {len(rsi_signals)}개 RSI 신호 DB 저장 완료")
            except Exception as db_error:
                print(f"❌ RSI 신호 DB 저장 오류: {db_error}")
                raise
        
        print(f"📉 {scan_date} RSI 과매도 종목: {len(rsi_signals)}개")
        
        # 결과 상세 출력
        for signal in rsi_signals[:5]:  # 상위 5개만 출력
            print(f"  - {signal['symbol']}: RSI {signal['rsi']:.1f} (${signal['close_price']:.2f})")
        
        context['task_instance'].xcom_push(key='rsi_count', value=len(rsi_signals))
        
        scanner.close()
        return f"✅ RSI 과매도 스캔 완료: {len(rsi_signals)}개"
        
    except Exception as e:
        print(f"❌ RSI 스캔 실패: {str(e)}")
        raise

def scan_macd_bullish(**context):
    """MACD 강세 종목 스캔 (PostgreSQL)"""
    
    scan_date = (datetime.now() - timedelta(days=1)).date()
    
    try:
        scanner = TechnicalScannerPostgreSQL()
        
        # MACD 강세 신호 스캔
        macd_signals = scanner.scan_macd_bullish_signals(scan_date)
        
        print(f"📈 {scan_date} MACD 강세 종목: {len(macd_signals)}개")
        
        # 결과 상세 출력
        for signal in macd_signals[:5]:  # 상위 5개만 출력
            hist = signal['condition_value']
            print(f"  - {signal['symbol']}: MACD 히스토그램 {hist:.4f} (${signal['close_price']:.2f})")
        
        context['task_instance'].xcom_push(key='macd_count', value=len(macd_signals))
        
        scanner.close()
        return f"✅ MACD 강세 스캔 완료: {len(macd_signals)}개"
        
    except Exception as e:
        print(f"❌ MACD 스캔 실패: {str(e)}")
        raise

def cleanup_old_watchlist(**context):
    """30일 이전 관심종목 데이터 정리 (PostgreSQL)"""
    
    try:
        scanner = TechnicalScannerPostgreSQL()
        
        # 30일 이전 데이터 삭제
        cutoff_date = (datetime.now() - timedelta(days=30)).date()
        
        deleted_count = 0
        try:
            with scanner.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM daily_watchlist 
                        WHERE date < %s
                    """, (cutoff_date,))
                    deleted_count = cur.rowcount
                    conn.commit()
        except Exception as e:
            print(f"⚠️ 테이블이 존재하지 않거나 삭제 실패: {e}")
            deleted_count = 0
        
        print(f"🧹 {cutoff_date} 이전 관심종목 데이터 {deleted_count}개 삭제")
        
        scanner.close()
        return f"✅ 오래된 데이터 정리 완료: {deleted_count}개 삭제"
        
    except Exception as e:
        print(f"❌ 데이터 정리 실패: {str(e)}")
        raise

def get_top_performers(**context):
    """상위 성과 종목 조회 (PostgreSQL)"""
    
    scan_date = (datetime.now() - timedelta(days=1)).date()
    
    try:
        scanner = TechnicalScannerPostgreSQL()
        
        # 상위 성과 종목 조회
        performers = scanner.get_top_performers(scan_date, limit=10)
        
        print(f"🏆 {scan_date} 상위 성과 종목:")
        for perf in performers:
            change = perf.get('change_percent', 0)
            print(f"  - {perf['symbol']}: {change:+.2f}% (${perf['close_price']:.2f})")
        
        context['task_instance'].xcom_push(key='top_performers', value=len(performers))
        
        scanner.close()
        return f"✅ 상위 성과 조회 완료: {len(performers)}개"
        
    except Exception as e:
        print(f"❌ 상위 성과 조회 실패: {str(e)}")
        raise

def send_watchlist_summary(**context):
    """관심종목 요약 정보 출력 (PostgreSQL)"""
    
    # 이전 태스크에서 결과 가져오기
    watchlist_count = context['task_instance'].xcom_pull(task_ids='scan_bollinger_bands', key='watchlist_count') or 0
    rsi_count = context['task_instance'].xcom_pull(task_ids='scan_rsi_oversold', key='rsi_count') or 0
    macd_count = context['task_instance'].xcom_pull(task_ids='scan_macd_bullish', key='macd_count') or 0
    top_performers = context['task_instance'].xcom_pull(task_ids='get_top_performers', key='top_performers') or 0
    scan_date = context['task_instance'].xcom_pull(task_ids='scan_bollinger_bands', key='scan_date')
    
    print(f"""
    📊 일별 관심종목 스캔 결과 ({scan_date}) - PostgreSQL
    ================================
    🎯 볼린저 밴드 상단 터치: {watchlist_count}개
    📉 RSI 과매도 신호: {rsi_count}개
    📈 MACD 강세 신호: {macd_count}개
    🏆 상위 성과 종목: {top_performers}개
    📅 스캔 날짜: {scan_date}
    ⏰ 처리 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    💾 데이터베이스: PostgreSQL
    """)
    
    return f"요약 전송 완료"

def sync_watchlist_to_redis(**context):
    """PostgreSQL 관심종목을 Redis로 동기화"""
    try:
        from database import PostgreSQLManager
        import redis
        import json
        from datetime import date, timedelta
        
        print("🚀 PostgreSQL → Redis 관심종목 동기화 시작...")
        
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        # Redis 연결 (다양한 주소 시도)
        redis_client = None
        redis_hosts = [
            {'host': 'redis', 'port': 6379},      # Docker 컨테이너명
            {'host': 'localhost', 'port': 6379},   # 로컬호스트
            {'host': '127.0.0.1', 'port': 6379},   # IP 주소
        ]
        
        for redis_config in redis_hosts:
            try:
                test_client = redis.Redis(
                    host=redis_config['host'],
                    port=redis_config['port'],
                    db=0,
                    decode_responses=True,
                    socket_connect_timeout=5,  # 5초 타임아웃
                    socket_timeout=5
                )
                # 연결 테스트
                test_client.ping()
                redis_client = test_client
                print(f"✅ Redis 연결 성공: {redis_config['host']}:{redis_config['port']}")
                break
            except Exception as e:
                print(f"⚠️ Redis 연결 실패 ({redis_config['host']}:{redis_config['port']}): {e}")
                continue
        
        if redis_client is None:
            print("⚠️ Redis 서버에 연결할 수 없습니다. PostgreSQL만 사용합니다.")
            # Redis 없이도 계속 진행
            redis_client = None
        
        # 최근 3일간의 관심종목 조회 (배치 누락 대비)
        today = date.today()
        start_date = today - timedelta(days=3)
        
        query = """
            SELECT DISTINCT 
                symbol, 
                date, 
                condition_type, 
                condition_value,
                created_at
            FROM daily_watchlist 
            WHERE date >= %s
            ORDER BY date DESC, symbol
        """
        
        redis_updated = 0
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (start_date,))
                results = cur.fetchall()
                
                print(f"📊 PostgreSQL에서 조회된 관심종목: {len(results)}개")
                
                # 날짜별로 그룹화
                watchlist_by_date = {}
                for row in results:
                    symbol, date_val, condition_type, condition_value, created_at = row
                    date_str = date_val.strftime('%Y-%m-%d')
                    
                    if date_str not in watchlist_by_date:
                        watchlist_by_date[date_str] = {}
                    
                    if symbol not in watchlist_by_date[date_str]:
                        watchlist_by_date[date_str][symbol] = {
                            'symbol': symbol,
                            'conditions': [],
                            'date': date_str,
                            'last_updated': created_at.isoformat() if created_at else None
                        }
                    
                    watchlist_by_date[date_str][symbol]['conditions'].append({
                        'type': condition_type,
                        'value': float(condition_value)
                    })
        
        # Redis에 저장 (Redis가 있는 경우에만)
        redis_updated = 0
        if redis_client is not None:
            for date_str, symbols_data in watchlist_by_date.items():
                try:
                    # 날짜별 관심종목 키
                    redis_key = f"watchlist:{date_str}"
                    
                    # 기존 Redis 데이터와 비교
                    existing_data = redis_client.get(redis_key)
                    existing_symbols = set()
                    
                    if existing_data:
                        try:
                            existing_dict = json.loads(existing_data)
                            existing_symbols = set(existing_dict.keys())
                        except:
                            pass
                    
                    # 새로운 심볼이 있는지 확인
                    new_symbols = set(symbols_data.keys()) - existing_symbols
                    
                    if new_symbols or not existing_data:
                        # Redis에 저장 (JSON 형태)
                        redis_data = json.dumps(symbols_data, ensure_ascii=False, indent=2)
                        redis_client.setex(redis_key, 86400 * 7, redis_data)  # 7일 TTL
                        
                        redis_updated += 1
                        print(f"✅ Redis 업데이트: {date_str} - {len(symbols_data)}개 심볼")
                        
                        if new_symbols:
                            print(f"   🆕 새로운 심볼: {', '.join(list(new_symbols)[:5])}{'...' if len(new_symbols) > 5 else ''}")
                    else:
                        print(f"⏭️  Redis 스킵: {date_str} - 변경사항 없음")
                    
                    # 최신 관심종목 (오늘) 별도 키로 저장
                    if date_str == today.strftime('%Y-%m-%d'):
                        latest_key = "watchlist:latest"
                        redis_client.setex(latest_key, 86400, redis_data)  # 1일 TTL
                        print(f"✅ 최신 관심종목 업데이트: {len(symbols_data)}개 심볼")
                    
                except Exception as e:
                    print(f"❌ Redis 저장 오류 ({date_str}): {e}")
        else:
            print("⚠️ Redis 미사용 - PostgreSQL에만 저장됨")
        
        # 통계 정보 Redis에 저장 (Redis가 있는 경우에만)
        stats = {
            'total_dates': len(watchlist_by_date),
            'total_symbols': sum(len(symbols) for symbols in watchlist_by_date.values()),
            'last_sync': datetime.now().isoformat(),
            'redis_updates': redis_updated,
            'redis_available': redis_client is not None
        }
        
        if redis_client is not None:
            try:
                redis_client.setex("watchlist:stats", 3600, json.dumps(stats))  # 1시간 TTL
                print("✅ Redis 통계 저장 완료")
            except Exception as e:
                print(f"⚠️ Redis 통계 저장 실패: {e}")
        
        print(f"✅ PostgreSQL → Redis 동기화 완료:")
        print(f"  - 처리된 날짜: {len(watchlist_by_date)}개")
        print(f"  - 총 심볼 수: {stats['total_symbols']}개")
        print(f"  - Redis 업데이트: {redis_updated}개")
        print(f"  - Redis 사용 가능: {'예' if redis_client else '아니오'}")
        
        # XCom에 결과 저장
        context['task_instance'].xcom_push(key='redis_sync_count', value=redis_updated)
        context['task_instance'].xcom_push(key='total_symbols', value=stats['total_symbols'])
        
        db.close()
        if redis_client is not None:
            redis_client.close()
        
        return f"✅ 동기화 완료: Redis {redis_updated}개 업데이트, PostgreSQL {stats['total_symbols']}개 심볼"
        
    except Exception as e:
        print(f"❌ 동기화 실패: {e}")
        import traceback
        traceback.print_exc()
        raise

def generate_additional_watchlist(**context):
    """추가 관심종목 생성 (배치 누락 대비)"""
    try:
        from database import PostgreSQLManager
        from datetime import date, timedelta
        
        print("🚀 추가 관심종목 생성 시작...")
        
        # PostgreSQL 연결
        db = PostgreSQLManager()
        print("✅ PostgreSQL 연결 성공")
        
        today = date.today()
        
        # 오늘 관심종목이 있는지 확인
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM daily_watchlist 
                    WHERE date = %s
                """, (today,))
                
                today_count = cur.fetchone()[0]
                
                if today_count > 0:
                    print(f"📅 오늘 관심종목이 이미 있습니다: {today_count}개")
                    context['task_instance'].xcom_push(key='additional_generated', value=0)
                    db.close()
                    return f"✅ 이미 존재: {today_count}개"
        
        print("🔍 오늘 관심종목이 없습니다. 추가 생성을 시도합니다...")
        
        # 실시간 관심종목 조건들
        realtime_conditions = [
            {
                'name': 'price_momentum',
                'query': """
                    SELECT DISTINCT s.symbol, s.close, 
                           (s.close - prev.close) / prev.close * 100 as price_change
                    FROM stock_data s
                    JOIN stock_data prev ON s.symbol = prev.symbol 
                        AND prev.date = s.date - INTERVAL '1 day'
                    WHERE s.date >= CURRENT_DATE - INTERVAL '2 days'
                      AND s.close > 5  -- 최소 가격 필터
                      AND s.volume > 100000  -- 최소 거래량
                      AND ABS((s.close - prev.close) / prev.close * 100) > 3  -- 3% 이상 변화
                    ORDER BY ABS((s.close - prev.close) / prev.close) DESC
                    LIMIT 30
                """
            },
            {
                'name': 'volume_breakout',
                'query': """
                    SELECT DISTINCT s1.symbol, s1.close, s1.volume
                    FROM stock_data s1
                    JOIN stock_data s2 ON s1.symbol = s2.symbol 
                        AND s2.date BETWEEN s1.date - INTERVAL '10 days' AND s1.date - INTERVAL '1 day'
                    WHERE s1.date >= CURRENT_DATE - INTERVAL '2 days'
                      AND s1.volume > 50000
                    GROUP BY s1.symbol, s1.close, s1.volume
                    HAVING s1.volume > AVG(s2.volume) * 1.5  -- 1.5배 이상
                    ORDER BY s1.volume / AVG(s2.volume) DESC
                    LIMIT 25
                """
            }
        ]
        
        total_added = 0
        
        for condition in realtime_conditions:
            try:
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(condition['query'])
                        results = cur.fetchall()
                        
                        added_count = 0
                        for row in results:
                            try:
                                # 오늘 날짜로 추가
                                cur.execute("""
                                    INSERT INTO daily_watchlist (symbol, date, condition_type, condition_value)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (symbol, date, condition_type) DO NOTHING
                                """, (row[0], today, condition['name'], float(row[1])))
                                
                                if cur.rowcount > 0:
                                    added_count += 1
                                
                            except Exception as e:
                                print(f"⚠️ {row[0]}: 관심종목 저장 오류 - {e}")
                        
                        conn.commit()
                        total_added += added_count
                        print(f"✅ {condition['name']}: {added_count}개 추가")
                        
            except Exception as e:
                print(f"❌ {condition['name']} 조건 처리 오류: {e}")
        
        context['task_instance'].xcom_push(key='additional_generated', value=total_added)
        
        db.close()
        
        if total_added > 0:
            print(f"✅ 추가 관심종목 생성 완료: 총 {total_added}개")
        else:
            print("⚠️ 추가할 관심종목이 없습니다.")
        
        return f"✅ 추가 생성: {total_added}개"
        
    except Exception as e:
        print(f"❌ 추가 관심종목 생성 실패: {e}")
        import traceback
        traceback.print_exc()
        raise

# DAG 기본 설정
default_args = {
    'owner': 'stock-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'daily_watchlist_scanner_postgres',
    default_args=default_args,
    description='일별 기술적 지표 기반 관심종목 스캔 및 Redis 동기화 (PostgreSQL)',
    schedule_interval='*/30 * * * *',  # 30분마다 실행 (지속적 모니터링)
    catchup=False,
    max_active_runs=1,
    tags=['watchlist', 'postgresql', 'redis', 'technical-analysis']
)

# 태스크 정의
scan_bollinger_task = PythonOperator(
    task_id='scan_bollinger_bands',
    python_callable=scan_and_update_watchlist,
    dag=dag,
    doc_md="""
    ## 볼린저 밴드 상단 터치 종목 스캔 (PostgreSQL)
    
    - 전일 종가 기준으로 볼린저 밴드 상단선 98% 이상 터치한 종목 검색
    - 시가총액별 티어 분류 (대형주: 1, 중형주: 2, 소형주: 3)
    - daily_watchlist 테이블에 결과 저장
    """
)

scan_rsi_task = PythonOperator(
    task_id='scan_rsi_oversold',
    python_callable=scan_rsi_oversold,
    dag=dag,
    doc_md="""
    ## RSI 과매도 종목 스캔 (PostgreSQL)
    
    - RSI 30 이하 과매도 종목 검색
    - 잠재적 반등 후보 종목 식별
    """
)

scan_macd_task = PythonOperator(
    task_id='scan_macd_bullish',
    python_callable=scan_macd_bullish,
    dag=dag,
    doc_md="""
    ## MACD 강세 종목 스캔 (PostgreSQL)
    
    - MACD 라인이 시그널 라인 위에 있고 양수인 종목 검색
    - 강세 모멘텀 종목 식별
    """
)

top_performers_task = PythonOperator(
    task_id='get_top_performers',
    python_callable=get_top_performers,
    dag=dag,
    doc_md="""
    ## 상위 성과 종목 조회 (PostgreSQL)
    
    - 전일 대비 상승률 상위 종목 조회
    - 거래량 조건 적용 (최소 10만주)
    """
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_watchlist,
    dag=dag,
    doc_md="""
    ## 오래된 관심종목 데이터 정리 (PostgreSQL)
    
    - 30일 이전 관심종목 데이터 삭제
    - 데이터베이스 용량 관리
    """
)

summary_task = PythonOperator(
    task_id='send_summary',
    python_callable=send_watchlist_summary,
    dag=dag,
    doc_md="""
    ## 스캔 결과 요약 (PostgreSQL)
    
    - 모든 스캔 결과 통계 출력
    - 로그에 요약 정보 기록
    """
)

# 시장 상황 인식 태스크 추가
market_setup_task = PythonOperator(
    task_id='market_aware_setup',
    python_callable=market_aware_setup,
    dag=dag,
    doc_md="""
    ## 시장 시간대별 스캔 강도 설정
    
    - 현재 EST 시간대 확인
    - HIGH/MEDIUM/LOW 강도 결정
    - 활성화할 스캔 조건 선택
    
    ### 강도별 기준:
    - **HIGH** (09:30-16:00 EST): 정규 거래시간, 모든 조건 활성화
    - **MEDIUM** (04:00-09:30, 16:00-20:00 EST): 확장 거래시간, 핵심 조건만 
    - **LOW** (20:00-04:00 EST, 주말): 시장 마감, 최소 모니터링
    """
)

# 새로운 태스크들 추가
generate_additional_task = PythonOperator(
    task_id='generate_additional_watchlist',
    python_callable=generate_additional_watchlist,
    dag=dag,
    doc_md="""
    ## 추가 관심종목 생성
    
    - 배치 누락 대비 추가 관심종목 생성
    - 실시간 조건으로 관심종목 발굴
    """
)

redis_sync_task = PythonOperator(
    task_id='sync_to_redis',
    python_callable=sync_watchlist_to_redis,
    dag=dag,
    doc_md="""
    ## PostgreSQL → Redis 동기화
    
    - PostgreSQL 관심종목을 Redis로 동기화
    - 최근 3일간 데이터 확인 및 업데이트
    - 지속적 모니터링으로 누락 방지
    """
)

# 태스크 의존성 설정 - 시장 상황 인식 후 조건부 스캔
# 1. 시장 상황 분석 → 2. 조건부 스캔들 병렬 실행 → 3. Redis 동기화 → 4. 정리 및 요약

market_setup_task >> [generate_additional_task, scan_bollinger_task, scan_rsi_task, scan_macd_task, top_performers_task]
[generate_additional_task, scan_bollinger_task, scan_rsi_task, scan_macd_task, top_performers_task] >> redis_sync_task >> cleanup_task >> summary_task
