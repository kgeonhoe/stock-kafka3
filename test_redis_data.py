#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import redis
import json
from datetime import datetime

# Redis 연결 설정
try:
    # Docker 환경의 Redis에 연결
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    print("✅ Redis 연결 성공")
    
    # Redis 연결 테스트
    redis_client.ping()
    print("✅ Redis ping 성공")
    
    # watchlist 키들 조회
    watchlist_keys = redis_client.keys("watchlist:*")
    print(f"✅ watchlist 키 개수: {len(watchlist_keys)}")
    
    # 샘플 키들 출력
    print("\n📋 샘플 키들:")
    for key in watchlist_keys[:10]:
        print(f"  - {key}")
    
    # 실제 데이터 구조 확인
    print("\n🔍 실제 데이터 구조 확인:")
    sample_count = 0
    for key in watchlist_keys:
        if not (len(key.split(':')[1]) == 10 and key.split(':')[1][4] == '-' and key.split(':')[1][7] == '-'):  # 날짜 패턴 제외
            sample_count += 1
            if sample_count <= 5:  # 처음 5개만 확인
                symbol = key.split(':')[1]
                key_type = redis_client.type(key)
                print(f"\n  키: {key}")
                print(f"  심볼: {symbol}")
                print(f"  타입: {key_type}")
                
                if key_type == 'hash':
                    hash_data = redis_client.hgetall(key)
                    print(f"  데이터: {hash_data}")
                elif key_type == 'string':
                    string_data = redis_client.get(key)
                    print(f"  데이터: {string_data}")
    
    # 유효한 심볼 키 개수 계산
    valid_symbols = []
    for key in watchlist_keys:
        if key.startswith('watchlist:') and len(key.split(':')) == 2:
            symbol = key.split(':')[1]
            # 날짜 패턴이 아닌 심볼 키만 처리
            if not (len(symbol) == 10 and symbol[4] == '-' and symbol[7] == '-'):
                valid_symbols.append(symbol)
    
    print(f"\n✅ 유효한 심볼 개수: {len(valid_symbols)}")
    print(f"✅ 유효한 심볼들: {valid_symbols}")
    
except redis.ConnectionError as e:
    print(f"❌ Redis 연결 오류: {e}")
    print("💡 Docker 컨테이너가 실행 중인지 확인하세요")
except Exception as e:
    print(f"❌ 오류 발생: {e}")
