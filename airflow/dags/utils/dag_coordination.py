#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DAG 간 조정 및 충돌 방지 유틸리티
- 대량 수집과 일별 수집 간 API 충돌 방지
- 리소스 사용 조정
"""

import os
import json
from datetime import datetime, timedelta
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from typing import Dict, Optional

class BulkCollectionSensor(BaseSensorOperator):
    """대량 수집 상태 감지 센서"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bulk_flag_file = "/tmp/nasdaq_bulk_collection_running.flag"
        self.completion_flag_file = "/tmp/nasdaq_bulk_collection_complete.flag"
    
    def poke(self, context: Context) -> bool:
        """
        대량 수집이 실행 중이 아닌지 확인
        
        Returns:
            True: 일별 수집 실행 가능 (대량 수집 중이 아님)
            False: 일별 수집 대기 (대량 수집 실행 중)
        """
        # 1. 실행 중 플래그 확인
        if os.path.exists(self.bulk_flag_file):
            try:
                with open(self.bulk_flag_file, 'r') as f:
                    bulk_info = json.load(f)
                
                # 실행 시작 시간 확인 (24시간 이상 지나면 무시)
                start_time = datetime.fromisoformat(bulk_info.get('start_time', ''))
                if datetime.now() - start_time > timedelta(hours=24):
                    # 오래된 플래그 제거
                    os.remove(self.bulk_flag_file)
                    self.log.warning("오래된 대량 수집 플래그 제거")
                    return True
                
                self.log.info(f"대량 수집 실행 중: {bulk_info}")
                return False
                
            except Exception as e:
                self.log.error(f"대량 수집 플래그 읽기 실패: {e}")
                # 에러 시 플래그 제거하고 진행
                try:
                    os.remove(self.bulk_flag_file)
                except:
                    pass
                return True
        
        # 2. 완료 플래그 확인 (최근 24시간 내 완료된 경우)
        if os.path.exists(self.completion_flag_file):
            try:
                with open(self.completion_flag_file, 'r') as f:
                    completion_info = json.load(f)
                
                completion_time = datetime.fromisoformat(completion_info.get('completion_time', ''))
                if datetime.now() - completion_time < timedelta(hours=24):
                    self.log.info(f"최근 대량 수집 완료: {completion_time}")
                    # 최근 완료된 경우에도 진행 허용
                    return True
                
            except Exception as e:
                self.log.error(f"완료 플래그 읽기 실패: {e}")
        
        # 3. 플래그가 없으면 진행 허용
        return True

def create_bulk_collection_running_flag(**kwargs):
    """대량 수집 시작 플래그 생성"""
    flag_info = {
        'start_time': datetime.now().isoformat(),
        'dag_run_id': kwargs['dag_run'].run_id,
        'task_instance': kwargs['task_instance'].task_id
    }
    
    flag_file = "/tmp/nasdaq_bulk_collection_running.flag"
    with open(flag_file, 'w') as f:
        json.dump(flag_info, f, indent=2)
    
    print(f"🚩 대량 수집 시작 플래그 생성: {flag_file}")
    return flag_info

def remove_bulk_collection_running_flag(**kwargs):
    """대량 수집 실행 중 플래그 제거"""
    flag_file = "/tmp/nasdaq_bulk_collection_running.flag"
    
    try:
        if os.path.exists(flag_file):
            os.remove(flag_file)
            print(f"🗑️ 대량 수집 실행 중 플래그 제거: {flag_file}")
        else:
            print("대량 수집 플래그가 이미 없음")
    except Exception as e:
        print(f"⚠️ 플래그 제거 실패: {e}")

def check_api_rate_limits(**kwargs):
    """API 호출 제한 확인"""
    import time
    import requests
    
    # Yahoo Finance API 상태 확인
    try:
        # 간단한 테스트 요청
        test_url = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"
        response = requests.get(test_url, timeout=10)
        
        if response.status_code == 429:  # Too Many Requests
            print("⚠️ Yahoo Finance API 제한 감지")
            return False
        elif response.status_code == 200:
            print("✅ Yahoo Finance API 정상")
            return True
        else:
            print(f"⚠️ Yahoo Finance API 응답: {response.status_code}")
            return True  # 다른 에러는 무시하고 진행
            
    except Exception as e:
        print(f"⚠️ API 상태 확인 실패: {e}")
        return True  # 에러 시 진행

def get_optimal_batch_size(**kwargs) -> int:
    """현재 시스템 상태에 따른 최적 배치 크기 계산"""
    import psutil
    
    # 메모리 사용률 기반 배치 크기 조정
    memory_percent = psutil.virtual_memory().percent
    
    if memory_percent > 80:
        return 10  # 메모리 부족 시 작은 배치
    elif memory_percent > 60:
        return 20  # 중간 배치
    else:
        return 50  # 여유 있을 때 큰 배치

def get_optimal_delay(**kwargs) -> float:
    """API 호출 간 최적 지연 시간 계산"""
    import random
    from datetime import datetime
    
    # 시간대별 지연 조정
    current_hour = datetime.now().hour
    
    if 9 <= current_hour <= 16:  # 미국 장시간
        base_delay = 2.0  # 장시간에는 더 긴 지연
    else:
        base_delay = 1.0  # 장외시간에는 짧은 지연
    
    # 랜덤 요소 추가 (429 에러 방지)
    return base_delay + random.uniform(0, 1)
