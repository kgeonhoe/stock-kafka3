# 워커 수 최적화 테스트 가이드

## 개요
Yahoo Finance API 호출 시 최적의 워커 수를 찾기 위한 테스트 스크립트입니다. API 제한을 피하면서 최대 성능을 얻을 수 있는 설정을 찾습니다.

## 설치 요구사항
```bash
pip install yfinance
```

## 사용법

### 1. 빠른 테스트 (5개 심볼, 5일 데이터, 3분 소요)
```bash
python test_worker_optimization.py --quick
```

### 2. 프로덕션 테스트 (1년 데이터, 실제 환경 시뮬레이션)
```bash
python test_worker_optimization.py --production --period 1y
```

### 3. 극한 테스트 (5년 데이터, API 제한 테스트)
```bash
python test_worker_optimization.py --production --period 5y
```

### 4. 기본 테스트 (15개 심볼, 5일 데이터, 10분 소요)
```bash
python test_worker_optimization.py
```

### 5. 커스텀 테스트
```bash
# 특정 워커 수만 테스트
python test_worker_optimization.py --workers 3,5,8

# 심볼 수 조정
python test_worker_optimization.py --symbols 20

# 쿨다운 시간 조정
python test_worker_optimization.py --cooldown 15
```

### 6. 전체 옵션
```bash
python test_worker_optimization.py \
    --workers 1,3,5,7,10,15 \
    --symbols 20 \
    --cooldown 10
```

## 출력 예시

```
🚀 워커 수 최적화 테스트 시작
============================================================
테스트 설정:
  - 워커 수: [3, 5, 8]
  - 심볼 수: 15
  - 쿨다운: 10초
============================================================

🧪 워커 수 3개 테스트 시작...
✅ 워커 3개 완료:
   성공: 15/15 (100.0%)
   API 제한 오류: 0개
   총 시간: 8.2초
   처리율: 1.83 symbols/sec
   효율성 점수: 1.83

⏰ 쿨다운 대기 중... (10초)

🧪 워커 수 5개 테스트 시작...
✅ 워커 5개 완료:
   성공: 14/15 (93.3%)
   API 제한 오류: 1개
   총 시간: 6.1초
   처리율: 2.30 symbols/sec
   효율성 점수: 2.04

📊 워커 수 최적화 테스트 결과 분석
======================================================================
워커수 성공률   API제한  처리율        평균시간   효율점수
----------------------------------------------------------------------
3      100.0%  0        1.83       2.45s    1.83
5      93.3%   1        2.30       1.98s    2.04
8      86.7%   2        2.85       1.45s    2.27

🎯 최적 설정:
   워커 수: 8개
   성공률: 86.7%
   처리율: 2.85 symbols/sec
   효율성 점수: 2.27

🔧 프로덕션 설정 제안:
   MAX_WORKERS = 8
   BATCH_SIZE = 12
   예상 처리 시간 (1000개 심볼): 5.8분
   예상 처리 시간 (나스닥 3000개): 0.3시간
   권장 스케줄: 매시간 실행 가능
```

## 해석 가이드

### 효율성 점수 계산
- 기본 점수 = 성공률 × 처리속도
- API 에러 페널티 = 에러 수 × 0.1
- 최종 점수 = 기본 점수 - 페널티

### 최적 워커 수 선택 기준
1. **높은 성공률**: API 제한 에러 최소화
2. **빠른 처리속도**: 단위 시간당 처리량 최대화
3. **안정성**: 지속적인 성능 유지

### 프로덕션 적용 가이드
1. **워커 수**: 테스트 결과의 최적 워커 수 사용
2. **배치 크기**: 워커 수에 따른 적절한 배치 크기 설정
3. **스케줄링**: 예상 처리 시간에 따른 실행 빈도 결정

## 문제 해결

### API 제한 에러가 많은 경우
- 워커 수를 줄여서 다시 테스트
- 쿨다운 시간을 늘려서 테스트
- `--symbols` 옵션으로 테스트 규모 축소

### 네트워크 오류가 발생하는 경우
- 인터넷 연결 상태 확인
- yfinance 라이브러리 업데이트
- 방화벽 설정 확인

### 테스트가 너무 오래 걸리는 경우
- `--quick` 옵션으로 빠른 테스트 실행
- `--symbols` 값을 줄여서 테스트 규모 축소

## 예제 스크립트 통합

테스트 결과를 자동으로 설정 파일에 적용하려면:

```python
# config_updater.py
import subprocess
import json

def update_worker_config():
    # 테스트 실행
    result = subprocess.run([
        'python', 'test_worker_optimization.py', '--quick'
    ], capture_output=True, text=True)
    
    # 결과 파싱 (간단한 예시)
    if "최적 워커 수:" in result.stdout:
        # 실제 구현에서는 더 정교한 파싱 필요
        optimal_workers = 5  # 파싱된 결과
        
        # 설정 파일 업데이트
        config = {
            "MAX_WORKERS": optimal_workers,
            "BATCH_SIZE": 100 // optimal_workers,
            "LAST_OPTIMIZED": "2024-01-01"
        }
        
        with open('config/worker_config.json', 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✅ 설정 업데이트 완료: {optimal_workers}개 워커")

if __name__ == "__main__":
    update_worker_config()
```

이 스크립트를 사용하면 Yahoo Finance API의 현재 상태에 맞는 최적의 워커 설정을 자동으로 찾고 적용할 수 있습니다.
