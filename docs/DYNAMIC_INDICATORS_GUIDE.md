# 📊 동적 기술적 지표 관리 시스템 사용법

## 🎯 개요

이 시스템은 **설정 파일 기반**으로 기술적 지표를 동적으로 추가하고 관리할 수 있는 시스템입니다. 코드 수정 없이 새로운 지표를 쉽게 추가할 수 있습니다.

---

## 📂 파일 구조

```
stock-kafka3/
├── config/
│   └── technical_indicators.json    # 지표 설정 파일
├── common/
│   └── database.py                  # 동적 지표 관리 시스템
└── scripts/
    └── add_indicator.py             # CLI 도구 (선택사항)
```

---

## 🚀 시작하기

### 1. 기본 설정

시스템을 처음 실행하면 자동으로 기본 설정 파일이 생성됩니다:

```python
from common.database import DuckDBManager

# 데이터베이스 초기화 (설정 파일 자동 생성)
db = DuckDBManager()
```

### 2. 설정 파일 확인

생성된 설정 파일 위치: `/app/data/technical_indicators.json`

```json
{
  "indicators": {
    "existing": {
      "bb_upper": {"type": "BB_UPPER", "period": 20},
      "bb_middle": {"type": "BB_MIDDLE", "period": 20},
      "bb_lower": {"type": "BB_LOWER", "period": 20},
      "macd": {"type": "MACD", "fast": 12, "slow": 26},
      "rsi": {"type": "RSI", "period": 14}
    }
  }
}
```

---

## 📝 지표 추가 방법

### 방법 1: Python 코드로 추가

```python
from common.database import DuckDBManager

db = DuckDBManager()

# 개별 지표 추가
db.add_indicator("momentum", "rsi_7", "RSI", period=7)
db.add_indicator("momentum", "rsi_21", "RSI", period=21)
db.add_indicator("trend", "adx", "ADX", period=14)
db.add_indicator("volume", "vwap", "VWAP", period=20)
db.add_indicator("volatility", "atr", "ATR", period=14)
```

### 방법 2: 설정 파일 직접 수정

```json
{
  "indicators": {
    "existing": {
      "bb_upper": {"type": "BB_UPPER", "period": 20},
      "bb_middle": {"type": "BB_MIDDLE", "period": 20},
      "bb_lower": {"type": "BB_LOWER", "period": 20},
      "macd": {"type": "MACD", "fast": 12, "slow": 26},
      "rsi": {"type": "RSI", "period": 14}
    },
    "momentum": {
      "rsi_7": {"type": "RSI", "period": 7},
      "rsi_21": {"type": "RSI", "period": 21},
      "stoch_k": {"type": "STOCH_K", "period": 14},
      "stoch_d": {"type": "STOCH_D", "period": 3},
      "williams_r": {"type": "WILLIAMS_R", "period": 14},
      "roc": {"type": "ROC", "period": 12}
    },
    "trend": {
      "adx": {"type": "ADX", "period": 14},
      "sma_10": {"type": "SMA", "period": 10},
      "sma_50": {"type": "SMA", "period": 50},
      "sma_200": {"type": "SMA", "period": 200},
      "ema_12": {"type": "EMA", "period": 12},
      "ema_26": {"type": "EMA", "period": 26}
    },
    "volatility": {
      "atr": {"type": "ATR", "period": 14},
      "bb_width": {"type": "BB_WIDTH", "period": 20},
      "bb_percent": {"type": "BB_PERCENT", "period": 20}
    },
    "volume": {
      "vwap": {"type": "VWAP", "period": 20},
      "ad_line": {"type": "AD_LINE", "period": 14},
      "cmf": {"type": "CMF", "period": 20}
    },
    "fibonacci": {
      "fib_23_6": {"type": "FIBONACCI", "level": 0.236},
      "fib_38_2": {"type": "FIBONACCI", "level": 0.382},
      "fib_61_8": {"type": "FIBONACCI", "level": 0.618}
    },
    "ichimoku": {
      "tenkan_sen": {"type": "ICHIMOKU_TENKAN", "period": 9},
      "kijun_sen": {"type": "ICHIMOKU_KIJUN", "period": 26},
      "senkou_span_a": {"type": "ICHIMOKU_SENKOU_A", "period": 26},
      "senkou_span_b": {"type": "ICHIMOKU_SENKOU_B", "period": 52}
    }
  }
}
```

---

## 🔧 CLI 도구 사용법 (선택사항)

### CLI 스크립트 생성

```python
# filepath: /home/grey1/stock-kafka3/scripts/add_indicator.py
import sys
import argparse
from common.database import DuckDBManager

def main():
    parser = argparse.ArgumentParser(description='기술적 지표 추가 도구')
    parser.add_argument('--category', required=True, help='지표 카테고리')
    parser.add_argument('--name', required=True, help='지표명')
    parser.add_argument('--type', required=True, help='지표 타입')
    parser.add_argument('--period', type=int, help='기간')
    parser.add_argument('--fast', type=int, help='빠른 기간')
    parser.add_argument('--slow', type=int, help='느린 기간')
    parser.add_argument('--level', type=float, help='레벨 (피보나치 등)')
    
    args = parser.parse_args()
    
    db = DuckDBManager()
    
    kwargs = {}
    if args.period:
        kwargs['period'] = args.period
    if args.fast:
        kwargs['fast'] = args.fast
    if args.slow:
        kwargs['slow'] = args.slow
    if args.level:
        kwargs['level'] = args.level
    
    db.add_indicator(args.category, args.name, args.type, **kwargs)
    print(f"✅ 지표 추가 완료: {args.name}")

if __name__ == "__main__":
    main()
```

### CLI 사용 예시

```bash
# 컨테이너 내에서 실행
cd /app
python scripts/add_indicator.py --category momentum --name rsi_7 --type RSI --period 7
python scripts/add_indicator.py --category trend --name macd_fast --type MACD --fast 8 --slow 21
python scripts/add_indicator.py --category fibonacci --name fib_50 --type FIBONACCI --level 0.5
```

---

## 💾 지표 데이터 저장

### 기본 사용법

```python
from common.database import DuckDBManager

db = DuckDBManager()

# 지표 데이터 저장
indicator_data = {
    'symbol': 'AAPL',
    'date': '2024-07-21',
    # 기존 지표
    'rsi': 65.4,
    'macd': 1.23,
    'bb_upper': 200.5,
    'bb_lower': 189.5,
    'sma_20': 195.5,
    # 새로 추가된 지표들
    'rsi_7': 68.2,
    'rsi_21': 62.1,
    'sma_10': 198.2,
    'sma_50': 192.1,
    'sma_200': 185.3,
    'stoch_k': 72.1,
    'stoch_d': 68.9,
    'atr': 3.45,
    'vwap': 196.8,
    'adx': 45.2
}

# 자동으로 모든 지표가 저장됨 (없는 값은 NULL)
db.save_technical_indicators(indicator_data)
```

---

## 📊 지표 조회

### 현재 등록된 모든 지표 확인

```python
# 모든 지표명 조회
all_indicators = db.indicator_manager.get_all_indicator_names()
print("등록된 지표들:", all_indicators)

# 카테고리별 지표 확인
config = db.indicator_manager.indicators_config
for category, indicators in config["indicators"].items():
    print(f"\n[{category}]")
    for name, config in indicators.items():
        print(f"  {name}: {config}")
```

### 테이블 구조 확인

```python
import duckdb

con = duckdb.connect("/data/duckdb/stock_data.db")

# 테이블 컬럼 확인
columns = con.execute("DESCRIBE stock_data_technical_indicators").fetchall()
for col in columns:
    print(f"{col[0]}: {col[1]}")
```

---

## 🎯 카테고리별 지표 예시

### 📈 추세 지표 (Trend)
- `sma_10`, `sma_20`, `sma_50`, `sma_200`: 단순이동평균
- `ema_12`, `ema_26`: 지수이동평균
- `adx`: 평균방향지수
- `parabolic_sar`: 패러볼릭 SAR

### ⚡ 모멘텀 지표 (Momentum)
- `rsi`, `rsi_7`, `rsi_21`: 상대강도지수
- `stoch_k`, `stoch_d`: 스토캐스틱
- `williams_r`: 윌리엄스 %R
- `roc`: 변화율

### 📊 변동성 지표 (Volatility)
- `atr`: 평균실체범위
- `bb_width`: 볼린저밴드 폭
- `bb_percent`: %B

### 📦 거래량 지표 (Volume)
- `vwap`: 거래량가중평균가격
- `ad_line`: 누적/분산선
- `cmf`: 차이킨 자금 흐름

### 🔢 피보나치 지표 (Fibonacci)
- `fib_23_6`, `fib_38_2`, `fib_61_8`: 피보나치 되돌림

### 🏯 일목균형표 (Ichimoku)
- `tenkan_sen`: 전환선
- `kijun_sen`: 기준선
- `senkou_span_a`, `senkou_span_b`: 선행스팬

---

## 🛠️ 고급 사용법

### 지표 설정 동적 로드

```python
# 런타임에 설정 파일 다시 로드
db.indicator_manager.indicators_config = db.indicator_manager._load_indicators_config()
db.indicator_manager._ensure_all_columns()
```

### 대량 지표 추가

```python
# 여러 지표 한번에 추가
new_indicators = [
    ("momentum", "rsi_3", "RSI", {"period": 3}),
    ("momentum", "rsi_14", "RSI", {"period": 14}),
    ("momentum", "rsi_30", "RSI", {"period": 30}),
    ("trend", "sma_5", "SMA", {"period": 5}),
    ("trend", "sma_15", "SMA", {"period": 15}),
    ("trend", "sma_30", "SMA", {"period": 30}),
]

for category, name, indicator_type, config in new_indicators:
    db.add_indicator(category, name, indicator_type, **config)
```

---

## ⚠️ 주의사항

### 1. 컬럼명 규칙
- 영문, 숫자, 언더스코어(_)만 사용
- 숫자로 시작하면 안됨
- SQL 예약어 피하기

### 2. 데이터 타입
- 모든 지표 값은 `DOUBLE` 타입으로 저장
- NULL 값 허용

### 3. 성능 고려사항
- 지표가 많아질수록 INSERT 성능 영향
- 자주 사용하지 않는 지표는 별도 테이블 고려

---

## 🔍 문제 해결

### 설정 파일이 생성되지 않는 경우
```python
# 수동으로 디렉토리 생성
import os
os.makedirs("/app/config", exist_ok=True)

# 설정 파일 수동 생성
db = DuckDBManager()
```

### 컬럼 추가가 안되는 경우
```python
# 수동으로 컬럼 추가
db.indicator_manager._add_column_if_not_exists("new_indicator_name")
```

### 기존 데이터와 호환성
- 기존 지표 컬럼은 그대로 유지
- 새 지표는 NULL 값으로 시작
- 점진적으로 데이터 채워나가기

---

## 🎉 활용 예시

### Airflow DAG에서 사용
```python
# airflow/plugins/collect_stock_data_yfinance.py
def save_indicators_with_new_system(symbol, date, indicators):
    db = DuckDBManager()
    
    indicator_data = {
        'symbol': symbol,
        'date': date,
        **indicators  # 모든 계산된 지표 포함
    }
    
    db.save_technical_indicators(indicator_data)
```

### 실시간 스트리밍에서 사용
```python
# kafka/realtime_consumer.py
def process_technical_indicators(self, symbol, price_data):
    db = DuckDBManager()
    
    # 실시간 지표 계산
    indicators = calculate_realtime_indicators(price_data)
    
    # 동적으로 저장
    db.save_technical_indicators({
        'symbol': symbol,
        'date': datetime.now().date(),
        **indicators
    })
```

이 시스템을 사용하면 **코드 수정 없이** 새로운 기술적 지표를 쉽게 추가하고 관리할 수 있습니다! 🚀