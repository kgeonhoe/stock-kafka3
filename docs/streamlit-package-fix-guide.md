# Streamlit 패키지 설치 가이드

## 🚨 문제 상황
```
ModuleNotFoundError: No module named 'dotenv'
ModuleNotFoundError: No module named 'psutil'
```

## 🔧 해결 방법

### 방법 1: 빠른 해결 (추천)
```bash
# 디렉토리 이동
cd /home/grey1/stock-kafka3/docker

# Streamlit 컨테이너에 직접 패키지 설치
docker compose exec streamlit pip install python-dotenv==1.0.0 psutil==5.9.6

# Streamlit 재시작
docker compose restart streamlit
```

### 방법 2: 자동 스크립트 실행
```bash
# 미리 만든 자동 해결 스크립트 실행
cd /home/grey1/stock-kafka3
./fix_streamlit_packages.sh
```

### 방법 3: 컨테이너 재빌드 (완전한 해결)
```bash
cd /home/grey1/stock-kafka3/docker

# Streamlit 컨테이너 중지 및 제거
docker compose down streamlit

# 컨테이너 재빌드 (requirements-streamlit.txt 적용)
docker compose build streamlit

# 컨테이너 시작
docker compose up -d streamlit
```

## 📊 테스트 및 확인

### 1. 브라우저에서 확인
```
http://localhost:8501
```

### 2. 새로운 페이지 확인
- "06_Kafka_부하테스트_모니터링" 페이지 접속
- 차트가 정상적으로 표시되는지 확인

### 3. 로그 확인
```bash
docker compose logs streamlit --tail=20
```

## 🔍 문제 해결

### 여전히 plotly 에러가 발생하는 경우:
```bash
docker compose exec streamlit pip install plotly==5.18.0 pandas==2.0.3
docker compose restart streamlit
```

### Redis 연결 에러가 발생하는 경우:
- Redis 서비스 상태 확인: `docker compose ps redis`
- Redis 재시작: `docker compose restart redis`

### 페이지가 로드되지 않는 경우:
- Streamlit 로그 확인: `docker compose logs streamlit -f`
- 8501 포트 확인: `netstat -an | grep 8501`

## 📝 업데이트된 파일들

1. **requirements-streamlit.txt**: `python-dotenv`, `psutil` 추가
2. **06_Kafka_부하테스트_모니터링.py**: import 에러 처리 추가
3. **Dockerfile.streamlit**: requirements 경로 수정

## 🎯 기대 결과

성공적으로 해결되면:
- ✅ ModuleNotFoundError 해결
- ✅ Kafka 부하테스트 대시보드 정상 동작
- ✅ 실시간 차트 및 메트릭 표시
- ✅ 시스템 리소스 모니터링 가능


데이터 수집 로직 추가 
-컬럼을 파서 다시 수집 이 필요한지 확인하게 해서 해당 종목만 다시 데이터 수집 
-실패했을 때 컬럼에 체크 default는 false 로 놓고 