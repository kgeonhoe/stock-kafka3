#!/bin/bash
# Streamlit 패키지 설치 및 재시작 스크립트

echo "🔧 Streamlit 컨테이너 패키지 문제 해결 시작..."

# 1. 현재 Streamlit 컨테이너 중지
echo "⏹️ Streamlit 컨테이너 중지..."
cd /home/grey1/stock-kafka3/docker
docker compose stop streamlit

# 2. 필요한 패키지를 컨테이너에 직접 설치
echo "📦 누락된 패키지 설치..."
docker compose exec streamlit pip install python-dotenv==1.0.0 psutil==5.9.6 plotly==5.18.0 pandas==2.0.3 || true

# 3. Streamlit 컨테이너 재시작
echo "🔄 Streamlit 컨테이너 재시작..."
docker compose up -d streamlit

# 4. 상태 확인
echo "📊 컨테이너 상태 확인..."
sleep 3
docker compose ps streamlit

echo "✅ 작업 완료!"
echo ""
echo "📌 확인 방법:"
echo "1. 브라우저에서 http://localhost:8501 접속"
echo "2. '06_Kafka_부하테스트_모니터링' 페이지 확인"
echo ""
echo "🔧 문제가 지속되면 컨테이너 재빌드를 실행하세요:"
echo "cd /home/grey1/stock-kafka3/docker"
echo "docker compose build streamlit"
echo "docker compose up -d streamlit"
