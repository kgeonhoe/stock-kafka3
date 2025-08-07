#!/bin/bash
# 부하테스트 실행 스크립트

echo "🚀 Stock Kafka 부하테스트 시작"
echo "================================"

# Python 가상환경 활성화 (필요시)
# source venv/bin/activate

# 의존성 설치 확인
echo "📦 의존성 설치 확인..."
pip install -r requirements-airflow.txt

# 1. Kafka 서비스 상태 확인
echo "🔍 Kafka 서비스 상태 확인..."
if docker compose ps kafka | grep -q "Up"; then
    echo "✅ Kafka 서비스 실행 중"
else
    echo "⚠️ Kafka 서비스를 시작합니다..."
    docker compose up -d kafka
    sleep 10
fi

# 2. 부하테스트 메뉴
echo "
🔧 부하테스트 옵션:
1. 가벼운 테스트 (사용자 5명, 3분)
2. 중간 테스트 (사용자 20명, 10분)  
3. 무거운 테스트 (사용자 50명, 20분)
4. Kafka 전용 테스트
5. 커스텀 설정
"

read -p "선택하세요 (1-5): " choice

case $choice in
    1)
        echo "💡 가벼운 테스트 실행..."
        python3 run_load_test.py --test-type all --users 5 --duration 3m --kafka-threads 5 --kafka-messages 100
        ;;
    2)
        echo "🔥 중간 테스트 실행..."
        python3 run_load_test.py --test-type all --users 20 --duration 10m --kafka-threads 10 --kafka-messages 500
        ;;
    3)
        echo "💥 무거운 테스트 실행..."
        python3 run_load_test.py --test-type all --users 50 --duration 20m --kafka-threads 20 --kafka-messages 1000
        ;;
    4)
        echo "📤 Kafka 전용 테스트 실행..."
        python3 run_load_test.py --test-type kafka --kafka-threads 15 --kafka-messages 2000
        ;;
    5)
        read -p "동시 사용자 수: " users
        read -p "실행 시간 (예: 5m): " duration
        read -p "Kafka 스레드 수: " threads
        read -p "스레드당 메시지 수: " messages
        
        echo "🎯 커스텀 테스트 실행..."
        python3 run_load_test.py --test-type all --users $users --duration $duration --kafka-threads $threads --kafka-messages $messages
        ;;
    *)
        echo "❌ 잘못된 선택입니다."
        exit 1
        ;;
esac

echo "
📊 테스트 결과 확인:
- HTML 리포트: load_test_report_*.html
- CSV 데이터: load_test_results_*.csv  
- 성능 로그: performance.log
- Prometheus 메트릭: http://localhost:8000
"

echo "🎉 부하테스트 완료!"
