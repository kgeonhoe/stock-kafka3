#!/bin/bash
# Redis 관심종목 샘플 데이터 로딩 스크립트

echo "🚀 Redis 관심종목 샘플 데이터 로딩"
echo "=================================="

# Redis 서버 상태 확인
echo "🔍 Redis 서버 상태 확인..."
if ! redis-cli ping > /dev/null 2>&1; then
    echo "❌ Redis 서버가 실행되지 않고 있습니다."
    echo "💡 Redis 시작 방법:"
    echo "   sudo systemctl start redis"
    echo "   또는"
    echo "   redis-server"
    exit 1
fi

echo "✅ Redis 서버 실행 중"

# Python 환경 확인
echo "🐍 Python 환경 확인..."
if ! python -c "import redis, duckdb" > /dev/null 2>&1; then
    echo "⚠️ 필요한 패키지가 설치되지 않았습니다."
    echo "📦 패키지 설치 중..."
    
    pip install redis duckdb pandas > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ 패키지 설치 완료"
    else
        echo "❌ 패키지 설치 실패"
        exit 1
    fi
else
    echo "✅ Python 환경 준비됨"
fi

# 샘플 데이터 로딩 실행
echo ""
echo "📊 샘플 데이터 로딩 시작..."
python load_sample_watchlist_to_redis.py

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 샘플 데이터 로딩 완료!"
    echo ""
    echo "📋 Redis 데이터 확인:"
    echo "=================================="
    
    # Redis 키 확인
    echo "🔑 생성된 키 목록:"
    redis-cli keys "watchlist:*" | sed 's/^/   /'
    
    echo ""
    echo "📈 종목 수 확인:"
    SYMBOL_COUNT=$(redis-cli get "watchlist:symbols" | jq '. | length' 2>/dev/null || echo "N/A")
    echo "   총 종목: ${SYMBOL_COUNT}개"
    
    echo ""
    echo "💡 다음 단계:"
    echo "   1. Streamlit 모니터링 앱 실행"
    echo "   2. 실시간 데이터 확인"
    echo "   3. Kafka 프로듀서 테스트"
    
else
    echo ""
    echo "❌ 샘플 데이터 로딩 실패"
    echo "📋 문제 해결:"
    echo "   1. Redis 서버 상태 확인"
    echo "   2. 파일 권한 확인" 
    echo "   3. Python 패키지 설치 상태 확인"
fi
