#!/bin/bash
# 메모리 사용량 모니터링 스크립트

echo "📊 시스템 메모리 모니터링"
echo "=========================="

# 현재 메모리 상태
echo "💾 현재 메모리 상태:"
free -h

echo ""
echo "🔄 프로세스별 메모리 사용량 (상위 10개):"
ps aux --sort=-%mem | head -n 11

echo ""
echo "🐍 Python 프로세스 메모리 사용량:"
ps aux | grep python | grep -v grep | awk '{print $2, $4"% ", $11}' | sort -k2 -nr

echo ""
echo "✈️ Airflow 관련 프로세스:"
ps aux | grep airflow | grep -v grep | awk '{print $2, $4"% ", $11}'

echo ""
echo "📈 메모리 사용률 경고:"
MEMORY_USAGE=$(free | grep Mem | awk '{printf("%.1f"), $3/$2 * 100.0}')
echo "메모리 사용률: ${MEMORY_USAGE}%"

if (( $(echo "$MEMORY_USAGE > 80" | bc -l) )); then
    echo "⚠️ 경고: 메모리 사용률이 80%를 초과했습니다!"
elif (( $(echo "$MEMORY_USAGE > 60" | bc -l) )); then
    echo "⚡ 주의: 메모리 사용률이 60%를 초과했습니다."
else
    echo "✅ 정상: 메모리 사용률이 양호합니다."
fi

echo ""
echo "🔧 메모리 최적화 제안:"
echo "1. 배치 크기를 줄여보세요 (현재: 20 → 15)"
echo "2. 워커 수를 줄여보세요 (현재: 5 → 3)"
echo "3. 가비지 컬렉션을 더 자주 실행하세요"
echo "4. DuckDB 연결을 배치마다 재시작하세요"
