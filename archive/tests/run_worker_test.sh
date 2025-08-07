#!/bin/bash
# 워커 최적화 테스트 실행 스크립트

echo "🚀 Yahoo Finance API 워커 최적화 테스트"
echo "========================================="

# 빠른 테스트 옵션
if [ "$1" == "--quick" ] || [ "$1" == "-q" ]; then
    echo "⚡ 빠른 테스트 실행 중... (5일 데이터)"
    python test_worker_optimization.py --quick
    exit 0
fi

# 프로덕션 테스트 옵션
if [ "$1" == "--production" ] || [ "$1" == "-p" ]; then
    echo "🏭 프로덕션 테스트 실행 중... (1년 데이터)"
    echo "⚠️ 주의: API 제한이 발생할 수 있습니다!"
    echo ""
    read -p "계속하시겠습니까? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        python test_worker_optimization.py --production --period 1y
    else
        echo "테스트가 취소되었습니다."
    fi
    exit 0
fi

# 5년 테스트 옵션
if [ "$1" == "--5y" ]; then
    echo "📊 5년 데이터 테스트 실행 중..."
    echo "⚠️ 경고: 매우 긴 시간이 소요되고 API 제한이 심할 수 있습니다!"
    echo ""
    read -p "정말 실행하시겠습니까? (y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        python test_worker_optimization.py --production --period 5y
    else
        echo "테스트가 취소되었습니다."
    fi
    exit 0
fi

# 기본 설정으로 테스트
echo "📊 기본 테스트 실행 중..."
echo "  - 워커 수: 1, 3, 5, 7, 10개"
echo "  - 테스트 심볼: 15개"
echo "  - 데이터 기간: 5일"
echo "  - 예상 소요 시간: 약 10-15분"
echo ""

read -p "계속하시겠습니까? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    python test_worker_optimization.py --workers 1,3,5,7,10 --symbols 15
else
    echo "테스트가 취소되었습니다."
fi
