-- 주식 분할/배당 체크 로그 테이블 생성
CREATE TABLE IF NOT EXISTS split_check_log (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    check_date DATE NOT NULL DEFAULT CURRENT_DATE,
    has_split BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 중복 체크 방지를 위한 유니크 제약
    UNIQUE(symbol, check_date)
);

-- 인덱스 생성 (성능 최적화)
CREATE INDEX IF NOT EXISTS idx_split_check_symbol_date ON split_check_log(symbol, check_date);
CREATE INDEX IF NOT EXISTS idx_split_check_date ON split_check_log(check_date);

-- 설명 추가
COMMENT ON TABLE split_check_log IS '주식 분할/배당 체크 이력을 저장하여 중복 체크를 방지';
COMMENT ON COLUMN split_check_log.symbol IS '체크한 주식 심볼';
COMMENT ON COLUMN split_check_log.check_date IS '체크한 날짜';
COMMENT ON COLUMN split_check_log.has_split IS '분할이 발견되었는지 여부';
