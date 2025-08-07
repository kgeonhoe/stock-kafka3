-- PostgreSQL Stock Database 초기화 스크립트

-- 기본 데이터베이스는 이미 존재하므로 테이블만 생성

-- 나스닥 종목 테이블
CREATE TABLE IF NOT EXISTS nasdaq_symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255),
    market_cap VARCHAR(50),
    sector VARCHAR(100),
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 주가 데이터 테이블
CREATE TABLE IF NOT EXISTS stock_data (
    symbol VARCHAR(10),
    date DATE,
    open NUMERIC(12,4),
    high NUMERIC(12,4),
    low NUMERIC(12,4),
    close NUMERIC(12,4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

-- 기술적 지표 테이블
CREATE TABLE IF NOT EXISTS stock_data_technical_indicators (
    symbol VARCHAR(10),
    date DATE,
    bb_upper NUMERIC(12,4),
    bb_middle NUMERIC(12,4),
    bb_lower NUMERIC(12,4),
    macd NUMERIC(12,4),
    macd_signal NUMERIC(12,4),
    macd_histogram NUMERIC(12,4),
    rsi NUMERIC(12,4),
    sma_5 NUMERIC(12,4),
    sma_20 NUMERIC(12,4),
    sma_60 NUMERIC(12,4),
    ema_5 NUMERIC(12,4),
    ema_20 NUMERIC(12,4),
    ema_60 NUMERIC(12,4),
    cci NUMERIC(12,4),
    obv BIGINT,
    ma_112 NUMERIC(12,4),
    ma_224 NUMERIC(12,4),
    ma_448 NUMERIC(12,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);

-- 일별 관심종목 테이블
CREATE TABLE IF NOT EXISTS daily_watchlist (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    condition_type VARCHAR(50) NOT NULL,  -- 'bollinger_upper_touch', 'rsi_oversold' 등
    condition_value NUMERIC(10,4),
    market_cap_tier INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date, condition_type)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_stock_data_symbol_date ON stock_data (symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_data_date ON stock_data (date DESC);
CREATE INDEX IF NOT EXISTS idx_nasdaq_symbols_collected_at ON nasdaq_symbols (collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_technical_indicators_symbol_date ON stock_data_technical_indicators (symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_watchlist_date ON daily_watchlist (date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_watchlist_symbol ON daily_watchlist (symbol);

-- 기술적 지표 계산용 뷰
CREATE OR REPLACE VIEW stock_technical_indicators AS
SELECT 
    symbol,
    date,
    close as close_price,
    -- 볼린저 밴드 (20일 이동평균 ± 2 표준편차)
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as bb_middle,
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) + 2 * STDDEV(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as bb_upper,
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) - 2 * STDDEV(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) as bb_lower,
    -- RSI 계산용 기본 데이터
    close - LAG(close) OVER (
        PARTITION BY symbol ORDER BY date
    ) as price_change
FROM stock_data
ORDER BY symbol, date;

-- 권한 설정 (필요시)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO stock_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO stock_user;

-- 데이터베이스 설정 최적화
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;

-- 설정 재로드
SELECT pg_reload_conf();

-- 테이블 생성 완료 메시지
SELECT 'PostgreSQL Stock Database 초기화 완료' as status;
