import os
from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, String, DateTime, Float, Integer, text
)

DB_URL = os.getenv(
    "GA_TRADING_DATABASE_URL",
    "postgresql+psycopg2://postgres:gozldsh12!@localhost:5432/ga_trading"
)

def get_engine():
    """SQLAlchemy 엔진 생성"""
    return create_engine(DB_URL, echo=False, pool_size=20, max_overflow=10)

def init_db():
    """market_data 테이블 생성 및 TimescaleDB 하이퍼테이블 설정"""
    engine = get_engine()
    metadata = MetaData()

    market_data = Table(
        "market_data", metadata,
        Column("ticker", String, primary_key=True),
        Column("datetime", DateTime, primary_key=True),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("volume", Integer),
    )

    metadata.create_all(engine)

    # Hypertable 생성 (if_not_exists 옵션으로 중복 방지)
    with engine.connect() as conn:
        conn.execute(text(
            "SELECT create_hypertable('market_data', 'datetime', if_not_exists => TRUE);"
        ))
    print("Initialized TimescaleDB hypertable: market_data")

if __name__ == "__main__":
    init_db()