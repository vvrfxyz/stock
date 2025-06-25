# data_models/models.py
import enum
from sqlalchemy import (
    create_engine, Column, Integer, String, Date, Boolean, Numeric,
    ForeignKey, UniqueConstraint, Enum, BigInteger, Text, TIMESTAMP
)
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class MarketType(enum.Enum):
    CNA = 'CNA'
    HK = 'HK'
    US = 'US'
    CRYPTO = 'CRYPTO'
    FOREX = 'FOREX'
    INDEX = 'INDEX'


class AssetType(enum.Enum):
    STOCK = 'STOCK'
    ETF = 'ETF'
    INDEX = 'INDEX'
    CRYPTO = 'CRYPTO'
    FOREX = 'FOREX'


class ActionType(enum.Enum):
    DIVIDEND = 'DIVIDEND'
    SPLIT = 'SPLIT'
    BONUS = 'BONUS'


class Security(Base):
    __tablename__ = 'securities'
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(255))
    market = Column(ENUM(MarketType, name='market_type'), nullable=False)
    type = Column(ENUM(AssetType, name='asset_type'), nullable=False)
    exchange = Column(String(50))
    currency = Column(String(10))
    sector = Column(String(100))
    industry = Column(String(100))
    is_active = Column(Boolean, default=True)
    list_date = Column(Date)
    delist_date = Column(Date)
    last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    # --- OPTIMIZATION START: 增加新字段以追踪数据更新状态 ---
    price_data_latest_date = Column(Date, nullable=True, index=True, comment="日线价格数据覆盖的最新日期")
    # --- OPTIMIZATION END ---


class DailyPrice(Base):
    __tablename__ = 'daily_prices'
    security_id = Column(Integer, primary_key=True)
    date = Column(Date, primary_key=True, index=True)
    open = Column(Numeric(19, 6))
    high = Column(Numeric(19, 6))
    low = Column(Numeric(19, 6))
    close = Column(Numeric(19, 6))
    volume = Column(BigInteger)
    adj_close = Column(Numeric(19, 6), nullable=True)
    turnover_rate = Column(Numeric(10, 6), nullable=True)
    adj_factor = Column(Numeric(20, 6), nullable=False, server_default='1.0')
    event_factor = Column(Numeric(20, 6), nullable=False, server_default='1.0')
    cal_event_factor = Column(Numeric(20, 6), nullable=False, server_default='1.0')


class CorporateAction(Base):
    __tablename__ = 'corporate_actions'
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer, nullable=False)
    event_date = Column(Date, nullable=False, index=True)
    event_type = Column(ENUM(ActionType, name='action_type'), nullable=False)
    value = Column(Numeric(20, 10), nullable=False)

    __table_args__ = (UniqueConstraint('security_id', 'event_date', 'event_type', name='_security_date_type_uc'),)


class SpecialAdjustment(Base):
    __tablename__ = 'special_adjustments'
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer, nullable=False)
    event_date = Column(Date, nullable=False)
    adjustment_factor = Column(Numeric(20, 10), nullable=False)
    description = Column(Text)

    __table_args__ = (UniqueConstraint('security_id', 'event_date', name='_security_date_uc'),)


class HistoricalShare(Base):
    __tablename__ = 'historical_shares'
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer, nullable=False)
    change_date = Column(Date, nullable=False, index=True)
    total_shares = Column(BigInteger, nullable=True)
    float_shares = Column(BigInteger, nullable=True)

    __table_args__ = (UniqueConstraint('security_id', 'change_date', name='_security_change_date_uc'),)

