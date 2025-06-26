import enum
import random as py_random
from sqlalchemy import (
    create_engine, Column, Integer, String, Date, Boolean, Numeric,
    ForeignKey, UniqueConstraint, Enum, BigInteger, Text, TIMESTAMP
)
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class MarketType(enum.Enum):
    CNA = 'CNA'  # 中国A股
    HK = 'HK'  # 港股
    US = 'US'  # 美股
    CRYPTO = 'CRYPTO'
    FOREX = 'FOREX'
    INDEX = 'INDEX'


class AssetType(enum.Enum):
    STOCK = 'STOCK'
    ETF = 'ETF'
    INDEX = 'INDEX'
    CRYPTO = 'CRYPTO'
    FOREX = 'FOREX'
    PREFERRED_STOCK = 'PREFERRED_STOCK'
    WARRANT = 'WARRANT'
    OTC = 'OTC'
    MUTUAL_FUND = 'MUTUAL_FUND'


class ActionType(enum.Enum):
    DIVIDEND = 'DIVIDEND'
    SPLIT = 'SPLIT'
    BONUS = 'BONUS'


class TradingCalendar(Base):
    """存储各市场的交易日信息"""
    __tablename__ = 'trading_calendars'
    id = Column(Integer, primary_key=True)
    market = Column(ENUM(MarketType, name='market_type'), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    __table_args__ = (UniqueConstraint('market', 'trade_date', name='_market_trade_date_uc'),)


class Security(Base):
    __tablename__ = 'securities'
    id = Column(Integer, primary_key=True)

    # MODIFIED: symbol现在存储标准、通用的代码 (如 '600519', 'NVDA')
    symbol = Column(String(30), nullable=False, index=True, comment="标准化的证券代码 (如 600519, NVDA, 00700)")

    # NEW: 新增字段，专门用于存储东方财富的原始代码
    em_code = Column(String(30), unique=True, nullable=True, index=True,
                     comment="东方财富专用代码 (如 106600519, 105.NVDA)")

    name = Column(String(255))
    market = Column(ENUM(MarketType, name='market_type'), nullable=False, index=True)  # MODIFIED: 增加索引

    # MODIFIED: 扩展了 AssetType 枚举，以容纳更丰富的证券类型
    type = Column(ENUM(AssetType, name='asset_type'), nullable=False)

    exchange = Column(String(50), comment="交易所 (如 SSE, SZSE, NASDAQ, NYSE, HKEX)")
    currency = Column(String(10))
    sector = Column(String(100))
    industry = Column(String(100))
    is_active = Column(Boolean, default=True, index=True)
    list_date = Column(Date)
    delist_date = Column(Date)

    last_updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(),
                             comment="记录行任意更新的时间")
    info_last_updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(),
                                  comment="基本信息上次成功更新的时间")
    price_data_latest_date = Column(Date, nullable=True, index=True, comment="日线价格数据覆盖的最新日期")
    full_data_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                       comment="上一次全量历史数据更新的成功时间")
    full_refresh_interval = Column(Integer, nullable=False, default=lambda: py_random.randint(25, 40),
                                   comment="自动全量刷新的随机周期（天）")
    actions_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                     comment="公司行动数据（分红/拆股）上次成功更新的时间")
    # MODIFIED: 调整唯一性约束，确保 (标准代码, 市场, 类型) 是唯一的
    __table_args__ = (UniqueConstraint('symbol', 'market', 'type', name='_symbol_market_type_uc'),)


class DailyPrice(Base):
    __tablename__ = 'daily_prices'
    # MODIFIED: 复合主键，与 Security 表的 id 关联
    security_id = Column(Integer, ForeignKey('securities.id'), primary_key=True)
    date = Column(Date, primary_key=True, index=True)

    open = Column(Numeric(19, 6))
    high = Column(Numeric(19, 6))
    low = Column(Numeric(19, 6))
    close = Column(Numeric(19, 6))
    volume = Column(BigInteger, comment="成交量（股）")

    # NEW: 新增成交额字段
    amount = Column(Numeric(25, 6), nullable=True, comment="成交额")

    # NEW: 新增平均价字段 (虽然可计算，但根据要求添加)
    avg_price = Column(Numeric(19, 6), nullable=True, comment="平均价 (成交额/成交量)")

    # MODIFIED: 字段已存在，确认其用途
    turnover_rate = Column(Numeric(10, 6), nullable=True, comment="换手率(%)")

    adj_close = Column(Numeric(19, 6), nullable=True, comment="后复权收盘价")
    adj_factor = Column(Numeric(20, 6), nullable=False, server_default='1.0', comment="后复权因子")

    # 以下字段保留，用于更精确的复权计算
    event_factor = Column(Numeric(20, 6), nullable=False, server_default='1.0')
    cal_event_factor = Column(Numeric(20, 6), nullable=False, server_default='1.0')
    # 关联到 Security 表
    security = relationship("Security")

    # 定义复合主键
    __table_args__ = (UniqueConstraint('security_id', 'date', name='_security_id_date_uc'),)


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
