import enum
import random as py_random
from sqlalchemy import (
    Column, Integer, String, Date, Boolean, Numeric,
    ForeignKey, UniqueConstraint, Enum, BigInteger, Text, TIMESTAMP
)
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
Base = declarative_base()
# 移除了 MarketType 枚举类
class ActionType(enum.Enum):
    DIVIDEND = 'DIVIDEND'
    SPLIT = 'SPLIT'
    BONUS = 'BONUS'
class TradingCalendar(Base):
    __tablename__ = 'trading_calendars'
    id = Column(Integer, primary_key=True)
    # market 字段类型修改为 String
    market = Column(String(50), nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    __table_args__ = (UniqueConstraint('market', 'trade_date', name='_market_trade_date_uc'),)
class Security(Base):
    __tablename__ = 'securities'
    id = Column(Integer, primary_key=True)
    # --- 核心标识符 ---
    symbol = Column(String(30), nullable=False, index=True,
                    comment="标准化的、小写的证券代码 (例如 'aapl', 'nvda', '00700')")
    em_code = Column(String(30), unique=True, nullable=True, index=True,
                     comment="东方财富专用代码, 用于关联 (例如 '105.NVDA')")
    name = Column(String(255), comment="公司或证券的官方全名")
    # --- 市场与分类 ---
    # market 字段类型修改为 String
    market = Column(String(50), nullable=True, index=True,
                    comment="市场板块 (US, HK, CNA等)")
    type = Column(String(50), nullable=True, comment="资产类型 (STOCK, ETF等)")
    exchange = Column(String(50), comment="主要上市交易所代码 (例如 'XNAS', 'NYSE', 'HKEX')")
    currency = Column(String(10), comment="交易货币 (例如 'USD', 'HKD')")
    # --- Polygon 提供的详细信息 ---
    cik = Column(String(20), nullable=True, index=True, comment="SEC CIK 中央索引码")
    composite_figi = Column(String(20), nullable=True, index=True, comment="复合金融工具全球标识符 (FIGI)")
    share_class_figi = Column(String(20), nullable=True, index=True, comment="股份类别FIGI标识符")
    market_cap = Column(Numeric(25, 4), nullable=True, comment="最新市值 (以交易货币为单位)")
    phone_number = Column(String(50), nullable=True, comment="公司联系电话")
    description = Column(Text, nullable=True, comment="公司业务描述")
    homepage_url = Column(String(255), nullable=True, comment="公司官网地址")
    total_employees = Column(Integer, nullable=True, comment="员工总数")
    sic_code = Column(String(10), nullable=True, comment="标准行业分类(SIC)代码")
    industry = Column(String(255), nullable=True, comment="行业描述 (来自SIC描述)")
    # --- 地址信息 ---
    address_line1 = Column(String(255), nullable=True, comment="公司地址行1")
    city = Column(String(100), nullable=True, comment="公司所在城市")
    state = Column(String(100), nullable=True, comment="公司所在州/省")
    postal_code = Column(String(20), nullable=True, comment="邮政编码")
    # --- 品牌信息 ---
    logo_url = Column(Text, nullable=True, comment="公司Logo的URL")
    icon_url = Column(Text, nullable=True, comment="公司Icon的URL")
    # --- 状态与日期 ---
    is_active = Column(Boolean, default=True, index=True, comment="该证券是否仍在活跃交易")
    list_date = Column(Date, comment="上市日期")
    delist_date = Column(Date, comment="退市日期")
    # --- 维护时间戳 (由脚本显式管理) ---
    info_last_updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(),
                                  comment="非价格详情(名称/描述等)的上次更新时间")
    price_data_latest_date = Column(Date, nullable=True, index=True, comment="日线价格数据在数据库中覆盖的最新日期")
    full_data_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                       comment="上一次全量历史价格数据更新的成功时间")
    actions_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                     comment="公司行动数据(分红/拆股)的上次更新时间")
    # --- 自动刷新策略 ---
    full_refresh_interval = Column(Integer, nullable=False, default=lambda: py_random.randint(25, 40),
                                   comment="自动全量刷新的随机周期(天)")
    # --- 数据库约束 ---
    __table_args__ = (UniqueConstraint('symbol', name='_symbol_market_type_uc'),)


class DailyPrice(Base):
    __tablename__ = 'daily_prices'
    security_id = Column(Integer, ForeignKey('securities.id'), primary_key=True)
    date = Column(Date, primary_key=True, index=True)
    open = Column(Numeric(19, 6))
    high = Column(Numeric(19, 6))
    low = Column(Numeric(19, 6))
    close = Column(Numeric(19, 6))
    volume = Column(BigInteger)

    # --- 新增字段 ---
    turnover = Column(Numeric(25, 4), nullable=True, comment="成交额")
    vwap = Column(Numeric(19, 6), nullable=True, comment="成交量加权平均价 (VWAP), 可作为平均价")
    turnover_rate = Column(Numeric(10, 6), nullable=True, comment="换手率 (需要总股本数据计算)")

    # --- 复权相关字段 ---
    adj_factor = Column(Numeric(20, 6), nullable=True, server_default='1.0')

    security = relationship("Security")
    __table_args__ = (UniqueConstraint('security_id', 'date', name='_security_id_date_uc'),)

class StockDividend(Base):
    """存储股票分红信息"""
    __tablename__ = 'stock_dividends'
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer, ForeignKey('securities.id'), nullable=False, index=True)
    # --- 关键日期 ---
    ex_dividend_date = Column(Date, nullable=False, index=True, comment="除权日")
    declaration_date = Column(Date, nullable=True, comment="公告日")
    record_date = Column(Date, nullable=True, comment="股权登记日")
    pay_date = Column(Date, nullable=True, comment="派息日")
    # --- 分红详情 ---
    cash_amount = Column(Numeric(20, 10), nullable=False, comment="每股分红金额")
    currency = Column(String(10), nullable=False, comment="分红货币 (USD, HKD等)")
    frequency = Column(Integer, nullable=True, comment="分红频率 (e.g., 0:一次性, 1:年, 2:半年, 4:季度)")
    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('security_id', 'ex_dividend_date', 'cash_amount', name='_dividend_uc'),
    )
class StockSplit(Base):
    """存储股票拆股/合股信息"""
    __tablename__ = 'stock_splits'
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer, ForeignKey('securities.id'), nullable=False, index=True)
    # --- 关键日期 ---
    execution_date = Column(Date, nullable=False, index=True, comment="拆/合股执行日")
    declaration_date = Column(Date, nullable=True, comment="公告日") # Polygon v1 Splits 有此数据
    # --- 拆股详情 ---
    split_to = Column(Numeric(20, 10), nullable=False, comment="拆股后的份数 (e.g., 2-for-1 split, a value of 2)")
    split_from = Column(Numeric(20, 10), nullable=False, comment="拆股前的份数 (e.g., 2-for-1 split, a value of 1)")
    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('security_id', 'execution_date', name='_split_uc'),
    )

class HistoricalShare(Base):
    __tablename__ = 'historical_shares'
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer, nullable=False)
    change_date = Column(Date, nullable=False, index=True)
    total_shares = Column(BigInteger, nullable=True)
    float_shares = Column(BigInteger, nullable=True)
    __table_args__ = (UniqueConstraint('security_id', 'change_date', name='_security_change_date_uc'),)

