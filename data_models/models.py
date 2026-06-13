import random as py_random
from sqlalchemy import (
    Column, Integer, String, Date, Boolean, Numeric,
    ForeignKey, UniqueConstraint, BigInteger, Text, TIMESTAMP, Index
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
Base = declarative_base()


def _default_current_symbol(context):
    return context.get_current_parameters().get("symbol")


class TradingCalendar(Base):
    __tablename__ = 'trading_calendars'
    exchange_mic = Column(String(20), ForeignKey('exchanges.mic'), primary_key=True)
    trade_date = Column(Date, primary_key=True)
    is_open = Column(Boolean, nullable=False, server_default='true')
    is_half_day = Column(Boolean, nullable=False, server_default='false')
    open_at = Column(TIMESTAMP(timezone=True), nullable=True)
    close_at = Column(TIMESTAMP(timezone=True), nullable=True)
    timezone = Column(String(50), nullable=True)
    holiday_name = Column(String(255), nullable=True)
    source = Column(String(30), nullable=False, server_default='manual', index=True)
    source_updated_at = Column(TIMESTAMP(timezone=True), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    exchange = relationship("Exchange")


class Exchange(Base):
    __tablename__ = 'exchanges'
    mic = Column(String(20), primary_key=True, comment="ISO 10383 Market Identifier Code, e.g. XNAS/XNYS")
    operating_mic = Column(String(20), nullable=True, index=True)
    acronym = Column(String(30), nullable=True)
    name = Column(String(255), nullable=False)
    exchange_type = Column(String(50), nullable=True)
    asset_class = Column(String(50), nullable=True, index=True)
    locale = Column(String(20), nullable=True, index=True)
    market = Column(String(50), nullable=True, index=True)
    participant_id = Column(String(30), nullable=True)
    url = Column(Text, nullable=True)
    source = Column(String(30), nullable=False, server_default='manual', index=True)
    source_id = Column(String(128), nullable=True)
    source_updated_at = Column(TIMESTAMP(timezone=True), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class Security(Base):
    __tablename__ = 'securities'
    id = Column(BigInteger, primary_key=True)
    # --- 核心标识符 ---
    symbol = Column(String(30), nullable=False, index=True,
                    comment="标准化的、小写的证券代码 (例如 'aapl', 'nvda', '00700')")
    current_symbol = Column(
        String(30),
        nullable=False,
        index=True,
        default=_default_current_symbol,
        comment="当前最新证券代码",
    )
    name = Column(String(255), comment="公司或证券的官方全名")
    # --- 市场与分类 ---
    # market 字段类型修改为 String
    market = Column(String(50), nullable=True, index=True,
                    comment="市场板块 (US, HK, CNA等)")
    type = Column(String(50), nullable=True, comment="资产类型 (STOCK, ETF等)")
    exchange = Column(String(50), comment="主要上市交易所代码 (例如 'XNAS', 'NYSE', 'HKEX')")
    currency = Column(String(10), comment="交易货币 (例如 'USD', 'HKD')")
    currency_symbol = Column(String(10), nullable=True, comment="Massive 返回的 ISO 货币代码")
    base_currency_name = Column(String(50), nullable=True, comment="Massive base currency name")
    base_currency_symbol = Column(String(10), nullable=True, comment="Massive base currency symbol")
    vendor_market = Column(String(30), nullable=True, index=True, comment="Massive market 字段，例如 stocks")
    locale = Column(String(10), nullable=True, index=True, comment="Massive locale 字段，例如 us/global")
    # --- Massive 提供的详细信息 ---
    cik = Column(String(20), nullable=True, index=True, comment="SEC CIK 中央索引码")
    composite_figi = Column(String(30), nullable=True, index=True, comment="复合金融工具全球标识符 (FIGI)")
    share_class_figi = Column(String(20), nullable=True, index=True, comment="股份类别FIGI标识符")
    ticker_root = Column(String(30), nullable=True, index=True, comment="Ticker root，例如 BRK.A 的 BRK")
    ticker_suffix = Column(String(30), nullable=True, comment="Ticker suffix，例如 BRK.A 的 A")
    round_lot = Column(Integer, nullable=True, comment="标准交易单位")
    share_class_shares_outstanding = Column(BigInteger, nullable=True, comment="该 share class 的 outstanding shares")
    weighted_shares_outstanding = Column(BigInteger, nullable=True, comment="按 share class 折算后的 weighted outstanding shares")
    market_cap = Column(Numeric(25, 4), nullable=True, comment="最新市值 (以交易货币为单位)")
    phone_number = Column(String(50), nullable=True, comment="公司联系电话")
    description = Column(Text, nullable=True, comment="公司业务描述")
    homepage_url = Column(String(255), nullable=True, comment="公司官网地址")
    total_employees = Column(Integer, nullable=True, comment="员工总数")
    sic_code = Column(String(10), nullable=True, comment="标准行业分类(SIC)代码")
    sector = Column(String(100), nullable=True, comment="行业板块")
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
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    vendor_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True, comment="Massive reference 数据自身的更新时间")
    info_last_updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(),
                                  comment="非价格详情(名称/描述等)的上次更新时间")
    price_data_latest_date = Column(Date, nullable=True, index=True, comment="日线价格数据在数据库中覆盖的最新日期")
    full_data_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                       comment="上一次全量历史价格数据更新的成功时间")
    actions_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                     comment="公司行动数据(分红/拆股)的上次更新时间")
    events_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                    comment="ticker events / symbol history 的上次更新时间")
    shares_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                    comment="股本与 free float 的上次更新时间")
    short_data_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                        comment="short interest / short volume 的上次更新时间")
    news_last_updated_at = Column(TIMESTAMP(timezone=True), nullable=True,
                                  comment="新闻数据的上次更新时间")
    # --- 自动刷新策略 ---
    full_refresh_interval = Column(Integer, nullable=False, default=lambda: py_random.randint(25, 40),
                                   comment="自动全量刷新的随机周期(天)")
    # --- 数据库约束 ---
    __table_args__ = (
        Index(
            '_active_symbol_uc',
            'symbol',
            unique=True,
            postgresql_where=(is_active.is_(True)),
        ),
        Index(
            '_active_current_symbol_exchange_uc',
            'current_symbol',
            'exchange',
            unique=True,
            postgresql_where=(is_active.is_(True)),
        ),
    )


class SecuritySymbolHistory(Base):
    __tablename__ = 'security_symbol_history'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    symbol = Column(String(30), nullable=False, index=True)
    exchange = Column(String(30), nullable=True)
    source = Column(String(30), nullable=False, index=True)
    source_event_id = Column(String(128), nullable=True)
    event_type = Column(String(30), nullable=True)
    start_date = Column(Date, nullable=True, index=True)
    end_date = Column(Date, nullable=True, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('security_id', 'symbol', 'source', 'start_date', name='_security_symbol_history_source_uc'),
    )


class CorporateAction(Base):
    __tablename__ = 'corporate_actions'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    action_type = Column(String(20), nullable=False, index=True, comment="'DIVIDEND' 或 'SPLIT'")

    ex_date = Column(Date, nullable=False, index=True)
    declaration_date = Column(Date, nullable=True)
    record_date = Column(Date, nullable=True)
    pay_date = Column(Date, nullable=True)

    cash_amount = Column(Numeric(20, 10), nullable=True)
    currency = Column(String(10), nullable=True)
    frequency = Column(Integer, nullable=True)
    distribution_type = Column(String(30), nullable=True)
    split_from = Column(Numeric(20, 10), nullable=True)
    split_to = Column(Numeric(20, 10), nullable=True)
    adjustment_type = Column(String(30), nullable=True)

    source = Column(String(30), nullable=False, index=True)
    source_event_id = Column(String(128), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('security_id', 'action_type', 'source', 'source_event_id', name='_corporate_action_source_event_uc'),
    )


class VendorAdjustmentFactor(Base):
    __tablename__ = 'vendor_adjustment_factors'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    source = Column(String(30), nullable=False, index=True)
    factor_type = Column(String(30), nullable=False, index=True)
    factor_key = Column(String(160), nullable=False)
    source_event_id = Column(String(128), nullable=True, index=True)
    adjustment_factor = Column(Numeric(24, 12), nullable=False)
    raw_close = Column(Numeric(19, 6), nullable=True)
    adjusted_close = Column(Numeric(19, 6), nullable=True)
    as_of_date = Column(Date, nullable=True, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('security_id', 'source', 'factor_key', name='_vendor_adjustment_factor_key_uc'),
    )


class ComputedAdjustmentFactor(Base):
    __tablename__ = 'computed_adjustment_factors'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    methodology_version = Column(String(50), nullable=False, index=True)
    factor_type = Column(String(30), nullable=False, index=True)
    factor_key = Column(String(160), nullable=False)
    source_event_id = Column(String(128), nullable=True, index=True)
    action_type = Column(String(20), nullable=True, index=True)
    single_event_factor = Column(Numeric(24, 12), nullable=True)
    cumulative_factor = Column(Numeric(24, 12), nullable=False)
    previous_close = Column(Numeric(19, 6), nullable=True)
    event_hash = Column(String(64), nullable=False)
    as_of_date = Column(Date, nullable=True, index=True)
    built_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('security_id', 'methodology_version', 'factor_key', name='_computed_adjustment_factor_key_uc'),
    )


class SecurityIdentifier(Base):
    __tablename__ = 'security_identifiers'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    id_type = Column(String(30), nullable=False, index=True)
    id_value = Column(String(80), nullable=False, index=True)
    start_date = Column(Date, nullable=True, index=True)
    end_date = Column(Date, nullable=True, index=True)
    source = Column(String(30), nullable=False, index=True)
    confidence = Column(String(20), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint(
            'security_id',
            'id_type',
            'id_value',
            'source',
            'start_date',
            name='_security_identifier_source_uc',
        ),
    )


class SecFiling(Base):
    __tablename__ = 'sec_filings'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=True, index=True)
    source = Column(String(30), nullable=False, index=True)
    cik = Column(String(20), nullable=True, index=True)
    ticker = Column(String(30), nullable=True, index=True)
    issuer_name = Column(String(255), nullable=True)
    form_type = Column(String(30), nullable=False, index=True)
    accession_number = Column(String(32), nullable=False, index=True)
    filing_date = Column(Date, nullable=False, index=True)
    accepted_at = Column(TIMESTAMP(timezone=True), nullable=True, index=True)
    period_of_report = Column(Date, nullable=True, index=True)
    filing_url = Column(Text, nullable=True)
    primary_document_url = Column(Text, nullable=True)
    available_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('source', 'accession_number', name='_sec_filing_source_accession_uc'),
    )


class InsiderTransaction(Base):
    __tablename__ = 'insider_transactions'
    id = Column(BigInteger, primary_key=True)
    filing_id = Column(BigInteger, ForeignKey('sec_filings.id'), nullable=True, index=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=True, index=True)
    source = Column(String(30), nullable=False, index=True)
    accession_number = Column(String(32), nullable=False, index=True)
    source_row_hash = Column(String(64), nullable=False)
    form_type = Column(String(10), nullable=True, index=True)
    filing_date = Column(Date, nullable=True, index=True)
    period_of_report = Column(Date, nullable=True)
    issuer_cik = Column(String(20), nullable=True, index=True)
    issuer_trading_symbol = Column(String(30), nullable=True, index=True)
    issuer_name = Column(String(255), nullable=True)
    owner_cik = Column(String(20), nullable=True, index=True)
    owner_name = Column(String(255), nullable=True, index=True)
    is_director = Column(Boolean, nullable=True)
    is_officer = Column(Boolean, nullable=True)
    is_ten_percent_owner = Column(Boolean, nullable=True)
    is_other = Column(Boolean, nullable=True)
    officer_title = Column(String(255), nullable=True)
    security_type = Column(String(50), nullable=True, index=True)
    record_type = Column(String(50), nullable=True, index=True)
    security_title = Column(String(255), nullable=True)
    transaction_timeliness = Column(String(10), nullable=True)
    aff_10b5_one = Column(Boolean, nullable=True)
    transaction_date = Column(Date, nullable=True, index=True)
    deemed_execution_date = Column(Date, nullable=True)
    transaction_code = Column(String(10), nullable=True, index=True)
    equity_swap_involved = Column(Boolean, nullable=True)
    transaction_shares = Column(Numeric(24, 6), nullable=True)
    transaction_price_per_share = Column(Numeric(24, 6), nullable=True)
    transaction_acquired_disposed = Column(String(5), nullable=True, index=True)
    shares_owned_following_transaction = Column(Numeric(24, 6), nullable=True)
    transaction_value = Column(Numeric(28, 6), nullable=True)
    exercise_date = Column(Date, nullable=True)
    expiration_date = Column(Date, nullable=True)
    underlying_security_title = Column(String(255), nullable=True)
    underlying_security_shares = Column(Numeric(24, 6), nullable=True)
    direct_or_indirect = Column(String(5), nullable=True)
    footnotes = Column(Text, nullable=True)
    remarks = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    filing = relationship("SecFiling")
    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('source', 'accession_number', 'source_row_hash', name='_insider_transaction_row_uc'),
    )


class InstitutionalHolding(Base):
    __tablename__ = 'institutional_holdings'
    id = Column(BigInteger, primary_key=True)
    filing_id = Column(BigInteger, ForeignKey('sec_filings.id'), nullable=True, index=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=True, index=True)
    source = Column(String(30), nullable=False, index=True)
    accession_number = Column(String(32), nullable=False, index=True)
    source_row_hash = Column(String(64), nullable=False)
    filer_cik = Column(String(20), nullable=False, index=True)
    filer_name = Column(String(255), nullable=True, index=True)
    form_type = Column(String(20), nullable=True, index=True)
    filing_date = Column(Date, nullable=True, index=True)
    period = Column(Date, nullable=True, index=True)
    issuer_name = Column(String(255), nullable=True, index=True)
    title_of_class = Column(String(100), nullable=True)
    cusip = Column(String(20), nullable=True, index=True)
    market_value = Column(Numeric(24, 4), nullable=True)
    shares_or_principal_amount = Column(Numeric(24, 4), nullable=True)
    shares_or_principal_type = Column(String(10), nullable=True)
    put_call = Column(String(10), nullable=True, index=True)
    investment_discretion = Column(String(20), nullable=True)
    other_managers = Column(ARRAY(String(255)), nullable=True)
    voting_authority_sole = Column(Numeric(24, 4), nullable=True)
    voting_authority_shared = Column(Numeric(24, 4), nullable=True)
    voting_authority_none = Column(Numeric(24, 4), nullable=True)
    file_number = Column(String(50), nullable=True)
    film_number = Column(String(50), nullable=True)
    filing_url = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    filing = relationship("SecFiling")
    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('source', 'accession_number', 'source_row_hash', name='_institutional_holding_row_uc'),
    )


class DailyPrice(Base):
    __tablename__ = 'daily_prices'
    # 复合主键 (security_id, date) 即唯一约束；ON CONFLICT 由主键推断。
    security_id = Column(BigInteger, ForeignKey('securities.id'), primary_key=True)
    date = Column(Date, primary_key=True, index=True)
    open = Column(Numeric(19, 6))
    high = Column(Numeric(19, 6))
    low = Column(Numeric(19, 6))
    close = Column(Numeric(19, 6))
    volume = Column(BigInteger)
    otc = Column(Boolean, nullable=True, comment="Massive otc 标记；false 时供应商常省略该字段")

    vwap = Column(Numeric(19, 6), nullable=True, comment="成交量加权平均价 (VWAP), 可作为平均价")
    trade_count = Column(BigInteger, nullable=True, comment="成交笔数")
    pre_market = Column(Numeric(19, 6), nullable=True, comment="盘前价格")
    after_hours = Column(Numeric(19, 6), nullable=True, comment="盘后价格")

    security = relationship("Security")

class HistoricalShare(Base):
    __tablename__ = 'historical_shares'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    filing_date = Column(Date, nullable=False, index=True)
    period_end_date = Column(Date, nullable=False, index=True)
    total_shares = Column(BigInteger, nullable=False)
    float_shares = Column(BigInteger, nullable=True)
    free_float_percent = Column(Numeric(10, 4), nullable=True)
    source = Column(String(30), nullable=False, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (UniqueConstraint('security_id', 'filing_date', 'source', name='_historical_shares_filing_source_uc'),)


class HistoricalFloat(Base):
    __tablename__ = 'historical_floats'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    effective_date = Column(Date, nullable=False, index=True)
    free_float = Column(BigInteger, nullable=False)
    free_float_percent = Column(Numeric(10, 4), nullable=True)
    source = Column(String(30), nullable=False, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (UniqueConstraint('security_id', 'effective_date', 'source', name='_historical_floats_effective_source_uc'),)


class ShortInterest(Base):
    __tablename__ = 'short_interests'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    ticker = Column(String(30), nullable=False, index=True)
    settlement_date = Column(Date, nullable=False, index=True)
    short_interest = Column(BigInteger, nullable=False)
    avg_daily_volume = Column(BigInteger, nullable=True)
    days_to_cover = Column(Numeric(20, 6), nullable=True)
    source = Column(String(30), nullable=False, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (UniqueConstraint('security_id', 'settlement_date', 'source', name='_short_interest_security_date_source_uc'),)


class ShortVolume(Base):
    __tablename__ = 'short_volumes'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=False, index=True)
    ticker = Column(String(30), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    short_volume = Column(BigInteger, nullable=False)
    total_volume = Column(BigInteger, nullable=True)
    short_volume_ratio = Column(Numeric(20, 6), nullable=True)
    exempt_volume = Column(BigInteger, nullable=True)
    non_exempt_volume = Column(BigInteger, nullable=True)
    adf_short_volume = Column(BigInteger, nullable=True)
    adf_short_volume_exempt = Column(BigInteger, nullable=True)
    nasdaq_carteret_short_volume = Column(BigInteger, nullable=True)
    nasdaq_carteret_short_volume_exempt = Column(BigInteger, nullable=True)
    nasdaq_chicago_short_volume = Column(BigInteger, nullable=True)
    nasdaq_chicago_short_volume_exempt = Column(BigInteger, nullable=True)
    nyse_short_volume = Column(BigInteger, nullable=True)
    nyse_short_volume_exempt = Column(BigInteger, nullable=True)
    source = Column(String(30), nullable=False, index=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (UniqueConstraint('security_id', 'date', 'source', name='_short_volume_security_date_source_uc'),)


class NewsArticle(Base):
    __tablename__ = 'news_articles'
    id = Column(BigInteger, primary_key=True)
    source = Column(String(30), nullable=False, index=True)
    source_article_id = Column(String(128), nullable=False, unique=True, index=True)
    published_utc = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    title = Column(Text, nullable=True)
    author = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    article_url = Column(Text, nullable=True)
    amp_url = Column(Text, nullable=True)
    image_url = Column(Text, nullable=True)
    publisher_name = Column(String(255), nullable=True, index=True)
    publisher_homepage_url = Column(Text, nullable=True)
    publisher_logo_url = Column(Text, nullable=True)
    publisher_favicon_url = Column(Text, nullable=True)
    tickers = Column(ARRAY(String(30)), nullable=True)
    keywords = Column(ARRAY(String(100)), nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class NewsArticleInsight(Base):
    __tablename__ = 'news_article_insights'
    id = Column(BigInteger, primary_key=True)
    source_article_id = Column(String(128), nullable=False, index=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=True, index=True)
    ticker = Column(String(30), nullable=False, index=True)
    sentiment = Column(String(30), nullable=True, index=True)
    sentiment_reasoning = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint('source_article_id', 'ticker', name='_news_article_insight_article_ticker_uc'),
    )


class SecFundamentalFact(Base):
    """SEC XBRL companyfacts 的 curated 基本面事实。

    一行 = 一个 (CIK, taxonomy, concept, unit, period, accession) 的申报值。
    point-in-time 边界是 filed_date：任何因子计算只能使用 filed_date <= as_of 的行。
    同一概念同一期间可能被后续申报修正（10-K/A 或下一期重述），按 filed_date
    最新者为当前最优值——读取层负责选择，本表保留全部申报历史。
    """
    __tablename__ = 'sec_fundamental_facts'
    id = Column(BigInteger, primary_key=True)
    security_id = Column(BigInteger, ForeignKey('securities.id'), nullable=True, index=True)
    cik = Column(String(20), nullable=False, index=True)
    taxonomy = Column(String(20), nullable=False, comment="us-gaap / dei")
    concept = Column(String(120), nullable=False, index=True, comment="XBRL 概念名（SEC 官方驼峰）")
    unit = Column(String(40), nullable=False, comment="USD / shares / USD-per-shares 等")
    period_start = Column(Date, nullable=False,
                          comment="duration 型事实的期间起点；instant 型存 period_end（零长期间），保证唯一键非空")
    period_end = Column(Date, nullable=False, index=True, comment="期间终点或 instant 时点")
    is_instant = Column(Boolean, nullable=False, default=False, comment="True=时点值(资产负债表)；False=期间值")
    value = Column(Numeric(28, 6), nullable=False)
    fiscal_year = Column(Integer, nullable=True, comment="SEC fy 字段（申报口径财年）")
    fiscal_period = Column(String(10), nullable=True, comment="FY/Q1/Q2/Q3/Q4")
    form_type = Column(String(30), nullable=True, index=True)
    accession_number = Column(String(32), nullable=False)
    filed_date = Column(Date, nullable=False, index=True, comment="point-in-time 可见日：早于该日不可用")
    frame = Column(String(30), nullable=True, comment="SEC frame 标签，如 CY2026Q1")
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    security = relationship("Security")
    __table_args__ = (
        UniqueConstraint(
            'cik', 'taxonomy', 'concept', 'unit', 'period_start', 'period_end', 'accession_number',
            name='_sec_fundamental_fact_uc',
        ),
    )


class FxRate(Base):
    """ECB 每日参考汇率（raw reference facts）。1 base_currency = rate quote_currency。

    USD 交叉换算在读取层完成（utils/fx_rates.py），不存换算结果。"""
    __tablename__ = 'fx_rates'
    rate_date = Column(Date, primary_key=True)
    base_currency = Column(String(10), primary_key=True)
    quote_currency = Column(String(10), primary_key=True)
    source = Column(String(30), primary_key=True)
    rate = Column(Numeric(20, 10), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
