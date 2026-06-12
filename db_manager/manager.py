"""组合各领域 mixin 的共享数据库入口；行为与拆分前的单文件实现一致。"""
from .core import DatabaseManagerCore
from .corporate_actions import CorporateActionsMixin
from .market_data import MarketDataMixin
from .reference_data import ReferenceDataMixin
from .securities import SecuritiesMixin


class DatabaseManager(
    SecuritiesMixin,
    CorporateActionsMixin,
    MarketDataMixin,
    ReferenceDataMixin,
    DatabaseManagerCore,
):
    pass
