"""fx_rates 的读取层：按日期取任意币种对 USD 的交叉汇率。

ECB 以 EUR 为基准（1 EUR = rate CCY），CCY->USD = rate(EUR->USD) / rate(EUR->CCY)。
ECB 仅 TARGET 工作日发布；取"当日或之前最近一个发布日"，超过 max_staleness_days
视为无可用汇率（返回 None，调用方自行决定跳过语义）。
"""
from __future__ import annotations

from bisect import bisect_right
from datetime import date
from decimal import Decimal, localcontext

from data_models.models import FxRate

DEFAULT_MAX_STALENESS_DAYS = 7


class UsdFxConverter:
    """按需加载单币种全序列并缓存；线程不安全，单脚本进程内使用。"""

    def __init__(self, db_manager, *, source: str = "ECB", max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS):
        self._db = db_manager
        self._source = source
        self._max_staleness_days = max_staleness_days
        self._series: dict[str, tuple[list[date], dict[date, Decimal]]] = {}

    def rate_to_usd(self, currency: str, on_date: date) -> Decimal | None:
        """返回 1 currency 兑多少 USD；无可用汇率返回 None。"""
        currency = (currency or "").upper()
        if currency == "USD":
            return Decimal("1")
        usd_rate = self._rate_asof("USD", on_date)
        if usd_rate is None:
            return None
        if currency == "EUR":
            return usd_rate
        ccy_rate = self._rate_asof(currency, on_date)
        if ccy_rate is None or ccy_rate == 0:
            return None
        with localcontext() as ctx:
            ctx.prec = 28
            return usd_rate / ccy_rate

    def _rate_asof(self, currency: str, on_date: date) -> Decimal | None:
        dates, by_date = self._load(currency)
        index = bisect_right(dates, on_date) - 1
        if index < 0:
            return None
        rate_date = dates[index]
        if (on_date - rate_date).days > self._max_staleness_days:
            return None
        return by_date[rate_date]

    def _load(self, currency: str) -> tuple[list[date], dict[date, Decimal]]:
        if currency not in self._series:
            with self._db.get_session() as session:
                rows = (
                    session.query(FxRate.rate_date, FxRate.rate)
                    .filter(
                        FxRate.source == self._source,
                        FxRate.base_currency == "EUR",
                        FxRate.quote_currency == currency,
                    )
                    .order_by(FxRate.rate_date.asc())
                    .all()
                )
            dates = [row.rate_date for row in rows]
            by_date = {row.rate_date: Decimal(str(row.rate)) for row in rows}
            self._series[currency] = (dates, by_date)
        return self._series[currency]
