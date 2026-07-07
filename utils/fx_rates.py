"""fx_rates 的读取层：按日期取任意币种对 USD 的交叉汇率。

ECB 以 EUR 为基准（1 EUR = rate CCY），CCY->USD = rate(EUR->USD) / rate(EUR->CCY)。
ECB 未覆盖的币种（如 TWD）回退到 USD 基直连行（1 USD = rate CCY，如 FRED DEXTAUS），
CCY->USD = 1 / rate；ECB 交叉路径始终优先。
两条路径都仅在源的发布日有行情；取"当日或之前最近一个发布日"，超过 max_staleness_days
视为无可用汇率（返回 None，调用方自行决定跳过语义）。
"""
from __future__ import annotations

from bisect import bisect_right
from datetime import date
from decimal import Decimal, localcontext

from data_models.models import FxRate

DEFAULT_MAX_STALENESS_DAYS = 7
DEFAULT_FALLBACK_SOURCE = "FRED"

# 硬锚定币种白名单：对 USD 长期 1:1 锚定、且 ECB/FRED 均无序列的货币。
# BMD（百慕大元）自 1972 年起 1:1 锚定 USD；库内自证：NTB 分红 2019-08 前标 BMD、
# 2019-11 起标 USD，换标前后金额同为 0.44（vendor 只改标签、从未换算）。
# 只放"绝无浮动"的硬锚定；软钉住（如 HKD 区间盯住）不进此表。
USD_PEGGED_CURRENCIES: dict[str, Decimal] = {
    "BMD": Decimal("1"),
}


class UsdFxConverter:
    """按需加载单序列并缓存；线程不安全，单脚本进程内使用。"""

    def __init__(
        self,
        db_manager,
        *,
        source: str = "ECB",
        fallback_source: str = DEFAULT_FALLBACK_SOURCE,
        max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS,
    ):
        self._db = db_manager
        self._source = source
        self._fallback_source = fallback_source
        self._max_staleness_days = max_staleness_days
        self._series: dict[tuple[str, str, str], tuple[list[date], dict[date, Decimal]]] = {}

    def rate_to_usd(self, currency: str, on_date: date) -> Decimal | None:
        """返回 1 currency 兑多少 USD；无可用汇率返回 None。"""
        currency = (currency or "").upper()
        if currency == "USD":
            return Decimal("1")
        pegged = USD_PEGGED_CURRENCIES.get(currency)
        if pegged is not None:
            return pegged
        rate = self._ecb_cross_rate(currency, on_date)
        if rate is not None:
            return rate
        return self._direct_usd_base_rate(currency, on_date)

    def _ecb_cross_rate(self, currency: str, on_date: date) -> Decimal | None:
        usd_rate = self._rate_asof(self._source, "EUR", "USD", on_date)
        if usd_rate is None:
            return None
        if currency == "EUR":
            return usd_rate
        ccy_rate = self._rate_asof(self._source, "EUR", currency, on_date)
        if ccy_rate is None or ccy_rate == 0:
            return None
        with localcontext() as ctx:
            ctx.prec = 28
            return usd_rate / ccy_rate

    def _direct_usd_base_rate(self, currency: str, on_date: date) -> Decimal | None:
        # USD 基直连行口径：1 USD = rate CCY（如 FRED DEXTAUS），折 USD 取倒数。
        rate = self._rate_asof(self._fallback_source, "USD", currency, on_date)
        if rate is None or rate == 0:
            return None
        with localcontext() as ctx:
            ctx.prec = 28
            return Decimal("1") / rate

    def _rate_asof(self, source: str, base: str, quote: str, on_date: date) -> Decimal | None:
        dates, by_date = self._load(source, base, quote)
        index = bisect_right(dates, on_date) - 1
        if index < 0:
            return None
        rate_date = dates[index]
        if (on_date - rate_date).days > self._max_staleness_days:
            return None
        return by_date[rate_date]

    def _load(self, source: str, base: str, quote: str) -> tuple[list[date], dict[date, Decimal]]:
        key = (source, base, quote)
        if key not in self._series:
            with self._db.get_session() as session:
                rows = (
                    session.query(FxRate.rate_date, FxRate.rate)
                    .filter(
                        FxRate.source == source,
                        FxRate.base_currency == base,
                        FxRate.quote_currency == quote,
                    )
                    .order_by(FxRate.rate_date.asc())
                    .all()
                )
            dates = [row.rate_date for row in rows]
            by_date = {row.rate_date: Decimal(str(row.rate)) for row in rows}
            self._series[key] = (dates, by_date)
        return self._series[key]
