"""集中式证券身份解析层（只读）。

历史上每个脚本都各自维护一份临时的 ``{symbol: security_id}`` 字典，
而 ``utils/adjusted_prices.resolve_security_id`` 只做最朴素的单行 symbol 查找，
既不认 FIGI/CIK，也不感知改名/回收（symbol 复用）。本模块把身份解析集中起来。

核心口径：
- ``security_id`` 是持久身份；``symbol`` 是可变属性，绝不当作持久键。
- FIGI 是最强信号（同一证券 FIGI 不变），其次 CIK，再次活跃 symbol，最后历史 symbol。
- 改名（rename）：同一身份、symbol 变了。
- 回收（recycle）：同一 symbol、却是另一个身份（老代码退市后被新股复用）。

只读——构造时预加载全量 ``securities`` 索引（含 inactive），之后所有方法均不写库、
不修改任何状态。
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import NamedTuple

from data_models.models import Security, SecuritySymbolHistory

__all__ = ["SecurityIdentityResolver", "ResolutionResult"]


@dataclass(frozen=True)
class ResolutionResult:
    """单次解析结果。"""

    security_id: int  # -1 表示未绑定既有身份（NEW，或历史 symbol 多身份无法消歧）
    resolution_type: str  # "FIGI" / "CIK" / "ACTIVE_SYMBOL" / "HISTORY_SYMBOL" / "NEW"
    confidence: str  # "HIGH" / "MEDIUM" / "LOW"
    matched_field: str  # 命中的字段，例如 "composite_figi" / "symbol"
    is_rename: bool  # 同一身份但 symbol 变了
    is_recycle: bool  # 命中了某行、但 incoming 身份与其冲突（symbol 复用 / 同 CIK 新证券）
    recycled_from: int | None = None  # 死票回收：NEW 上市但 symbol 仍挂在某 inactive 行名下时，指向该旧身份


class _SecurityRow(NamedTuple):
    id: int
    symbol: str
    current_symbol: str
    composite_figi: str | None
    share_class_figi: str | None
    cik: str | None
    exchange: str | None
    is_active: bool


def _norm_figi(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip().upper()
    return v or None


def _norm_symbol(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip().lower()
    return v or None


def _norm_cik(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip()
    return v or None


class SecurityIdentityResolver:
    """集中式证券身份解析器。

    预加载全量 securities 索引（含 inactive），提供按 FIGI/CIK/symbol 的多路查找。
    只读——不写库，不修改任何状态。

    典型用法::

        resolver = SecurityIdentityResolver(session)
        result = resolver.resolve(symbol="META", composite_figi="BBG000MM2P62")
        # result.security_id, result.resolution_type, result.is_rename, ...
    """

    def __init__(self, session) -> None:
        self._by_id: dict[int, _SecurityRow] = {}
        self._by_figi: dict[str, _SecurityRow] = {}
        self._by_cik: dict[str, list[_SecurityRow]] = {}
        self._by_symbol: dict[str, _SecurityRow] = {}
        self._by_symbol_all: dict[str, list[_SecurityRow]] = {}
        self._by_history_symbol: dict[str, list[tuple[_SecurityRow, date | None, date | None]]] = {}
        self._load(session)

    # ------------------------------------------------------------------ #
    # 索引加载
    # ------------------------------------------------------------------ #
    def _load(self, session) -> None:
        rows_by_id: dict[int, _SecurityRow] = {}
        cols = (
            Security.id,
            Security.symbol,
            Security.current_symbol,
            Security.composite_figi,
            Security.share_class_figi,
            Security.cik,
            Security.exchange,
            Security.is_active,
        )
        for r in session.query(*cols).all():
            row = _SecurityRow(*r)
            rows_by_id[row.id] = row
            self._by_id[row.id] = row

            figi = _norm_figi(row.composite_figi)
            if figi is not None:
                # 同一 FIGI 理论上唯一；若重复，活跃行优先。
                existing = self._by_figi.get(figi)
                if existing is None or (not existing.is_active and row.is_active):
                    self._by_figi[figi] = row

            cik = _norm_cik(row.cik)
            if cik is not None:
                self._by_cik.setdefault(cik, []).append(row)

            sym = _norm_symbol(row.symbol)
            if sym is not None:
                self._by_symbol_all.setdefault(sym, []).append(row)
                if row.is_active:
                    # 活跃 symbol 有 partial unique index，唯一。
                    self._by_symbol[sym] = row

        hist_cols = (
            SecuritySymbolHistory.security_id,
            SecuritySymbolHistory.symbol,
            SecuritySymbolHistory.start_date,
            SecuritySymbolHistory.end_date,
        )
        for sec_id, sym_raw, start_date, end_date in session.query(*hist_cols).all():
            row = rows_by_id.get(sec_id)
            if row is None:
                continue
            sym = _norm_symbol(sym_raw)
            if sym is None:
                continue
            self._by_history_symbol.setdefault(sym, []).append((row, start_date, end_date))

    # ------------------------------------------------------------------ #
    # 测试用构造器
    # ------------------------------------------------------------------ #
    @classmethod
    def _from_indexes(
        cls,
        by_figi: dict,
        by_cik: dict,
        by_symbol: dict,
        by_symbol_all: dict,
        by_history_symbol: dict,
    ) -> "SecurityIdentityResolver":
        """测试用：跳过数据库加载，直接注入索引。"""
        obj = object.__new__(cls)
        # 从 by_symbol_all 构建 by_id
        by_id: dict[int, _SecurityRow] = {}
        for rows in by_symbol_all.values():
            for row in rows:
                by_id[row.id] = row
        obj._by_id = by_id
        obj._by_figi = by_figi
        obj._by_cik = by_cik
        obj._by_symbol = by_symbol
        obj._by_symbol_all = by_symbol_all
        obj._by_history_symbol = by_history_symbol
        return obj

    # ------------------------------------------------------------------ #
    # 解析
    # ------------------------------------------------------------------ #
    def resolve(
        self,
        *,
        symbol: str,
        composite_figi: str | None = None,
        share_class_figi: str | None = None,
        cik: str | None = None,
        exchange: str | None = None,
    ) -> ResolutionResult:
        """解析一行 incoming 数据到一个 security 身份。

        优先级：FIGI -> CIK -> 活跃 symbol -> 历史 symbol -> 新上市。
        返回的 ``ResolutionResult`` 携带匹配类型、置信度，以及改名/回收标记。
        CIK / symbol 命中后都会用 ``_identity_conflict`` 复核 incoming 的
        FIGI/CIK：冲突时一律降级为 LOW + is_recycle=True（绝不判 rename），
        让上游走 quarantine/skip 而非自动改名。
        """
        norm_symbol = _norm_symbol(symbol)
        norm_figi = _norm_figi(composite_figi)
        norm_cik = _norm_cik(cik)
        norm_exchange = (exchange or "").strip() or None

        # 1) FIGI：最强信号。
        if norm_figi is not None:
            hit = self._by_figi.get(norm_figi)
            if hit is not None:
                is_rename = _norm_symbol(hit.symbol) != norm_symbol
                return ResolutionResult(
                    security_id=hit.id,
                    resolution_type="FIGI",
                    confidence="HIGH",
                    matched_field="composite_figi",
                    is_rename=is_rename,
                    is_recycle=False,
                )

        # 2) CIK：FIGI 未命中时退而求其次。
        if norm_cik is not None:
            candidates = self._by_cik.get(norm_cik)
            if candidates:
                hit = None
                confidence = "HIGH"
                if len(candidates) == 1:
                    hit = candidates[0]
                elif norm_exchange is not None:
                    # 多个候选：尝试用交易所消歧。
                    exchange_matches = [
                        c for c in candidates if (c.exchange or "").strip() == norm_exchange
                    ]
                    if len(exchange_matches) == 1:
                        hit = exchange_matches[0]
                        confidence = "MEDIUM"
                if hit is not None:
                    # incoming FIGI 与既有行 FIGI 冲突：多半是同 CIK 下的另一只
                    # 证券（新 share class / 同 trust 新 ETF），不能判 rename——
                    # 与 ACTIVE_SYMBOL 分支的冲突处理一致，降级交 quarantine/人工。
                    if self._identity_conflict(hit, norm_figi, None):
                        return ResolutionResult(
                            security_id=hit.id,
                            resolution_type="CIK",
                            confidence="LOW",
                            matched_field="cik",
                            is_rename=False,
                            is_recycle=True,
                        )
                    is_rename = _norm_symbol(hit.symbol) != norm_symbol
                    return ResolutionResult(
                        security_id=hit.id,
                        resolution_type="CIK",
                        confidence=confidence,
                        matched_field="cik",
                        is_rename=is_rename,
                        is_recycle=False,
                    )
                # 无法消歧：落到 symbol 匹配。

        # 3) 活跃 symbol。
        if norm_symbol is not None:
            hit = self._by_symbol.get(norm_symbol)
            if hit is not None:
                conflict = self._identity_conflict(hit, norm_figi, norm_cik)
                if conflict:
                    return ResolutionResult(
                        security_id=hit.id,
                        resolution_type="ACTIVE_SYMBOL",
                        confidence="LOW",
                        matched_field="symbol",
                        is_rename=False,
                        is_recycle=True,
                    )
                return ResolutionResult(
                    security_id=hit.id,
                    resolution_type="ACTIVE_SYMBOL",
                    confidence="HIGH",
                    matched_field="symbol",
                    is_rename=False,
                    is_recycle=False,
                )

        # 4) 历史 symbol：老代码在 feed 里又出现了。
        if norm_symbol is not None:
            hist = self._by_history_symbol.get(norm_symbol)
            if hist:
                hit = self._select_history_candidate(hist)
                if hit is None:
                    # 多行指向多个不同身份且 end_date 无法消歧：不绑定任何
                    # 既有身份、不判 rename，交人工（dry_run 进 ambiguous）。
                    return ResolutionResult(
                        security_id=-1,
                        resolution_type="HISTORY_SYMBOL",
                        confidence="LOW",
                        matched_field="symbol",
                        is_rename=False,
                        is_recycle=False,
                    )
                if self._identity_conflict(hit, norm_figi, norm_cik):
                    return ResolutionResult(
                        security_id=hit.id,
                        resolution_type="HISTORY_SYMBOL",
                        confidence="LOW",
                        matched_field="symbol",
                        is_rename=False,
                        is_recycle=True,
                    )
                return ResolutionResult(
                    security_id=hit.id,
                    resolution_type="HISTORY_SYMBOL",
                    confidence="MEDIUM",
                    matched_field="symbol",
                    is_rename=False,
                    is_recycle=False,
                )

        # 5) 全部未命中：新上市。若该 symbol 仍挂在某 inactive 行名下（老代码
        #    退市后被新股复用——"死票回收"），带上 recycled_from 供上游写
        #    RECYCLE 审计事件；此场景旧行已 inactive、不占活跃 symbol 唯一索引，
        #    新行照常插入，与 quarantine 型回收（active 行冲突）不同。
        recycled_from = None
        if norm_symbol is not None:
            inactive_holders = [
                row for row in self._by_symbol_all.get(norm_symbol, []) if not row.is_active
            ]
            if len(inactive_holders) == 1:
                recycled_from = inactive_holders[0].id
        return ResolutionResult(
            security_id=-1,
            resolution_type="NEW",
            confidence="HIGH",
            matched_field="",
            is_rename=False,
            is_recycle=False,
            recycled_from=recycled_from,
        )

    @staticmethod
    def _identity_conflict(
        row: _SecurityRow, incoming_figi: str | None, incoming_cik: str | None
    ) -> bool:
        """incoming 的 FIGI/CIK 与既有行不一致，说明命中的行并非同一身份。"""
        if incoming_figi is not None:
            existing_figi = _norm_figi(row.composite_figi)
            if existing_figi is not None and existing_figi != incoming_figi:
                return True
        if incoming_cik is not None:
            existing_cik = _norm_cik(row.cik)
            if existing_cik is not None and existing_cik != incoming_cik:
                return True
        return False

    @staticmethod
    def _select_history_candidate(
        hist: list[tuple[_SecurityRow, date | None, date | None]],
    ) -> _SecurityRow | None:
        """从同一历史 symbol 的多行区间里确定性地选出一个身份。

        区间语义为 (symbol, start_date=生效日, end_date=失效日/NULL)：按
        end_date 最近者优先，NULL（未闭合，视为仍在用）排最前。存量数据混有
        两套矛盾的 start_date 写法且 end_date 大量未闭合，所以 start_date
        不参与排序；最优 end_date 上若并列多个不同 security_id（典型即多行
        NULL），视为无法消歧，返回 None。
        """
        ids = {row.id for row, _, _ in hist}
        if len(ids) == 1:
            return hist[0][0]

        def _recency(entry: tuple[_SecurityRow, date | None, date | None]):
            end_date = entry[2]
            return (end_date is None, end_date or date.min)

        best_key = max(_recency(entry) for entry in hist)
        top_ids = {entry[0].id for entry in hist if _recency(entry) == best_key}
        if len(top_ids) > 1:
            return None
        top_id = next(iter(top_ids))
        return next(entry[0] for entry in hist if entry[0].id == top_id)

    # ------------------------------------------------------------------ #
    # 批量
    # ------------------------------------------------------------------ #
    def resolve_batch(
        self,
        rows: list[dict],
        *,
        symbol_key: str = "symbol",
        figi_key: str = "composite_figi",
        cik_key: str = "cik",
        exchange_key: str = "exchange",
    ) -> list[ResolutionResult]:
        """批量解析，每个 input row 对应一个 ResolutionResult。"""
        return [
            self.resolve(
                symbol=row[symbol_key],
                composite_figi=row.get(figi_key),
                cik=row.get(cik_key),
                exchange=row.get(exchange_key),
            )
            for row in rows
        ]

    def dry_run_report(
        self,
        rows: list[dict],
        *,
        symbol_key: str = "symbol",
        figi_key: str = "composite_figi",
        cik_key: str = "cik",
        exchange_key: str = "exchange",
    ) -> dict:
        """生成一批 incoming 数据的解析预演汇总（不写库）。

        返回 dict::

            {
                'total': int,
                'by_type': Counter,             # resolution_type 计数
                'renames': [(symbol, security_id, existing_symbol), ...],
                'recycles': [(symbol, security_id, conflict_detail), ...],
                'new_listings': [symbol, ...],
                'ambiguous': [(symbol, detail), ...],
            }
        """
        by_type: Counter = Counter()
        renames: list[tuple] = []
        recycles: list[tuple] = []
        new_listings: list = []
        ambiguous: list[tuple] = []

        results = self.resolve_batch(
            rows,
            symbol_key=symbol_key,
            figi_key=figi_key,
            cik_key=cik_key,
            exchange_key=exchange_key,
        )
        for row, result in zip(rows, results):
            symbol = row[symbol_key]
            by_type[result.resolution_type] += 1
            if result.resolution_type == "NEW":
                new_listings.append(symbol)
            if result.is_rename:
                existing = self._existing_symbol(result.security_id)
                renames.append((symbol, result.security_id, existing))
            if result.is_recycle:
                detail = (
                    f"incoming figi={row.get(figi_key)} cik={row.get(cik_key)} "
                    f"!= existing identity of security_id={result.security_id}"
                )
                recycles.append((symbol, result.security_id, detail))
            if result.confidence == "LOW" and not result.is_recycle:
                ambiguous.append((symbol, f"low-confidence {result.resolution_type} match"))

        return {
            "total": len(rows),
            "by_type": by_type,
            "renames": renames,
            "recycles": recycles,
            "new_listings": new_listings,
            "ambiguous": ambiguous,
        }

    def _existing_symbol(self, security_id: int) -> str | None:
        """从已加载索引里反查 security_id 当前的 symbol。"""
        row = self._by_id.get(security_id)
        return row.symbol if row else None
