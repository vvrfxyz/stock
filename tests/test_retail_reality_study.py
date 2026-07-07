"""wave-10b 散户集中度模拟的持仓延续抽样（_pick_with_continuity）单元测试。

锁定 2026-07-07 修复语义：保留仍在 q5 的旧持仓、只随机补充离场者——
每期独立重抽是首版 bug（虚增年换手到 24 倍）。
"""
from __future__ import annotations

import numpy as np

from research.retail_reality_study import _pick_with_continuity


def _members(rows: list[list[int]], n_cols: int) -> np.ndarray:
    mat = np.zeros((len(rows), n_cols), dtype=bool)
    for r, cols in enumerate(rows):
        mat[r, cols] = True
    return mat


def test_holdings_persist_while_still_members():
    # 成员集不变时持仓一只不换（换手为零），且张数 = min(holdings, 成员数)
    members = _members([[0, 1, 2, 3, 4]] * 4, n_cols=6)
    picks = _pick_with_continuity(members, holdings=3, rng=np.random.default_rng(0))
    assert picks.sum(axis=1).tolist() == [3, 3, 3, 3]
    for r in range(1, len(members)):
        assert (picks[r] == picks[0]).all()


def test_only_leavers_are_replaced():
    # 第二期 0/1 离场：2 必须保留，补充的两只来自新成员集且无重复
    members = _members([[0, 1, 2], [2, 3, 4, 5]], n_cols=6)
    picks = _pick_with_continuity(members, holdings=3, rng=np.random.default_rng(0))
    assert set(np.flatnonzero(picks[0])) == {0, 1, 2}
    second = np.flatnonzero(picks[1])
    assert 2 in second
    assert set(second) <= {2, 3, 4, 5}
    assert len(second) == 3  # bool 矩阵天然无重复，张数补齐到 holdings


def test_pool_smaller_than_holdings_takes_all():
    members = _members([[0, 1]] * 2, n_cols=4)
    picks = _pick_with_continuity(members, holdings=30, rng=np.random.default_rng(0))
    assert picks.sum(axis=1).tolist() == [2, 2]


def test_empty_row_skipped_and_holdings_survive_gap():
    # 空成员期不建仓；此前持仓在下一有效期若仍在成员集则延续
    members = _members([[0, 1, 2], [], [0, 1, 2]], n_cols=4)
    picks = _pick_with_continuity(members, holdings=2, rng=np.random.default_rng(0))
    assert picks[1].sum() == 0
    assert (picks[2] == picks[0]).all()


def test_stale_holdings_never_picked_outside_members():
    # 每期 picks 严格是当期成员子集（陈旧持仓不会越界残留）
    rng = np.random.default_rng(7)
    members = rng.random((8, 20)) < 0.4
    picks = _pick_with_continuity(members, holdings=5, rng=rng)
    assert not (picks & ~members).any()
    take = np.minimum(members.sum(axis=1), 5)
    assert (picks.sum(axis=1) == take).all()
