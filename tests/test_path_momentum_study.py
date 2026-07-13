"""路径质量双排序：顺序分桶、独立分桶、交互分解与样本裁决。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.path_momentum_study import (
    _double_sort_labels,
    _window_verdict,
    spread_series,
)


def test_sequential_and_independent_double_sorts_are_distinct():
    pret = np.arange(8, dtype="float64")
    information_discreteness = np.arange(8, dtype="float64")
    eligible = np.ones(8, dtype=bool)

    seq_pret, seq_id = _double_sort_labels(
        pret,
        information_discreteness,
        eligible,
        mode="sequential",
        n_quantiles=2,
        min_cell_size=1,
    )
    independent_pret, independent_id = _double_sort_labels(
        pret,
        information_discreteness,
        eligible,
        mode="independent",
        n_quantiles=2,
        min_cell_size=1,
    )

    np.testing.assert_array_equal(seq_pret, [1, 1, 1, 1, 2, 2, 2, 2])
    np.testing.assert_array_equal(seq_id, [1, 1, 2, 2, 1, 1, 2, 2])
    np.testing.assert_array_equal(independent_pret, seq_pret)
    np.testing.assert_array_equal(independent_id, [1, 1, 1, 1, 2, 2, 2, 2])


def test_spread_series_matches_fip_interaction_decomposition():
    date = pd.Timestamp("2026-01-30")
    cell_returns = {
        (1, 1): -0.10,
        (1, 2): -0.03,
        (2, 1): 0.12,
        (2, 2): 0.05,
    }
    cells = pd.DataFrame(
        [
            {
                "date": date,
                "sort_mode": "sequential",
                "horizon": 126,
                "pret_q": pret_q,
                "id_q": id_q,
                "mean_return": value,
                "n_stocks": 20,
            }
            for (pret_q, id_q), value in cell_returns.items()
        ]
    )

    row = spread_series(cells, n_quantiles=2).iloc[0]

    assert row["momentum_id_q1"] == pytest.approx(0.22)
    assert row["momentum_id_q2"] == pytest.approx(0.08)
    assert row["winner_continuity"] == pytest.approx(0.07)
    assert row["loser_continuity"] == pytest.approx(0.07)
    assert row["fip_spread"] == pytest.approx(0.14)
    assert row["fip_spread"] == pytest.approx(
        row["winner_continuity"] + row["loser_continuity"]
    )


def _passing_summary() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sort_mode": "sequential",
                "horizon": 21,
                "fip_spread": 0.01,
                "fip_nw_t": 1.0,
                "winner_continuity": 0.01,
                "loser_continuity": 0.01,
            },
            {
                "sort_mode": "sequential",
                "horizon": 63,
                "fip_spread": 0.02,
                "fip_nw_t": 2.0,
                "winner_continuity": 0.02,
                "loser_continuity": 0.01,
            },
            {
                "sort_mode": "sequential",
                "horizon": 126,
                "fip_spread": 0.03,
                "fip_nw_t": 3.1,
                "winner_continuity": 0.02,
                "loser_continuity": 0.01,
            },
            {
                "sort_mode": "independent",
                "horizon": 126,
                "fip_spread": 0.01,
                "fip_nw_t": 1.2,
                "winner_continuity": 0.01,
                "loser_continuity": 0.00,
            },
        ]
    )


def test_primary_verdict_applies_full_preregistered_gate():
    summary = _passing_summary()

    assert _window_verdict(summary, sample_role="primary") is True
    summary.loc[
        (summary["sort_mode"] == "sequential") & (summary["horizon"] == 126),
        "fip_nw_t",
    ] = 2.99
    assert _window_verdict(summary, sample_role="primary") is False


def test_stability_verdict_requires_only_same_sign_at_126_days():
    summary = _passing_summary()
    summary = summary[
        (summary["sort_mode"] == "sequential") & (summary["horizon"] == 126)
    ].copy()
    summary["fip_nw_t"] = -4.0

    assert _window_verdict(summary, sample_role="stability") is True
    summary["fip_spread"] = -0.001
    assert _window_verdict(summary, sample_role="stability") is False
