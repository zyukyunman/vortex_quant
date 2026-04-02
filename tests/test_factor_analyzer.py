from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from vortex.analysis.analyzer import FactorAnalyzer


class TestFactorAnalyzer:
    def test_ic_report_multi_horizon(self):
        ds = MagicMock()
        fh = MagicMock()
        fh.factors = {"ridge_minutes": object()}
        analyzer = FactorAnalyzer(ds, fh)

        def mock_calc_ic(name, dates, forward_days):
            mapping = {
                1: pd.Series([0.10, 0.20], index=["20250131", "20250228"]),
                5: pd.Series([0.30, 0.25], index=["20250131", "20250228"]),
                20: pd.Series([0.15, 0.10], index=["20250131", "20250228"]),
            }
            return mapping[forward_days]

        with patch.object(analyzer, "calc_ic", side_effect=mock_calc_ic):
            df = analyzer.ic_report_multi_horizon(
                factor_names=["ridge_minutes"],
                dates=["20250131", "20250228"],
                forward_days_list=[1, 5, 20],
            )

        assert len(df) == 1
        row = df.iloc[0]
        assert row["factor"] == "ridge_minutes"
        assert row["mean_ic_1d"] == pytest.approx(0.15)
        assert row["mean_ic_5d"] == pytest.approx(0.275)
        assert row["best_horizon"] == 5

    def test_long_short_report(self):
        ds = MagicMock()
        fh = MagicMock()
        fh.factors = {"ridge_minutes": object()}
        analyzer = FactorAnalyzer(ds, fh)

        mock_ls = pd.DataFrame([
            {"date": "20250131", "long": 0.04, "short": -0.01, "long_short": 0.05},
            {"date": "20250228", "long": 0.03, "short": 0.00, "long_short": 0.03},
        ])

        with patch.object(analyzer, "calc_long_short_returns", return_value=mock_ls):
            df = analyzer.long_short_report(
                factor_names=["ridge_minutes"],
                dates=["20250131", "20250228"],
                forward_days=5,
                n_groups=5,
            )

        assert len(df) == 1
        row = df.iloc[0]
        assert row["factor"] == "ridge_minutes"
        assert abs(row["long_short_5d"] - 0.04) < 1e-9
        assert abs(row["long_mean"] - 0.035) < 1e-9
        assert abs(row["short_mean"] - (-0.005)) < 1e-9
        assert row["n_periods"] == 2
