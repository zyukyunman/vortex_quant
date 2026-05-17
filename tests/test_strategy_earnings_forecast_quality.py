from __future__ import annotations

import pandas as pd

from vortex.strategy.earnings_forecast_quality import build_holding_quality_review


def test_holding_quality_review_blocks_negative_forecast_and_flags_financial_decay():
    holdings = pd.DataFrame(
        {
            "symbol": ["000001.SZ", "300517.SZ", "603992.SH", "688308.SH"],
            "target_weight": [0.05, 0.05, 0.05, 0.05],
        }
    )
    forecast = pd.DataFrame(
        [
            {
                "symbol": "000001.SZ",
                "ann_date": "20260120",
                "type": "预增",
                "p_change_min": 100.0,
                "p_change_max": 150.0,
            },
            {
                "symbol": "300517.SZ",
                "ann_date": "20260318",
                "type": "预减",
                "p_change_min": -94.0,
                "p_change_max": -91.0,
            },
            {
                "symbol": "603992.SH",
                "ann_date": "20260318",
                "type": "预增",
                "p_change_min": 40.0,
                "p_change_max": 60.0,
            },
            {
                "symbol": "688308.SH",
                "ann_date": "20260318",
                "type": "预增",
                "p_change_min": 40.0,
                "p_change_max": 60.0,
            },
        ]
    )
    fina_indicator = pd.DataFrame(
        [
            {
                "symbol": "603992.SH",
                "ann_date": "20251028",
                "report_date": "20250930",
                "q_sales_yoy": -4.0,
                "netprofit_yoy": -20.0,
                "dt_netprofit_yoy": -22.0,
            },
            {
                "symbol": "603992.SH",
                "ann_date": "20260331",
                "report_date": "20251231",
                "q_sales_yoy": -6.0,
                "netprofit_yoy": -50.0,
                "dt_netprofit_yoy": -57.0,
            },
            {
                "symbol": "688308.SH",
                "ann_date": "20260331",
                "report_date": "20251231",
                "q_sales_yoy": 33.0,
                "netprofit_yoy": -43.0,
                "dt_netprofit_yoy": -54.0,
            },
        ]
    )

    review = build_holding_quality_review(
        holdings,
        forecast=forecast,
        fina_indicator=fina_indicator,
        as_of="20260506",
    ).set_index("symbol")

    assert review.loc["000001.SZ", "quality_label"] == "pass"
    assert review.loc["300517.SZ", "quality_label"] == "blocked"
    assert "非正业绩预告分数" in review.loc["300517.SZ", "quality_reason"]
    assert review.loc["603992.SH", "quality_label"] == "review"
    assert "营收同比连续 2 期为负" in review.loc["603992.SH", "quality_reason"]
    assert review.loc["688308.SH", "quality_label"] == "watch"
