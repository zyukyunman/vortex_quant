from __future__ import annotations

import json

import pandas as pd
import pytest

from vortex.strategy.opening_liquidity import (
    OpeningLiquidityConfig,
    analyze_opening_ask1_capacity,
    load_opening_snapshots,
    write_opening_liquidity_report,
)


def test_load_opening_snapshots_supports_json_and_lots(tmp_path):
    path = tmp_path / "snapshots.json"
    path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "date": "20240103",
                        "symbol": "000001.SZ",
                        "open_price": 10.0,
                        "ask1_price": 10.01,
                        "ask1_volume": 3,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    frame = load_opening_snapshots(path, config=OpeningLiquidityConfig(volume_unit="lots", lot_size=100))

    assert frame.loc[0, "ask1_shares"] == 300
    assert frame.loc[0, "ask1_notional"] == pytest.approx(300 * 10.01)


def test_load_opening_snapshots_supports_downloaded_auction_dataset_dir(tmp_path):
    dataset_dir = tmp_path / "stk_auction_o" / "date=20240103"
    dataset_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "date": "20240103",
                "symbol": "000001.SZ",
                "close": 10.05,
                "volume": 2300,
            }
        ]
    ).to_parquet(dataset_dir / "part-000.parquet", index=False)

    frame = load_opening_snapshots(tmp_path / "stk_auction_o")

    assert frame.loc[0, "open_price"] == pytest.approx(10.05)
    assert frame.loc[0, "ask1_price"] == pytest.approx(10.05)
    assert frame.loc[0, "ask1_shares"] == 2300
    assert frame.loc[0, "ask1_notional"] == pytest.approx(2300 * 10.05)


def test_analyze_opening_ask1_capacity_reports_one_lot_and_target_feasibility():
    intents = pd.DataFrame(
        [
            {
                "date": "20240103",
                "symbol": "000001.SZ",
                "side": "buy",
                "requested_shares": 500,
                "requested_notional": 5000.0,
            },
            {
                "date": "20240103",
                "symbol": "000002.SZ",
                "side": "buy",
                "requested_shares": 100,
                "requested_notional": 2000.0,
            },
        ]
    )
    snapshots = pd.DataFrame(
        [
            {"date": "20240103", "symbol": "000001.SZ", "open_price": 10.0, "ask1_price": 10.0, "ask1_volume": 300},
            {"date": "20240103", "symbol": "000002.SZ", "open_price": 20.0, "ask1_price": 20.0, "ask1_volume": 100},
        ]
    )

    report = analyze_opening_ask1_capacity(intents, snapshots)

    order_level = report.order_level.set_index("symbol")
    assert bool(order_level.loc["000001.SZ", "one_lot_feasible"]) is True
    assert bool(order_level.loc["000001.SZ", "target_feasible"]) is False
    assert int(order_level.loc["000001.SZ", "covered_shares"]) == 300
    assert bool(order_level.loc["000002.SZ", "target_feasible"]) is True
    assert report.overall_summary["one_lot_feasible_rate"] == pytest.approx(1.0)
    assert report.overall_summary["target_feasible_rate"] == pytest.approx(0.5)


def test_write_opening_liquidity_report_writes_all_artifacts(tmp_path):
    intents = pd.DataFrame(
        [{"date": "20240103", "symbol": "000001.SZ", "side": "buy", "requested_shares": 100, "requested_notional": 1000.0}]
    )
    snapshots = pd.DataFrame(
        [{"date": "20240103", "symbol": "000001.SZ", "open_price": 10.0, "ask1_price": 10.0, "ask1_volume": 100}]
    )
    report = analyze_opening_ask1_capacity(intents, snapshots)

    paths = write_opening_liquidity_report(report, output_dir=tmp_path, stem="test-opening")

    assert paths["csv_path"].exists()
    assert paths["json_path"].exists()
    assert paths["md_path"].exists()
