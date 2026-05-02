from __future__ import annotations

from vortex.research.data_audit import audit_research_datasets, missing_research_datasets


def test_audit_research_datasets_detects_available_dataset(tmp_path):
    dataset = tmp_path / "adj_factor" / "date=20200101"
    dataset.mkdir(parents=True)
    (dataset / "part.parquet").write_bytes(b"not-a-real-parquet")

    items = audit_research_datasets(tmp_path, granted_permissions=set())
    adj = next(item for item in items if item.dataset == "adj_factor")

    assert adj.available
    assert adj.parquet_files == 1
    assert "字段口径" in adj.next_action


def test_audit_research_datasets_marks_minutes_as_permission_gated(tmp_path):
    items = audit_research_datasets(tmp_path, granted_permissions=set())
    minute = next(item for item in items if item.dataset == "stk_mins")

    assert not minute.available
    assert minute.permission_key == "stock_minutes"
    assert minute.permission_granted is False
    assert "缺少独立权限" in minute.next_action


def test_audit_research_datasets_marks_permission_granted_when_configured(tmp_path):
    items = audit_research_datasets(tmp_path, granted_permissions={"stock_minutes"})
    minute = next(item for item in items if item.dataset == "stk_mins")

    assert minute.permission_granted is True
    assert "小样本试抓" in minute.next_action


def test_missing_research_datasets_filters_unavailable_items(tmp_path):
    dataset = tmp_path / "moneyflow"
    dataset.mkdir()
    (dataset / "part.parquet").write_bytes(b"not-a-real-parquet")

    items = audit_research_datasets(tmp_path, granted_permissions=set())
    missing = missing_research_datasets(items)

    assert "moneyflow" not in {item.dataset for item in missing}
    assert "index_daily" in {item.dataset for item in missing}
