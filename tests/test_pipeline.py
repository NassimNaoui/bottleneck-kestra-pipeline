import json
from pathlib import Path

import duckdb

from data_pipeline.cli import normalize_identifier, run_pipeline
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_normalize_identifier_preserves_alphanumeric_keys() -> None:
    assert normalize_identifier(15298.0) == "15298"
    assert normalize_identifier(" 13127-1 ") == "13127-1"
    assert normalize_identifier("bon-cadeau-25-euros") == "bon-cadeau-25-euros"
    assert normalize_identifier(None) is None


def test_reference_dataset_end_to_end(tmp_path: Path) -> None:
    run_pipeline(
        input_dir=PROJECT_ROOT / "bottleneck",
        work_dir=tmp_path,
        sql_dir=PROJECT_ROOT / "sql",
    )

    with duckdb.connect(str(tmp_path / "pipeline.duckdb"), read_only=True) as connection:
        merged_count = connection.execute("SELECT COUNT(*) FROM merged_wines").fetchone()[0]
        total_revenue = connection.execute(
            "SELECT total_revenue FROM revenue_summary"
        ).fetchone()[0]
        premium_count = connection.execute(
            "SELECT COUNT(*) FROM wine_segmentation WHERE z_score > 2"
        ).fetchone()[0]
        erp_after_deduplication = connection.execute(
            "SELECT COUNT(*) FROM erp_clean"
        ).fetchone()[0]
        liaison_after_deduplication = connection.execute(
            "SELECT COUNT(*) FROM liaison_clean"
        ).fetchone()[0]
        web_after_cleaning = connection.execute(
            "SELECT COUNT(*) FROM web_not_null"
        ).fetchone()[0]
        web_after_deduplication = connection.execute(
            "SELECT COUNT(*) FROM web_clean"
        ).fetchone()[0]

    assert merged_count == 714
    assert total_revenue == 70568.60
    assert premium_count == 30
    assert erp_after_deduplication == 825
    assert liaison_after_deduplication == 825
    assert web_after_cleaning == 1428
    assert web_after_deduplication == 714

    premium = pd.read_csv(tmp_path / "outputs" / "vins_premium.csv")
    ordinary = pd.read_csv(tmp_path / "outputs" / "vins_ordinaires.csv")
    assert len(premium) == 30
    assert len(ordinary) == 684
    assert (premium["z_score"] > 2).all()
    assert (ordinary["z_score"] <= 2).all()

    assert (tmp_path / "outputs" / "rapport_chiffre_affaires.xls").exists()
    assert (tmp_path / "outputs" / "rapport_chiffre_affaires.xlsx").exists()
    assert (tmp_path / "outputs" / "quality_report.json").exists()

    quality_report = json.loads(
        (tmp_path / "outputs" / "quality_report.json").read_text(encoding="utf-8")
    )
    assert quality_report["row_count_tests"] == {
        "erp_after_deduplication": {
            "actual": 825,
            "expected": 825,
            "passed": True,
        },
        "liaison_after_deduplication": {
            "actual": 825,
            "expected": 825,
            "passed": True,
        },
        "web_after_cleaning": {
            "actual": 1428,
            "expected": 1428,
            "passed": True,
        },
        "web_after_deduplication": {
            "actual": 714,
            "expected": 714,
            "passed": True,
        },
    }
