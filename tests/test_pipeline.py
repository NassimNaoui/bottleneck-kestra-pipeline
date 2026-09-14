import subprocess
import sys

import pytest

from data_pipeline.cli import validate_pipeline
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

    with duckdb.connect(
        str(tmp_path / "pipeline.duckdb"), read_only=True
    ) as connection:
        merged_count = connection.execute(
            "SELECT COUNT(*) FROM merged_wines"
        ).fetchone()[0]
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
    assert float(total_revenue) == pytest.approx(70568.60, rel=0, abs=0.000001)
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
    expected_counts = {
        "erp_after_deduplication": 825,
        "liaison_after_deduplication": 825,
        "web_after_cleaning": 1428,
        "web_after_deduplication": 714,
        "merged_count": 714,
        "revenue_product_count": 714,
        "segmentation_count": 714,
    }

    for name, expected in expected_counts.items():
        assert quality_report["row_count_tests"][name] == {
            "actual": expected,
            "expected": expected,
            "passed": True,
        }

    assert quality_report["status"] == "passed"
    assert all(quality_report["checks"].values())

    for stage in ("cleaning", "merge", "revenue", "segmentation"):
        stage_report = json.loads(
            (tmp_path / "outputs" / f"quality_{stage}.json").read_text(encoding="utf-8")
        )
        assert stage_report["status"] == "passed"


@pytest.fixture
def reference_run(tmp_path):
    run_pipeline(
        input_dir=PROJECT_ROOT / "bottleneck",
        work_dir=tmp_path,
        sql_dir=PROJECT_ROOT / "sql",
    )
    return tmp_path


@pytest.mark.parametrize("removed_rows", [1, 14])
def test_merge_rejects_missing_rows(reference_run, removed_rows):
    database = reference_run / "pipeline.duckdb"

    with duckdb.connect(str(database)) as connection:
        connection.execute(f"""
            DELETE FROM merged_wines
            WHERE product_id IN (
                SELECT product_id
                FROM merged_wines
                ORDER BY product_id
                LIMIT {removed_rows}
            )
            """)

    with pytest.raises(ValueError, match="merged_count_matches_expected"):
        validate_pipeline(database, Path("outputs"), reference_run, stage="merge")


def test_revenue_rejects_coherent_but_wrong_total(reference_run):
    database = reference_run / "pipeline.duckdb"

    with duckdb.connect(str(database)) as connection:
        connection.execute("""
            UPDATE revenue_by_product
            SET revenue = revenue + 1
            WHERE product_id = (
                SELECT MIN(product_id) FROM revenue_by_product
            )
            """)
        connection.execute("""
            UPDATE revenue_summary
            SET total_revenue = (
                SELECT ROUND(SUM(revenue), 2)
                FROM revenue_by_product
            )
            """)

    with pytest.raises(ValueError, match="matches_reference"):
        validate_pipeline(database, Path("outputs"), reference_run, stage="revenue")

    report = json.loads(
        (reference_run / "outputs" / "quality_revenue.json").read_text(encoding="utf-8")
    )

    # Détail et synthèse restent cohérents, mais la référence est incorrecte.
    assert report["checks"]["revenue_reconciles"] is True
    assert report["checks"]["revenue_summary_matches_reference"] is False
    assert report["status"] == "failed"


@pytest.mark.parametrize(
    "source_category,target_category",
    [
        ("premium", "ordinaire"),
        ("ordinaire", "premium"),
    ],
)
def test_segmentation_rejects_29_or_31_premium(
    reference_run, source_category, target_category
):
    database = reference_run / "pipeline.duckdb"

    with duckdb.connect(str(database)) as connection:
        connection.execute(
            """
            UPDATE wine_segmentation
            SET category = ?
            WHERE product_id = (
                SELECT MIN(product_id)
                FROM wine_segmentation
                WHERE category = ?
            )
            """,
            [target_category, source_category],
        )

    with pytest.raises(ValueError, match="premium_count_matches_reference"):
        validate_pipeline(
            database, Path("outputs"), reference_run, stage="segmentation"
        )


def test_segmentation_rejects_wrong_labels_with_30_premium(reference_run):
    database = reference_run / "pipeline.duckdb"

    with duckdb.connect(str(database)) as connection:
        premium_id = connection.execute("""
            SELECT MIN(product_id) FROM wine_segmentation
            WHERE category = 'premium'
            """).fetchone()[0]
        ordinary_id = connection.execute("""
            SELECT MIN(product_id) FROM wine_segmentation
            WHERE category = 'ordinaire'
            """).fetchone()[0]

        connection.execute(
            """
            UPDATE wine_segmentation
            SET category = CASE
                WHEN product_id = ? THEN 'ordinaire'
                WHEN product_id = ? THEN 'premium'
                ELSE category
            END
            WHERE product_id IN (?, ?)
            """,
            [premium_id, ordinary_id, premium_id, ordinary_id],
        )

    with pytest.raises(ValueError, match="invalid_classification"):
        validate_pipeline(
            database, Path("outputs"), reference_run, stage="segmentation"
        )

    report = json.loads(
        (reference_run / "outputs" / "quality_segmentation.json").read_text(
            encoding="utf-8"
        )
    )
    assert report["metrics"]["premium_count"] == 30
    assert report["checks"]["segmentation_invalid_classification"] is False


@pytest.mark.parametrize(
    "mutation,failed_check",
    [
        (
            """
            INSERT INTO erp_clean
            SELECT * FROM erp_clean LIMIT 1
            """,
            "erp_clean_product_id_duplicates",
        ),
        (
            """
            UPDATE erp_clean SET price = NULL
            WHERE product_id = (
                SELECT MIN(product_id) FROM erp_clean
            )
            """,
            "erp_clean_invalid_required_fields",
        ),
    ],
)
def test_cleaning_rejects_invalid_data(reference_run, mutation, failed_check):
    database = reference_run / "pipeline.duckdb"

    with duckdb.connect(str(database)) as connection:
        connection.execute(mutation)

    with pytest.raises(ValueError, match=failed_check):
        validate_pipeline(database, Path("outputs"), reference_run, stage="cleaning")


def test_cli_returns_nonzero_exit_code_on_failed_validation(reference_run):
    database = reference_run / "pipeline.duckdb"

    with duckdb.connect(str(database)) as connection:
        connection.execute("""
            DELETE FROM merged_wines
            WHERE product_id = (
                SELECT MIN(product_id) FROM merged_wines
            )
            """)

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "src" / "data_pipeline" / "cli.py"),
            "validate",
            "--database",
            str(database),
            "--output-dir",
            "outputs",
            "--work-dir",
            str(reference_run),
            "--stage",
            "merge",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "merged_count_matches_expected" in result.stderr
