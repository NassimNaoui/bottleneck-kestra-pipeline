"""Commandes du pipeline Bottleneck exécutées localement ou par Kestra."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


# Le dossier de données ./bottleneck masque sinon le package optionnel homonyme
# que Pandas tente de détecter lors de son import depuis la racine du dépôt.
_repository_root = Path(__file__).resolve().parents[2]
_original_sys_path = sys.path[:]
sys.path = [
    item
    for item in sys.path
    if Path(item or ".").resolve() != _repository_root
]
try:
    import pandas as pd
finally:
    sys.path = _original_sys_path

import duckdb


PROJECT_ROOT = _repository_root
DEFAULT_SQL_DIR = PROJECT_ROOT / "sql"

SOURCE_FILES = {
    "erp": "Fichier_erp.xlsx",
    "liaison": "fichier_liaison.xlsx",
    "web": "Fichier_web.xlsx",
}

REQUIRED_COLUMNS = {
    "erp": {"product_id", "onsale_web", "price", "stock_quantity", "stock_status"},
    "liaison": {"product_id", "id_web"},
    "web": {"sku", "total_sales", "post_title", "post_type", "post_modified", "guid"},
}

EXPECTED_ROW_COUNTS = {
    "erp_after_deduplication": 825,
    "liaison_after_deduplication": 825,
    "web_after_cleaning": 1428,
    "web_after_deduplication": 714,
}


def normalize_identifier(value: object) -> str | None:
    """Normalise une clé Excel sans perdre les identifiants alphanumériques."""

    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"-?\d+\.0", text):
        return text[:-2]
    return text


def _check_columns(name: str, dataframe: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS[name] - set(dataframe.columns)
    if missing:
        raise ValueError(
            f"Colonnes absentes dans {SOURCE_FILES[name]} : {', '.join(sorted(missing))}"
        )


def stage_excel_sources(input_dir: Path, staging_dir: Path) -> dict[str, dict[str, int]]:
    """Convertit les classeurs Excel en CSV bruts lisibles par DuckDB."""

    input_dir = input_dir.resolve()
    staging_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, dict[str, int]] = {}

    for source_name, file_name in SOURCE_FILES.items():
        file_path = input_dir / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"Fichier source introuvable : {file_path}")

        dataframe = pd.read_excel(file_path, engine="openpyxl", dtype=object)
        dataframe.columns = [str(column).strip() for column in dataframe.columns]
        _check_columns(source_name, dataframe)

        for identifier in ("product_id", "id_web", "sku"):
            if identifier in dataframe.columns:
                dataframe[identifier] = dataframe[identifier].map(normalize_identifier)

        output_path = staging_dir / f"{source_name}.csv"
        dataframe.to_csv(output_path, index=False, encoding="utf-8", lineterminator="\n")
        manifest[source_name] = {
            "rows": int(len(dataframe)),
            "columns": int(len(dataframe.columns)),
            "full_row_duplicates": int(dataframe.duplicated().sum()),
        }

    (staging_dir / "staging_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest


@contextmanager
def working_directory(path: Path) -> Iterable[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def resolve_from_work_dir(path: Path, work_dir: Path) -> Path:
    return path if path.is_absolute() else (work_dir / path).resolve()


def execute_sql_script(database: Path, script: Path, work_dir: Path) -> None:
    """Exécute un fichier SQL DuckDB dans le répertoire de travail partagé."""

    work_dir = work_dir.resolve()
    database = resolve_from_work_dir(database, work_dir)
    script = script.resolve()
    database.parent.mkdir(parents=True, exist_ok=True)
    if not script.exists():
        raise FileNotFoundError(f"Script SQL introuvable : {script}")

    with working_directory(work_dir):
        with duckdb.connect(str(database)) as connection:
            connection.execute(script.read_text(encoding="utf-8"))


def segment_wines(database: Path, output_dir: Path, work_dir: Path) -> dict[str, float | int]:
    """Calcule le z-score et extrait vins premium et vins ordinaires."""

    work_dir = work_dir.resolve()
    database = resolve_from_work_dir(database, work_dir)
    output_dir = resolve_from_work_dir(output_dir, work_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(database)) as connection:
        wines = connection.execute(
            """
            SELECT
                revenue.product_id,
                revenue.id_web,
                revenue.post_title,
                revenue.price,
                revenue.total_sales,
                revenue.revenue
            FROM revenue_by_product AS revenue
            ORDER BY revenue.product_id
            """
        ).fetch_df()

        if wines.empty:
            raise ValueError("La table fusionnée est vide : impossible de calculer les z-scores.")

        mean_price = float(wines["price"].mean())
        std_price = float(wines["price"].std(ddof=0))
        if math.isclose(std_price, 0.0):
            raise ValueError("L'écart type des prix est nul : le z-score n'est pas défini.")

        wines["z_score"] = (wines["price"] - mean_price) / std_price
        wines["category"] = wines["z_score"].gt(2).map(
            {True: "premium", False: "ordinaire"}
        )
        wines["wine_type"] = wines["z_score"].gt(2).map(
            {True: "millésime", False: "ordinaire"}
        )

        connection.register("segmentation_dataframe", wines)
        connection.execute(
            "CREATE OR REPLACE TABLE wine_segmentation AS "
            "SELECT * FROM segmentation_dataframe"
        )
        connection.unregister("segmentation_dataframe")

    export_columns = [
        "product_id",
        "id_web",
        "post_title",
        "price",
        "total_sales",
        "revenue",
        "z_score",
        "category",
        "wine_type",
    ]
    premium = wines.loc[wines["category"] == "premium", export_columns].copy()
    ordinary = wines.loc[wines["category"] == "ordinaire", export_columns].copy()
    for dataframe in (premium, ordinary):
        dataframe["price"] = dataframe["price"].round(2)
        dataframe["revenue"] = dataframe["revenue"].round(2)
        dataframe["z_score"] = dataframe["z_score"].round(6)

    premium.sort_values("z_score", ascending=False).to_csv(
        output_dir / "vins_premium.csv", index=False, encoding="utf-8-sig"
    )
    ordinary.sort_values("product_id").to_csv(
        output_dir / "vins_ordinaires.csv", index=False, encoding="utf-8-sig"
    )

    metrics: dict[str, float | int] = {
        "mean_price": round(mean_price, 6),
        "population_standard_deviation": round(std_price, 6),
        "premium_count": int(len(premium)),
        "ordinary_count": int(len(ordinary)),
    }
    (output_dir / "zscore_metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return metrics


def _write_legacy_xls(
    report_path: Path, revenue: pd.DataFrame, summary: dict[str, float | int]
) -> None:
    import xlwt

    workbook = xlwt.Workbook(encoding="utf-8")
    summary_sheet = workbook.add_sheet("Synthèse")
    detail_sheet = workbook.add_sheet("CA par produit")

    title_style = xlwt.easyxf(
        "font: bold on, height 320, colour white;"
        "pattern: pattern solid, fore_colour dark_blue;"
        "align: horiz center, vert center;"
    )
    label_style = xlwt.easyxf(
        "font: bold on, colour white; pattern: pattern solid, fore_colour blue_gray;"
    )
    header_style = xlwt.easyxf(
        "font: bold on, colour white; pattern: pattern solid, fore_colour dark_blue;"
        "align: horiz center; borders: bottom thin, bottom_colour gray40;"
    )
    currency_style = xlwt.easyxf(num_format_str='€ #,##0.00')
    integer_style = xlwt.easyxf(num_format_str='#,##0')

    summary_sheet.write_merge(0, 0, 0, 5, "Rapport du chiffre d'affaires", title_style)
    summary_rows = [
        ("Chiffre d'affaires total", float(summary["total_revenue"]), currency_style),
        ("Nombre de produits", int(summary["product_count"]), integer_style),
        ("Unités vendues", int(summary["bottles_sold"]), integer_style),
    ]
    for row_index, (label, value, style) in enumerate(summary_rows, start=2):
        summary_sheet.write(row_index, 0, label, label_style)
        summary_sheet.write(row_index, 1, value, style)
    summary_sheet.write(7, 0, "Source")
    summary_sheet.write(7, 1, "Fusion ERP → liaison → web ; CA = prix × ventes")
    summary_sheet.col(0).width = 9000
    summary_sheet.col(1).width = 9000

    headers = [
        "product_id",
        "id_web",
        "Produit",
        "Prix unitaire (€)",
        "Quantité vendue",
        "Chiffre d'affaires (€)",
    ]
    for column_index, header in enumerate(headers):
        detail_sheet.write(0, column_index, header, header_style)

    for row_index, row in enumerate(revenue.itertuples(index=False), start=1):
        detail_sheet.write(row_index, 0, int(row.product_id))
        detail_sheet.write(row_index, 1, str(row.id_web))
        detail_sheet.write(row_index, 2, str(row.post_title))
        detail_sheet.write(row_index, 3, float(row.price), currency_style)
        detail_sheet.write(row_index, 4, int(row.total_sales), integer_style)
        detail_sheet.write(row_index, 5, float(row.revenue), currency_style)

    for column, width in enumerate([3500, 4500, 18000, 5000, 5000, 6500]):
        detail_sheet.col(column).width = width
    detail_sheet.set_panes_frozen(True)
    detail_sheet.set_horz_split_pos(1)
    detail_sheet.set_remove_splits(True)
    workbook.save(str(report_path))


def _write_modern_xlsx(
    report_path: Path, revenue: pd.DataFrame, summary: dict[str, float | int]
) -> None:
    from openpyxl import Workbook
    from openpyxl.formatting.rule import DataBarRule
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.worksheet.table import Table, TableStyleInfo

    workbook = Workbook()
    summary_sheet = workbook.active
    summary_sheet.title = "Synthèse"
    detail_sheet = workbook.create_sheet("CA par produit")

    dark_blue = "17324D"
    medium_blue = "365B73"
    gold = "D6A84B"
    light_blue = "EAF1F5"
    white = "FFFFFF"
    thin_gray = Side(style="thin", color="D7E0E5")

    summary_sheet.sheet_view.showGridLines = False
    summary_sheet.merge_cells("A1:F1")
    summary_sheet["A1"] = "Rapport du chiffre d'affaires"
    summary_sheet["A1"].font = Font(name="Aptos Display", size=18, bold=True, color=white)
    summary_sheet["A1"].fill = PatternFill("solid", fgColor=dark_blue)
    summary_sheet["A1"].alignment = Alignment(horizontal="center", vertical="center")
    summary_sheet.row_dimensions[1].height = 32

    cards = [
        ("A3", "Chiffre d'affaires total", float(summary["total_revenue"]), '€ #,##0.00'),
        ("C3", "Nombre de produits", int(summary["product_count"]), '#,##0'),
        ("E3", "Unités vendues", int(summary["bottles_sold"]), '#,##0'),
    ]
    for anchor, label, value, number_format in cards:
        cell = summary_sheet[anchor]
        cell.value = label
        cell.font = Font(name="Aptos", size=10, bold=True, color=medium_blue)
        value_cell = summary_sheet.cell(row=cell.row + 1, column=cell.column)
        value_cell.value = value
        value_cell.number_format = number_format
        value_cell.font = Font(name="Aptos Display", size=16, bold=True, color=dark_blue)
        for row in (cell.row, cell.row + 1):
            for column in (cell.column, cell.column + 1):
                target = summary_sheet.cell(row=row, column=column)
                target.fill = PatternFill("solid", fgColor=light_blue)
                target.border = Border(bottom=thin_gray)
        summary_sheet.merge_cells(
            start_row=cell.row,
            start_column=cell.column,
            end_row=cell.row,
            end_column=cell.column + 1,
        )
        summary_sheet.merge_cells(
            start_row=cell.row + 1,
            start_column=cell.column,
            end_row=cell.row + 1,
            end_column=cell.column + 1,
        )

    summary_sheet["A7"] = "Méthode"
    summary_sheet["A7"].font = Font(bold=True, color=dark_blue)
    summary_sheet["A8"] = "Fusion ERP → liaison → web ; chiffre d'affaires = prix ERP × ventes web."
    summary_sheet.merge_cells("A8:F8")
    summary_sheet["A8"].alignment = Alignment(wrap_text=True)
    for column in range(1, 7):
        summary_sheet.column_dimensions[chr(64 + column)].width = 16

    detail_sheet.sheet_view.showGridLines = False
    detail_sheet.merge_cells("A1:F1")
    detail_sheet["A1"] = "Chiffre d'affaires par produit"
    detail_sheet["A1"].font = Font(name="Aptos Display", size=16, bold=True, color=white)
    detail_sheet["A1"].fill = PatternFill("solid", fgColor=dark_blue)
    detail_sheet["A1"].alignment = Alignment(horizontal="center")
    detail_sheet.row_dimensions[1].height = 28

    headers = [
        "product_id",
        "id_web",
        "Produit",
        "Prix unitaire (€)",
        "Quantité vendue",
        "Chiffre d'affaires (€)",
    ]
    detail_sheet.append([])
    detail_sheet.append(headers)
    for row in revenue.itertuples(index=False):
        detail_sheet.append(
            [
                int(row.product_id),
                str(row.id_web),
                str(row.post_title),
                float(row.price),
                int(row.total_sales),
                float(row.revenue),
            ]
        )

    last_row = len(revenue) + 3
    table = Table(displayName="RevenueByProduct", ref=f"A3:F{last_row}")
    table.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium2",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False,
    )
    detail_sheet.add_table(table)
    detail_sheet.freeze_panes = "A4"
    detail_sheet.column_dimensions["A"].width = 14
    detail_sheet.column_dimensions["B"].width = 16
    detail_sheet.column_dimensions["C"].width = 72
    detail_sheet.column_dimensions["D"].width = 19
    detail_sheet.column_dimensions["E"].width = 19
    detail_sheet.column_dimensions["F"].width = 24
    for row in range(4, last_row + 1):
        detail_sheet.cell(row, 4).number_format = '€ #,##0.00'
        detail_sheet.cell(row, 5).number_format = '#,##0'
        detail_sheet.cell(row, 6).number_format = '€ #,##0.00'
    detail_sheet.conditional_formatting.add(
        f"F4:F{last_row}",
        DataBarRule(start_type="min", end_type="max", color=gold, showValue=True),
    )

    workbook.save(report_path)


def export_revenue_reports(database: Path, output_dir: Path, work_dir: Path) -> dict[str, float | int]:
    """Exporte le CA par produit et le CA global en formats Excel moderne et legacy."""

    work_dir = work_dir.resolve()
    database = resolve_from_work_dir(database, work_dir)
    output_dir = resolve_from_work_dir(output_dir, work_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(database), read_only=True) as connection:
        revenue = connection.execute(
            "SELECT * FROM revenue_by_product ORDER BY revenue DESC, product_id"
        ).fetch_df()
        summary_row = connection.execute("SELECT * FROM revenue_summary").fetchone()

    if summary_row is None:
        raise ValueError("La synthèse du chiffre d'affaires est absente.")
    summary: dict[str, float | int] = {
        "product_count": int(summary_row[0]),
        "bottles_sold": int(summary_row[1]),
        "total_revenue": float(summary_row[2]),
    }
    _write_legacy_xls(output_dir / "rapport_chiffre_affaires.xls", revenue, summary)
    _write_modern_xlsx(output_dir / "rapport_chiffre_affaires.xlsx", revenue, summary)
    return summary


def validate_pipeline(database: Path, output_dir: Path, work_dir: Path) -> dict[str, object]:
    """Contrôle les clés, la réconciliation et les partitions de sortie."""

    work_dir = work_dir.resolve()
    database = resolve_from_work_dir(database, work_dir)
    output_dir = resolve_from_work_dir(output_dir, work_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(database), read_only=True) as connection:
        row_counts = {
            "erp_after_deduplication": int(
                connection.execute("SELECT COUNT(*) FROM erp_clean").fetchone()[0]
            ),
            "liaison_after_deduplication": int(
                connection.execute("SELECT COUNT(*) FROM liaison_clean").fetchone()[0]
            ),
            "web_after_cleaning": int(
                connection.execute("SELECT COUNT(*) FROM web_not_null").fetchone()[0]
            ),
            "web_after_deduplication": int(
                connection.execute("SELECT COUNT(*) FROM web_clean").fetchone()[0]
            ),
        }
        merged_count = int(connection.execute("SELECT COUNT(*) FROM merged_wines").fetchone()[0])
        duplicate_products = int(
            connection.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM merged_wines"
            ).fetchone()[0]
        )
        duplicate_web_ids = int(
            connection.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT id_web) FROM merged_wines"
            ).fetchone()[0]
        )
        null_business_fields = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM merged_wines
                WHERE product_id IS NULL OR id_web IS NULL OR post_title IS NULL
                   OR price IS NULL OR total_sales IS NULL
                """
            ).fetchone()[0]
        )
        segmentation_count = int(
            connection.execute("SELECT COUNT(*) FROM wine_segmentation").fetchone()[0]
        )
        premium_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM wine_segmentation WHERE category = 'premium'"
            ).fetchone()[0]
        )
        ordinary_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM wine_segmentation WHERE category = 'ordinaire'"
            ).fetchone()[0]
        )
        total_from_detail = float(
            connection.execute("SELECT ROUND(SUM(revenue), 2) FROM revenue_by_product").fetchone()[0]
        )
        total_from_summary = float(
            connection.execute("SELECT total_revenue FROM revenue_summary").fetchone()[0]
        )

    row_count_tests = {
        name: {
            "actual": row_counts[name],
            "expected": expected,
            "passed": row_counts[name] == expected,
        }
        for name, expected in EXPECTED_ROW_COUNTS.items()
    }
    checks = {
        **{
            f"{name}_matches_expected": test["passed"]
            for name, test in row_count_tests.items()
        },
        "merged_table_not_empty": merged_count > 0,
        "unique_product_id": duplicate_products == 0,
        "unique_id_web": duplicate_web_ids == 0,
        "required_fields_not_null": null_business_fields == 0,
        "segmentation_is_complete": segmentation_count == merged_count,
        "segments_are_exhaustive": premium_count + ordinary_count == merged_count,
        "revenue_reconciles": math.isclose(
            total_from_detail, total_from_summary, abs_tol=0.01
        ),
    }
    report: dict[str, object] = {
        "status": "passed" if all(checks.values()) else "failed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            **row_counts,
            "merged_count": merged_count,
            "premium_count": premium_count,
            "ordinary_count": ordinary_count,
            "total_revenue": total_from_summary,
        },
        "row_count_tests": row_count_tests,
        "checks": checks,
    }
    (output_dir / "quality_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if report["status"] != "passed":
        failed = [name for name, passed in checks.items() if not passed]
        raise ValueError(f"Contrôles qualité en échec : {', '.join(failed)}")
    return report


def run_pipeline(input_dir: Path, work_dir: Path, sql_dir: Path = DEFAULT_SQL_DIR) -> None:
    """Exécute le workflow complet hors Kestra, utile pour les tests et le debug."""

    work_dir.mkdir(parents=True, exist_ok=True)
    stage_excel_sources(input_dir, work_dir / "staging")
    database = Path("pipeline.duckdb")
    for script_name in (
        "00_load_raw.sql",
        "01_remove_missing_values.sql",
        "02_deduplicate.sql",
        "03_merge_systems.sql",
        "04_calculate_revenue.sql",
    ):
        execute_sql_script(database, sql_dir / script_name, work_dir)
    segment_wines(database, Path("outputs"), work_dir)
    export_revenue_reports(database, Path("outputs"), work_dir)
    validate_pipeline(database, Path("outputs"), work_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    stage = subparsers.add_parser("stage", help="Convertir les sources Excel en CSV bruts")
    stage.add_argument("--input-dir", type=Path, required=True)
    stage.add_argument("--staging-dir", type=Path, default=Path("staging"))

    sql = subparsers.add_parser("sql", help="Exécuter un script SQL DuckDB")
    sql.add_argument("--database", type=Path, default=Path("pipeline.duckdb"))
    sql.add_argument("--script", type=Path, required=True)
    sql.add_argument("--work-dir", type=Path, default=Path("."))

    segment = subparsers.add_parser("segment", help="Calculer les z-scores et exporter les segments")
    segment.add_argument("--database", type=Path, default=Path("pipeline.duckdb"))
    segment.add_argument("--output-dir", type=Path, default=Path("outputs"))
    segment.add_argument("--work-dir", type=Path, default=Path("."))

    export = subparsers.add_parser("export", help="Exporter les rapports de chiffre d'affaires")
    export.add_argument("--database", type=Path, default=Path("pipeline.duckdb"))
    export.add_argument("--output-dir", type=Path, default=Path("outputs"))
    export.add_argument("--work-dir", type=Path, default=Path("."))

    validate = subparsers.add_parser("validate", help="Exécuter les contrôles qualité")
    validate.add_argument("--database", type=Path, default=Path("pipeline.duckdb"))
    validate.add_argument("--output-dir", type=Path, default=Path("outputs"))
    validate.add_argument("--work-dir", type=Path, default=Path("."))

    run = subparsers.add_parser("run", help="Exécuter le pipeline complet sans Kestra")
    run.add_argument("--input-dir", type=Path, required=True)
    run.add_argument("--work-dir", type=Path, default=Path("build"))
    run.add_argument("--sql-dir", type=Path, default=DEFAULT_SQL_DIR)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.command == "stage":
        print(json.dumps(stage_excel_sources(args.input_dir, args.staging_dir), indent=2))
    elif args.command == "sql":
        execute_sql_script(args.database, args.script, args.work_dir)
        print(f"Script exécuté : {args.script}")
    elif args.command == "segment":
        print(json.dumps(segment_wines(args.database, args.output_dir, args.work_dir), indent=2))
    elif args.command == "export":
        print(json.dumps(export_revenue_reports(args.database, args.output_dir, args.work_dir), indent=2))
    elif args.command == "validate":
        print(json.dumps(validate_pipeline(args.database, args.output_dir, args.work_dir), indent=2))
    elif args.command == "run":
        run_pipeline(args.input_dir, args.work_dir, args.sql_dir)
        print(f"Pipeline terminé. Sorties : {(args.work_dir / 'outputs').resolve()}")


if __name__ == "__main__":
    main()
