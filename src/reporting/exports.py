from __future__ import annotations

import json
from io import BytesIO, StringIO

import pandas as pd
from pyspark.sql import DataFrame


def _quarantine_records(quarantine: DataFrame) -> list[dict]:
    return quarantine.toPandas().to_dict(orient="records")


def json_report(metrics: dict, profile: dict, quarantine: DataFrame) -> bytes:
    return json.dumps(
        {"metrics": metrics, "profile": profile, "quarantined_records": _quarantine_records(quarantine)},
        indent=2,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def csv_report(quarantine: DataFrame) -> bytes:
    return quarantine.toPandas().to_csv(index=False).encode("utf-8-sig")


def excel_report(metrics: dict, profile: dict, quarantine: DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame([metrics]).drop(columns=["issue_counts", "dataset_issues"], errors="ignore").to_excel(writer, sheet_name="Metrics", index=False)
        pd.DataFrame([{"issue": key, "affected_records": value} for key, value in metrics["issue_counts"].items()]).to_excel(writer, sheet_name="Issue Counts", index=False)
        pd.DataFrame(profile.get("columns", [])).to_excel(writer, sheet_name="Profile", index=False)
        invalid = quarantine.toPandas()
        for column in invalid.columns:
            if invalid[column].map(lambda value: isinstance(value, (list, dict, tuple))).any():
                invalid[column] = invalid[column].map(
                    lambda value: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict, tuple)) else value
                )
        invalid.to_excel(writer, sheet_name="Quarantine", index=False)
    return output.getvalue()
