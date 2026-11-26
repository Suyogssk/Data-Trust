from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, functions as F


def quality_metrics(frame: DataFrame, dataset_issues: tuple[str, ...] = ()) -> dict[str, Any]:
    total = frame.count()
    counts = frame.agg(
        F.sum(F.when(F.col("_dq_is_valid"), 1).otherwise(0)).alias("valid"),
        F.sum(F.when(~F.col("_dq_is_valid"), 1).otherwise(0)).alias("invalid"),
    ).first()
    valid = int(counts["valid"] or 0)
    invalid = int(counts["invalid"] or 0)
    aggregate = frame.select(F.explode_outer("_dq_issue_codes").alias("issue")).groupBy("issue").count().collect()
    issue_counts = {row["issue"]: int(row["count"]) for row in aggregate if row["issue"] is not None}
    duplicate_codes = [key for key in issue_counts if "unique" in key or "duplicate" in key]
    source_columns = [name for name in frame.columns if not name.startswith("_dq_")]
    missing_exprs = [
        F.sum(
            F.when(
                F.col(f"`{name.replace('`', '``')}`").isNull()
                | (F.trim(F.col(f"`{name.replace('`', '``')}`")) == ""),
                1,
            ).otherwise(0)
        ).alias(f"missing_{index}")
        for index, name in enumerate(source_columns)
    ]
    missing_row = frame.agg(*missing_exprs).first() if missing_exprs else None
    missing_values = sum(int(value or 0) for value in missing_row) if missing_row else 0
    return {
        "total_records": total,
        "valid_records": valid,
        "invalid_records": invalid,
        "duplicate_records": sum(issue_counts[key] for key in duplicate_codes),
        "missing_values": missing_values,
        "invalid_email_count": sum(value for key, value in issue_counts.items() if "valid_email" in key),
        "invalid_phone_count": sum(value for key, value in issue_counts.items() if "valid_phone" in key),
        "invalid_date_count": sum(value for key, value in issue_counts.items() if "valid_signup_date" in key),
        "quality_percentage": round(100.0 * valid / total, 2) if total else None,
        "issue_counts": issue_counts,
        "dataset_issues": list(dataset_issues),
    }
