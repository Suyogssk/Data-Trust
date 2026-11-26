from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, functions as F


def profile_dataframe(frame: DataFrame) -> dict[str, Any]:
    columns = [column for column in frame.columns if not column.startswith("_dq_")]
    expressions = [F.count(F.lit(1)).alias("total_records")]
    for index, column in enumerate(columns):
        quoted = F.col(f"`{column.replace('`', '``')}`")
        nonblank = quoted.isNotNull() & (F.trim(quoted) != "")
        numeric_value = F.expr(f"try_cast(`{column.replace('`', '``')}` as double)")
        date_value = None
        for pattern in ("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy"):
            parsed = F.try_to_timestamp(quoted, F.lit(pattern))
            date_value = parsed if date_value is None else F.coalesce(date_value, parsed)
        expressions.extend(
            [
                F.sum(F.when(quoted.isNull() | (F.trim(quoted) == ""), 1).otherwise(0)).alias(f"missing_{index}"),
                F.countDistinct(quoted).alias(f"distinct_{index}"),
                F.sum(F.when(nonblank, 1).otherwise(0)).alias(f"nonblank_{index}"),
                F.sum(F.when(nonblank & numeric_value.isNotNull(), 1).otherwise(0)).alias(f"numeric_count_{index}"),
                F.sum(F.when(nonblank & date_value.isNotNull(), 1).otherwise(0)).alias(f"date_count_{index}"),
                F.min(numeric_value).alias(f"min_{index}"),
                F.max(numeric_value).alias(f"max_{index}"),
                F.avg(numeric_value).alias(f"mean_{index}"),
            ]
        )
    summary = frame.agg(*expressions).first()
    total = int(summary["total_records"] or 0)
    stats: list[dict[str, Any]] = []
    for index, column in enumerate(columns):
        item: dict[str, Any] = {
            "column": column,
            "data_type": "string",
            "missing_count": int(summary[f"missing_{index}"] or 0),
            "distinct_count": int(summary[f"distinct_{index}"] or 0),
        }
        nonblank_count = int(summary[f"nonblank_{index}"] or 0)
        numeric_count = int(summary[f"numeric_count_{index}"] or 0)
        date_count = int(summary[f"date_count_{index}"] or 0)
        semantic_text = column.casefold()
        identifier_semantics = any(token in semantic_text for token in ("phone", "mobile", "telephone", "postal", "zip", "_id", "identifier"))
        item["inferred_type"] = (
            "unknown" if not nonblank_count else
            "string" if identifier_semantics else
            "numeric" if numeric_count == nonblank_count else
            "date-like" if date_count == nonblank_count else
            "mixed / partially parseable" if numeric_count or date_count else
            "string"
        )
        numeric_semantics = not identifier_semantics
        if summary[f"mean_{index}"] is not None and numeric_semantics:
            item["numeric"] = {
                key: float(summary[f"{key}_{index}"])
                for key in ("min", "max", "mean")
                if summary[f"{key}_{index}"] is not None
            }
        stats.append(item)
    exact_duplicates = total - frame.dropDuplicates(columns).count() if total else 0
    return {
        "total_records": total,
        "total_columns": len(columns),
        "columns": stats,
        "exact_duplicate_records": int(exact_duplicates),
    }
