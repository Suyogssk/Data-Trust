from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, Window, functions as F


@dataclass(frozen=True)
class ValidationResult:
    frame: DataFrame
    dataset_issues: tuple[str, ...]
    rules_applied: tuple[str, ...]


def load_rules(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}
    return config


def _column(name: str):
    return F.col(f"`{name.replace('`', '``')}`")


def _failure(rule: dict[str, Any], frame_columns: set[str]):
    kind, name = rule["type"], rule.get("column", "")
    if name not in frame_columns:
        return None
    value = _column(name)
    blank = value.isNull() | (F.trim(value) == "")
    if kind == "required":
        return blank
    if kind == "unique":
        window = Window.partitionBy(value)
        return (F.count(F.lit(1)).over(window) > 1) & (~blank)
    if kind == "email":
        return (~blank) & (~value.rlike(r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$"))
    if kind == "phone":
        digits = F.regexp_replace(value, r"\D", "")
        valid = (F.length(digits) >= int(rule.get("min_digits", 7))) & (F.length(digits) <= int(rule.get("max_digits", 15)))
        return (~blank) & (~valid)
    if kind == "range":
        number = F.expr(f"try_cast(`{name.replace('`', '``')}` as {rule.get('data_type', 'double')})")
        return blank | number.isNull() | (number < F.lit(rule["min"])) | (number > F.lit(rule["max"]))
    if kind == "date":
        formats = rule.get("formats", ["yyyy-MM-dd"])
        parsed = None
        for pattern in formats:
            candidate = F.try_to_timestamp(value, F.lit(pattern))
            parsed = candidate if parsed is None else F.coalesce(parsed, candidate)
        return blank | parsed.isNull()
    if kind == "allowed_values":
        choices = [str(item).casefold() for item in rule.get("values", [])]
        return (~blank) & (~F.lower(F.trim(value)).isin(choices))
    if kind == "type":
        target = rule.get("data_type", "string")
        cast = F.expr(f"try_cast(`{name.replace('`', '``')}` as {target})")
        return (~blank) & cast.isNull()
    raise ValueError(f"Unsupported validation rule type: {kind}")


def validate_dataframe(frame: DataFrame, config: dict[str, Any]) -> ValidationResult:
    reserved = {"_dq_row_id", "_dq_issue_codes", "_dq_issue_details", "_dq_is_valid"}
    collisions = reserved.intersection(frame.columns)
    if collisions:
        raise ValueError(f"Input uses reserved validation columns: {', '.join(sorted(collisions))}")

    required = config.get("schema", {}).get("required_columns", [])
    available = set(frame.columns)
    missing_columns = sorted(set(required) - available)
    extra = sorted(available - set(required))
    issues = [f"Missing required columns: {', '.join(missing_columns)}"] if missing_columns else []
    if extra and not config.get("dataset", {}).get("allow_extra_columns", True):
        issues.append(f"Unexpected columns: {', '.join(extra)}")

    result = frame.withColumn("_dq_row_id", F.monotonically_increasing_id().cast("string"))
    code_arrays = []
    detail_arrays = []
    applied = []
    if issues:
        schema_code = "schema_mismatch"
        code_arrays.append(F.array(F.lit(schema_code)))
        detail_arrays.append(F.array(F.lit("; ".join(issues))))
        applied.append(schema_code)
    for rule in config.get("rules", []):
        if not rule.get("enabled", True):
            continue
        rule_id = str(rule["id"])
        fail = _failure(rule, available)
        if fail is None:
            continue
        applied.append(rule_id)
        code_arrays.append(F.when(fail, F.array(F.lit(rule_id))).otherwise(F.array().cast("array<string>")))
        description = f"{rule_id}: validation failed for {rule.get('column', 'dataset')}"
        detail_arrays.append(F.when(fail, F.array(F.lit(description))).otherwise(F.array().cast("array<string>")))

    if code_arrays:
        result = result.withColumn("_dq_issue_codes", F.concat(*code_arrays))
        result = result.withColumn("_dq_issue_details", F.concat(*detail_arrays))
    else:
        result = result.withColumn("_dq_issue_codes", F.array().cast("array<string>"))
        result = result.withColumn("_dq_issue_details", F.array().cast("array<string>"))
    result = result.withColumn("_dq_is_valid", F.size("_dq_issue_codes") == 0)
    return ValidationResult(result, tuple(issues), tuple(applied))
