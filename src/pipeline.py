from __future__ import annotations

from dataclasses import dataclass
import os
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.ai.explainer import explain
from src.ingestion.loader import LoadedDataset
from src.profiling.profiler import profile_dataframe
from src.reporting.metrics import quality_metrics
from src.storage.parquet import save_layers
from src.transformation.classify import classify
from src.validation.engine import load_rules, validate_dataframe


@dataclass
class PipelineResult:
    profile: dict[str, Any]
    metrics: dict[str, Any]
    explanation: str
    explanation_source: str
    rules_applied: tuple[str, ...]
    validated: DataFrame
    valid: DataFrame
    invalid: DataFrame
    stored_paths: dict[str, str]

    def release(self) -> None:
        self.validated.unpersist(blocking=False)
        self.valid.unpersist(blocking=False)
        self.invalid.unpersist(blocking=False)


def get_spark(app_name: str = "ai-data-quality-monitor") -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )


def run_pipeline(
    loaded: LoadedDataset,
    rules_path: str | Path,
    storage_root: str | Path,
    max_records: int = 10_000,
    spark: SparkSession | None = None,
) -> PipelineResult:
    if len(loaded.frame) > max_records:
        raise ValueError(f"Dataset has {len(loaded.frame):,} rows; the configured limit is {max_records:,}.")
    session = spark or get_spark()
    schema = StructType([StructField(str(column), StringType(), True) for column in loaded.frame.columns])
    source = session.createDataFrame(
        (tuple(row) for row in loaded.frame.itertuples(index=False, name=None)), schema=schema
    )
    source = source.cache()
    profile = profile_dataframe(source)
    validation = validate_dataframe(source, load_rules(rules_path))
    validation.frame.cache()
    valid, invalid = classify(validation.frame)
    metrics = quality_metrics(validation.frame, validation.dataset_issues)
    explanation, source_name = explain(metrics)
    stored = save_layers(source, valid, invalid, storage_root)
    source.unpersist(blocking=False)
    return PipelineResult(
        profile, metrics, explanation, source_name, validation.rules_applied,
        validation.frame, valid, invalid, stored,
    )
