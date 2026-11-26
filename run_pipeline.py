from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.ingestion.loader import load_upload
from src.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the customer data quality pipeline.")
    parser.add_argument("input", type=Path, help="Input CSV, XLSX, or XLS file")
    parser.add_argument("--rules", type=Path, default=Path("config/validation_rules.yaml"))
    parser.add_argument("--storage", type=Path, default=Path("data"))
    parser.add_argument("--max-records", type=int, default=10_000)
    args = parser.parse_args()

    loaded = load_upload(args.input.name, args.input.read_bytes())
    result = run_pipeline(loaded, args.rules, args.storage, max_records=args.max_records)
    print(json.dumps({"profile": result.profile, "metrics": result.metrics, "explanation": result.explanation, "stored_paths": result.stored_paths}, indent=2))


if __name__ == "__main__":
    main()
