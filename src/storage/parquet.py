from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame


def save_layers(source: DataFrame, valid: DataFrame, invalid: DataFrame, root: str | Path) -> dict[str, str]:
    base = Path(root)
    paths = {"bronze": base / "bronze", "silver": base / "silver", "quarantine": base / "quarantine"}
    for name, frame in (("bronze", source), ("silver", valid), ("quarantine", invalid)):
        if os.name == "nt" and not os.environ.get("HADOOP_HOME") and not os.environ.get("hadoop.home.dir"):
            # Hadoop's Windows output committer requires winutils.exe. Keep the
            # local 10k demo usable without that optional binary; Spark still
            # performs all transformations and Arrow writes the bounded output.
            paths[name].mkdir(parents=True, exist_ok=True)
            for old_part in paths[name].glob("part-*.parquet"):
                old_part.unlink()
            frame.toPandas().to_parquet(paths[name] / "part-00000.parquet", index=False)
        else:
            frame.write.mode("overwrite").parquet(str(paths[name]))
    return {name: str(path) for name, path in paths.items()}
