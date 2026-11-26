from pyspark.sql import DataFrame


def classify(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    return frame.filter("_dq_is_valid = true"), frame.filter("_dq_is_valid = false")
