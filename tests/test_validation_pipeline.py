import tempfile
import unittest
from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType

from src.pipeline import get_spark
from src.reporting.exports import csv_report, excel_report, json_report
from src.reporting.metrics import quality_metrics
from src.transformation.classify import classify
from src.validation.engine import load_rules, validate_dataframe


ROOT = Path(__file__).resolve().parents[1]


class ValidationPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = get_spark("quality-monitor-unit-tests")
        cls.config = load_rules(ROOT / "config" / "validation_rules.yaml")
        cls.schema = StructType([StructField(name, StringType(), True) for name in (
            "customer_id", "first_name", "last_name", "email", "phone", "age",
            "signup_date", "country", "customer_status",
        )])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def make_frame(self):
        return self.spark.createDataFrame([
            ("C1", "Asha", "Rao", "asha@example.com", "+919876543210", "34", "2025-03-12", "India", "active"),
            ("C2", "Mina", "Shah", "bad-email", "+919876543211", "12", "2025-03-13", "India", "active"),
            ("C2", "Ravi", "Das", "ravi@example.com", "123", "45", "2025-03-14", "", "pending"),
        ], self.schema)

    def test_rules_classify_rows_and_metrics(self):
        result = validate_dataframe(self.make_frame(), self.config)
        valid, invalid = classify(result.frame)
        metrics = quality_metrics(result.frame, result.dataset_issues)
        self.assertEqual(valid.count(), 1)
        self.assertEqual(invalid.count(), 2)
        self.assertEqual(metrics["total_records"], 3)
        self.assertEqual(metrics["valid_records"] + metrics["invalid_records"], 3)
        self.assertEqual(metrics["duplicate_records"], 2)
        self.assertEqual(metrics["missing_values"], 1)
        self.assertEqual(metrics["invalid_email_count"], 1)
        self.assertAlmostEqual(metrics["quality_percentage"], 33.33)

    def test_schema_mismatch_marks_rows_invalid(self):
        partial = self.spark.createDataFrame([("C1",)], ["customer_id"])
        result = validate_dataframe(partial, self.config)
        self.assertTrue(result.dataset_issues)
        self.assertEqual(result.frame.filter("_dq_is_valid = false").count(), 1)
        self.assertIn("schema_mismatch", result.frame.first()["_dq_issue_codes"])

    def test_exports_include_quarantined_records(self):
        result = validate_dataframe(self.make_frame(), self.config)
        _, invalid = classify(result.frame)
        metrics = quality_metrics(result.frame)
        report_json = json_report(metrics, {"total_records": 3}, invalid)
        self.assertIn(b"quarantined_records", report_json)
        self.assertTrue(csv_report(invalid).startswith(b"\xef\xbb\xbf"))
        workbook = excel_report(metrics, {"columns": []}, invalid)
        self.assertTrue(workbook.startswith(b"PK"))


if __name__ == "__main__":
    unittest.main()
