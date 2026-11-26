import unittest
from io import BytesIO

import pandas as pd

from src.ingestion.loader import IngestionError, load_upload


class IngestionTests(unittest.TestCase):
    def test_csv_headers_normalized_and_metadata_captured(self):
        loaded = load_upload("customers.CSV", b" Customer_ID ,Name\nC001,Asha\n")
        self.assertEqual(loaded.frame.columns.tolist(), ["customer_id", "name"])
        self.assertEqual(loaded.source_columns, (" Customer_ID ", "Name"))
        self.assertEqual(loaded.frame.iloc[0]["customer_id"], "C001")
        self.assertEqual(loaded.extension, ".csv")

    def test_empty_and_unsupported_uploads_are_rejected(self):
        with self.assertRaises(IngestionError):
            load_upload("empty.csv", b"")
        with self.assertRaises(IngestionError):
            load_upload("data.json", b"{}")

    def test_normalized_duplicate_headers_are_rejected(self):
        with self.assertRaises(IngestionError):
            load_upload("duplicates.csv", b"Email,email\na@b.com,a@b.com\n")

    def test_xlsx_upload_is_read(self):
        buffer = BytesIO()
        pd.DataFrame([{"Customer_ID": "C001", "age": 34}]).to_excel(buffer, index=False)
        loaded = load_upload("customers.xlsx", buffer.getvalue())
        self.assertEqual(loaded.frame.columns.tolist(), ["customer_id", "age"])
        self.assertEqual(loaded.frame.iloc[0]["age"], "34")


if __name__ == "__main__":
    unittest.main()
