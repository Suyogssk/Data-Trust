# Interview Preparation

## 60-second explanation

I built a data quality monitoring application for customer records arriving as CSV or Excel. It profiles the input, runs configurable PySpark checks for required fields, duplicate customer IDs, email and phone formats, age range, dates, and allowed status values, then writes the original, valid, and quarantined datasets as separate Parquet layers. A Streamlit dashboard shows metrics and lets users download JSON, CSV, and Excel reports. Optional LLM explanations receive aggregate metrics only; the application has a deterministic fallback and never changes records automatically. I verified the local workflow against 10,000 generated records and added focused ingestion, profiling, validation, metrics, and report checks. The current upload boundary uses Pandas and the local demo limit is 10,000, so I am not claiming large-scale production performance.

## Three-minute explanation

The business problem is that customer data often arrives with missing values, duplicate identifiers, malformed contact details, invalid dates, and unexpected categories. Manual checks are inconsistent and make it hard to understand whether a dataset is safe for downstream use.

The application separates ingestion, profiling, validation, classification, storage, reporting, AI explanation, and presentation. Ingestion accepts bounded CSV and Excel uploads, validates file type and headers, normalizes headers for rule binding, and reads source values as strings so malformed values are preserved. The data enters PySpark with an explicit all-string schema. Profiling calculates row/column counts, missing values, distinct counts, exact duplicate count, and parseable numeric statistics.

Validation rules live in YAML and are evaluated independently with Spark expressions. Duplicate keys use a window count; field checks use regular expressions, `try_cast`, date parsing, and allowed-value membership. Every row retains a list of failed rule identifiers and readable issue details. A row with no failed rules goes to Silver; a row with any failures goes to Quarantine. Bronze keeps the unmodified input. The overall quality percentage is valid rows divided by total rows, multiplied by 100.

The Streamlit app exposes upload, overview metrics, profiling, applied rules, quarantine inspection, AI explanations, and downloads. The AI path sends only aggregates, not records or cell values. If configuration is absent or the API fails, a rule-based summary remains available. Reports include issue counts and quarantined rows. On Windows without Hadoop `winutils.exe`, the bounded local demo uses PyArrow to write Parquet; on a normal Hadoop-capable environment the storage module uses Spark's Parquet writer.

I tested the workflow on 10,000 synthetic rows and added eight automated checks. This is a functional validation point, not a scale benchmark. Larger ingestion, durable cloud storage, authentication, and managed production monitoring remain future engineering work.

## Architecture and data flow

1. CSV/XLS/XLSX upload is checked for type, byte size, parse errors, empty data, and duplicate normalized headers.
2. Source values are loaded without coercing malformed values and passed into a Spark DataFrame with string columns.
3. Profiling creates aggregate column metrics without sending source values to AI.
4. Enabled YAML rules append `_dq_issue_codes`, `_dq_issue_details`, `_dq_is_valid`, and a run-local `_dq_row_id`.
5. Bronze stores original source fields; Silver stores records with no failed rules; Quarantine stores all failed records and flags.
6. Metrics drive dashboard charts and JSON/CSV/Excel exports. AI explanation is optional and aggregate-only.

## PySpark implementation points

- Most validation is expressed as Spark SQL column expressions, so transformations remain distributed.
- Duplicate key checks use a partition window and flag every member of a conflicting key group.
- Invalid casts use `try_cast` so malformed values become validation failures instead of aborting the job.
- DataFrame counts and aggregate metrics are computed with Spark actions. The UI only brings at most 500 quarantine rows into its inspection table.
- The file decoder currently uses Pandas at the input boundary and imposes a 25 MB and 10,000-row public demo limit. It should be replaced with distributed ingestion before claiming very large input support.

## AI integration

The explainer accepts a metrics dictionary containing counts, percentages, issue IDs, and schema-level issue descriptions. It does not receive full records, personal data samples, or direct identifiers. The OpenAI-compatible endpoint is opt-in through environment variables. Network errors, invalid responses, or absent credentials fall back to deterministic local text. The AI path has no write access to the data pipeline.

## Challenges and decisions

- Preserve malformed values: input columns are read as strings and validation performs safe parsing without overwriting source cells.
- Windows Spark worker selection: the local Spark builder points Python workers to the active interpreter rather than relying on a `python3` executable on PATH.
- Windows Parquet permissions: without `winutils.exe`, Spark's local output committer fails. The bounded Windows demo writes each Spark result with PyArrow; Spark remains responsible for validation and classification.
- Avoid unsupported scale claims: the workflow was functionally run at 10,000 records, but no formal performance benchmark or distributed cluster run was performed.

## Performance and testing

Profiling combines column aggregates into one Spark aggregation and counts exact duplicates separately. Spark DataFrames are cached across the pipeline actions. The app caps uploads at 25 MB and default runs at 10,000 rows; report downloads intentionally materialize quarantine output for export, so they remain demo-sized. Automated checks cover supported/rejected ingestion cases, duplicate and field validation, quality metrics, schema mismatch, and report serialization.

## Twenty technical interview questions and answers

1. **Why use Spark for 10,000 rows?** The demo uses a small, verifiable scale, but Spark lets the core rules and classification use the same distributed DataFrame model intended for future larger inputs. The current Pandas file-decoding boundary remains a scaling limit.
2. **Why read all source values as strings?** It prevents bad source values from being silently coerced or dropped before the validation engine can report them.
3. **How are records classified?** A row is valid when its issue-code array is empty. Any failed enabled rule sends it to Quarantine.
4. **How do you detect duplicate primary keys?** A window count partitioned by `customer_id` identifies every row in a duplicate group. Every member is marked invalid.
5. **Why mark the first duplicate too?** There is no safe winner without a business precedence rule. Flagging all conflicting rows avoids silently selecting one.
6. **What happens when a required column is absent?** The run records a dataset-level schema issue and marks each row with a schema mismatch so no row is incorrectly classified as valid.
7. **How do you handle invalid numbers?** `try_cast` returns null for non-parseable values, which becomes a range/type validation failure rather than aborting the Spark job.
8. **How are dates validated?** The rule tries each configured Spark datetime format and fails the row only if no format parses.
9. **How is the quality score calculated?** `100 × valid_records / total_records`; zero-row files are rejected during ingestion.
10. **What does the missing-value metric count?** Blank or null cells across source columns. Required-field issue counts are also reported separately by rule.
11. **What is in Bronze, Silver, and Quarantine?** Bronze contains source values. Silver contains valid rows. Quarantine contains invalid rows plus all applicable issue codes and descriptions.
12. **Does validation mutate source records?** No. It appends generated technical fields and writes separate outputs; Bronze preserves the input values.
13. **What does the LLM receive?** Aggregate counts, percentages, issue identifiers, and schema-level messages only. It never receives entire rows or cell values.
14. **What if the AI endpoint is down?** The system returns a deterministic explanation from the metrics; data processing and reports still work.
15. **Can AI remediation update customer data?** No. Its output is text advice only and has no pipeline write access.
16. **How do you support Excel?** Pandas reads the first worksheet at the bounded upload boundary; the result is normalized and passed to Spark. `.xlsx` uses openpyxl and `.xls` uses xlrd.
17. **How do you avoid loading all invalid rows in the dashboard?** The inspection table limits the Spark DataFrame to 500 rows. Full quarantine materialization occurs only when a user downloads a report, within the demo limit.
18. **What is the main scalability limitation?** CSV/Excel decoding currently passes through Pandas and the public workflow caps files at 25 MB/10,000 rows. A large-scale version should use distributed file ingestion and cloud object storage.
19. **What was the Windows storage challenge?** Hadoop's local filesystem expects `winutils.exe` for permission operations. Without it, the local demo converts bounded result sets to Pandas and writes Parquet through PyArrow; Linux/Hadoop environments use Spark's Parquet writer.
20. **What would you improve next?** Add durable object storage, authentication and authorization, PostgreSQL metadata, structured run history, stronger schema evolution, managed Spark deployment, and measured load/performance tests before production claims.
