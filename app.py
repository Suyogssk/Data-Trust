from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
from pyspark.sql import functions as F

from src.ingestion.loader import IngestionError, load_upload
from src.pipeline import PipelineResult, get_spark, run_pipeline
from src.reporting.exports import csv_report, excel_report, json_report


ROOT = Path(__file__).parent
RULES = ROOT / "config" / "validation_rules.yaml"
DATA_ROOT = ROOT / "data"

st.set_page_config(page_title="Data Quality Monitor", page_icon="◈", layout="wide")
st.markdown(
    """
    <style>
    .stApp { background: #f4f7fb; }
    [data-testid="stMetric"] { background: white; border: 1px solid #e5eaf1; padding: 16px; border-radius: 12px; }
    .block-container { padding-top: 1.5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("◈ Data Quality Monitor")
st.caption("Profile, validate, and understand customer data before it reaches downstream systems.")

result: PipelineResult | None = st.session_state.get("pipeline_result")
page = st.sidebar.radio("Workspace", ["Overview", "Upload & run", "Profiling", "Validation", "Quarantine", "AI explanation", "Downloads"])


def require_result() -> PipelineResult | None:
    if result is None:
        st.info("Upload a CSV or Excel file and run the checks to populate this view.")
    return result


if page == "Upload & run":
    st.subheader("Analyze a dataset")
    st.write("CSV and Excel uploads are accepted. The public demo is capped at 10,000 records per run.")
    upload = st.file_uploader("Choose a customer dataset", type=["csv", "xlsx", "xls"])
    if upload is not None:
        st.caption(f"{upload.name} · {upload.size:,} bytes")
    if st.button("Run quality checks", type="primary", disabled=upload is None):
        try:
            with st.status("Processing dataset with PySpark…", expanded=True) as status:
                loaded = load_upload(upload.name, upload.getvalue())
                st.write(f"Read {len(loaded.frame):,} records and {len(loaded.frame.columns)} columns.")
                new_result = run_pipeline(loaded, RULES, DATA_ROOT, max_records=10_000, spark=get_spark())
                st.session_state["pipeline_result"] = new_result
                st.session_state["source_filename"] = loaded.filename
                status.update(label="Quality checks complete", state="complete", expanded=False)
            st.success("Processing finished. Open Overview to review the results.")
        except (IngestionError, ValueError) as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Processing failed: {exc}")

elif page == "Overview":
    current = require_result()
    if current:
        metrics = current.metrics
        cols = st.columns(4)
        cols[0].metric("Total records", f"{metrics['total_records']:,}")
        cols[1].metric("Valid", f"{metrics['valid_records']:,}")
        cols[2].metric("Invalid", f"{metrics['invalid_records']:,}")
        quality = metrics["quality_percentage"]
        cols[3].metric("Quality rate", f"{quality:.1f}%" if quality is not None else "N/A")
        left, right = st.columns([1, 1])
        with left:
            st.subheader("Records by status")
            st.bar_chart({"Records": {"Valid": metrics["valid_records"], "Invalid": metrics["invalid_records"]}})
        with right:
            st.subheader("Most frequent findings")
            if metrics["issue_counts"]:
                st.bar_chart({"Affected rows": metrics["issue_counts"]})
            else:
                st.success("No row-level issues found.")
        st.caption(f"Source: {st.session_state.get('source_filename', 'uploaded dataset')} · explanation mode: {current.explanation_source}")

elif page == "Profiling":
    current = require_result()
    if current:
        st.subheader("Dataset profile")
        st.json({key: value for key, value in current.profile.items() if key != "columns"})
        st.dataframe(current.profile["columns"], use_container_width=True, hide_index=True)

elif page == "Validation":
    current = require_result()
    if current:
        st.subheader("Validation rules applied")
        st.write(f"{len(current.rules_applied)} enabled rules ran.")
        st.dataframe([{"Rule ID": rule, "Status": "Applied"} for rule in current.rules_applied], use_container_width=True, hide_index=True)
        if current.metrics["dataset_issues"]:
            st.error("Dataset-level schema issues: " + " · ".join(current.metrics["dataset_issues"]))
        st.caption("Rules are defined in config/validation_rules.yaml. Each row can carry multiple issue codes.")

elif page == "Quarantine":
    current = require_result()
    if current:
        st.subheader(f"Quarantined records ({current.metrics['invalid_records']:,})")
        issue_options = sorted(current.metrics["issue_counts"])
        selected = st.multiselect("Filter by issue", issue_options)
        search = st.text_input("Search in displayed records", placeholder="Search visible values")
        frame = current.invalid
        if selected:
            frame = frame.filter(F.size(F.array_intersect(F.col("_dq_issue_codes"), F.array(*[F.lit(i) for i in selected]))) > 0)
        view = frame.limit(500).toPandas()
        if search:
            view = view[view.astype(str).apply(lambda column: column.str.contains(search, case=False, regex=False)).any(axis=1)]
        st.caption("Showing at most 500 records in this table. Use downloads for the full quarantine export.")
        st.dataframe(view, use_container_width=True, hide_index=True)

elif page == "AI explanation":
    current = require_result()
    if current:
        st.subheader("Dataset quality explanation")
        st.info(current.explanation)
        st.caption(f"Source: {current.explanation_source}. Only aggregate metrics are sent to the optional LLM. AI recommendations are advisory and never modify records.")

elif page == "Downloads":
    current = require_result()
    if current:
        st.subheader("Download reports")
        st.write("Exports include aggregate quality metrics and the full quarantined record set.")
        st.download_button("Download JSON report", json_report(current.metrics, current.profile, current.invalid), "quality_report.json", "application/json")
        st.download_button("Download quarantine CSV", csv_report(current.invalid), "quarantine.csv", "text/csv")
        st.download_button("Download Excel workbook", excel_report(current.metrics, current.profile, current.invalid), "quality_report.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.caption("Exports are created on request. Excel/CSV quarantine exports materialize the quarantined data and are intended for the 10,000-record demo limit.")

with st.sidebar:
    st.divider()
    st.caption("Local demo · PySpark · Parquet")
    st.caption("Files are stored in local ephemeral workspace folders.")
