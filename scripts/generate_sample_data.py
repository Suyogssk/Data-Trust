"""Generate a reproducible, intentionally imperfect customer CSV dataset."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path


FIELDS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "age",
    "signup_date",
    "country",
    "customer_status",
]

FIRST_NAMES = ["Asha", "Ravi", "Mina", "Arjun", "Leela", "Kabir", "Nina", "Dev", "Sara", "Ishan"]
LAST_NAMES = ["Rao", "Shah", "Patel", "Khan", "Mehta", "Iyer", "Das", "Nair", "Singh", "Joshi"]
COUNTRIES = ["India", "United States", "United Kingdom", "Singapore", "Australia"]
STATUSES = ["active", "inactive", "pending"]


def make_valid_row(row_number: int, rng: random.Random) -> dict[str, str]:
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    customer_id = f"C{row_number:06d}"
    signup = date(2020, 1, 1) + timedelta(days=rng.randint(0, 2200))
    return {
        "customer_id": customer_id,
        "first_name": first,
        "last_name": last,
        "email": f"{first.lower()}.{last.lower()}.{row_number}@example.com",
        "phone": f"+91{rng.randint(6000000000, 9999999999)}",
        "age": str(rng.randint(18, 80)),
        "signup_date": signup.isoformat(),
        "country": rng.choice(COUNTRIES),
        "customer_status": rng.choice(STATUSES),
    }


def introduce_issues(rows: list[dict[str, str]], rng: random.Random) -> None:
    """Mutate selected fields to represent common source-system defects."""
    count = len(rows)
    def selected(rate: float) -> list[int]:
        sample_size = min(count, round(count * rate))
        return rng.sample(range(count), k=sample_size) if sample_size else []

    for index in selected(0.025):
        rows[index]["email"] = ""
    for index in selected(0.035):
        rows[index]["phone"] = ""
    for index in selected(0.025):
        rows[index]["age"] = ""
    for index in selected(0.018):
        rows[index]["signup_date"] = ""
    for index in selected(0.012):
        rows[index]["country"] = ""

    for index in selected(0.025):
        rows[index]["email"] = rng.choice(
            ["not-an-email", "missing-domain@", "name.example.com", "@example.com"]
        )
    for index in selected(0.018):
        rows[index]["phone"] = rng.choice(["123", "phone-unknown", "+", "555-12"])
    for index in selected(0.012):
        rows[index]["age"] = rng.choice(["twelve", "-4", "145", "21.5"])
    for index in selected(0.012):
        rows[index]["signup_date"] = rng.choice(["2025-99-45", "unknown", "31/31/2024"])
    for index in selected(0.01):
        rows[index]["customer_status"] = rng.choice(["ACTIVE", "archived", "unknown"])
    for index in selected(0.008):
        rows[index]["customer_id"] = ""

    # A few values use alternate but parseable formats to exercise format handling.
    for index in selected(0.025):
        try:
            raw_date = date.fromisoformat(rows[index]["signup_date"])
        except ValueError:
            continue
        rows[index]["signup_date"] = raw_date.strftime("%m/%d/%Y")

    # Duplicate complete records, including their customer IDs, after defects are added.
    duplicate_count = min(round(count * 0.012), count // 2) if count > 1 else 0
    sources = rng.sample(range(count - duplicate_count), k=duplicate_count) if duplicate_count else []
    for offset, source_index in enumerate(sources):
        target_index = count - duplicate_count + offset
        rows[target_index] = rows[source_index].copy()


def write_dataset(output: Path, row_count: int, seed: int) -> None:
    if row_count < 1:
        raise ValueError("row_count must be at least 1")

    rng = random.Random(seed)
    rows = [make_valid_row(i + 1, rng) for i in range(row_count)]
    introduce_issues(rows, rng)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDS, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000, help="number of records (default: 10000)")
    parser.add_argument("--seed", type=int, default=42, help="random seed for reproducible output")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/customers_dirty.csv"),
        help="CSV output path (default: data/raw/customers_dirty.csv)",
    )
    args = parser.parse_args()

    write_dataset(args.output, args.rows, args.seed)
    print(f"Wrote {args.rows:,} synthetic customer records to {args.output}")
    print(f"Seed: {args.seed}")


if __name__ == "__main__":
    main()
