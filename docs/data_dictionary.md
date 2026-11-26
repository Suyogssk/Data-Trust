# Data Dictionary

## Demonstration customer input

| Field | Expected type | Example | Initial validation intent |
| --- | --- | --- | --- |
| `customer_id` | String | `C000123` | Required; unique key |
| `first_name` | String | `Asha` | Required |
| `last_name` | String | `Rao` | Required |
| `email` | String | `asha@example.com` | Required; email format |
| `phone` | String | `+919876543210` | Optional format check when populated |
| `age` | Integer-like | `34` | Required; parseable integer in configured range (initially 18–100) |
| `signup_date` | Date-like string | `2025-03-12` | Required; parseable using configured formats |
| `country` | String | `India` | Required |
| `customer_status` | String | `active` | Allowed values: `active`, `inactive`, `pending` |

Input column names are matched case-insensitively after trimming for rule binding. Original headers and values should be retained for audit. A missing required column is a schema issue; a present column with a blank cell is a row-level required/null issue. Extra columns are retained and reported as schema observations unless configuration makes them disallowed.

## Generated validation fields

| Field | Type | Meaning |
| --- | --- | --- |
| `_dq_row_id` | String/integer | Unique row identity assigned during this processing run |
| `_dq_issue_codes` | Array of strings | Failed rule IDs for the record |
| `_dq_issue_details` | Array of strings | Readable findings, with no source values embedded by default |
| `_dq_is_valid` | Boolean | True only when no enabled rule failed |

These names are reserved. Ingestion must reject or safely rename conflicting input headers rather than overwrite source data.
