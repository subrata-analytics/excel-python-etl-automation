# Excel–Python ETL Automation (Retail Sales)

This project implements a **production‑grade Excel → Python ETL pipeline** for retail sales data.
It is configuration‑driven, auditable, and designed for scale.

---

## High‑Level Flow

1. **Extract**
   - Reads Excel files using configuration in `config/pipeline.yaml`
   - Supports sheet selection and column mapping

2. **Profile (Optional)**
   - Captures before/after statistics
   - Feeds data quality scoring

3. **Normalize / Transform**
   - Text normalization
   - Numeric cleaning
   - Date parsing
   - Reference data normalization (region, category)
   - Schema enforcement
   - Feature engineering
   - Filtering and validation

4. **Load**
   - Writes cleaned data to `data/processed`
   - Writes curated outputs to `data/curated`

---

## Project Structure

```
excel-python-etl/
├── config/
│   ├── pipeline.yaml          # Main ETL configuration
│   └── reference_data.yaml    # Canonical region/category values
├── data/
│   ├── raw/                   # Input Excel files
│   ├── processed/             # Normalized outputs
│   └── curated/               # Analytics‑ready outputs
├── logs/
│   ├── pipeline.log           # Pipeline lifecycle + summaries
│   └── anomalies.log          # Data quality issues
├── src/
│   ├── anomalies.py
│   ├── curate.py
|   ├── extract.py
│   ├── load.py
│   ├── normalize.py
│   ├── profile.py
│   ├── transform/             # Modular transformation rules
│   └── utils/
|       ├── __init__.py
│       ├── logger.py
│       ├── lineage.py
│       └── helpers.py
└── pipeline.py
```

---

## Lineage & Auditability

This pipeline provides **row‑level, field‑level lineage**.

### Lineage Output
- File: `data/processed/normalization_lineage.csv`
- Captures:
  - `row_id`
  - column name
  - old value
  - new value
  - rule responsible
  - timestamp

---

## Empty Row Handling

Empty rows are **explicitly detected and dropped** early in the pipeline.

- Fully empty rows are logged
- Drop reason is deterministic
- Prevents silent data loss

---

## Logging Strategy

| Log | Purpose |
|---|---|
| `pipeline.log` | Pipeline start/stop, summaries |
| `anomalies.log` | Invalid values, rule violations |
| lineage CSV | High‑volume audit events |

Row‑level changes are **not** written to logs to avoid log overflow.

---

## Running the Pipeline

```bash
python pipeline.py
```

Ensure:
- `pipeline.yaml` paths are correct
- Excel input file exists in `data/raw`
- CSV output path exists in `data/`

---

## Design Principles

- Configuration > code
- Explicit over implicit behavior
- Deterministic transformations
- Auditor‑friendly outputs
- Scales from thousands → millions of rows

---

This ETL is designed for **real analytics work**, not notebooks.