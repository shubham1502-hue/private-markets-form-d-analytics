# Private Markets Fundraising Intelligence
### SEC Form D Analytics — Data Acquisition, Curation & Quality Assurance Pipeline

A structured data management and analytics project simulating how private markets data platforms acquire, standardise, quality-assure, and analyse alternative assets fundraising data.

Built on the SEC Form D regulatory framework — the mandatory filings that PE, VC, hedge funds, and other private funds submit when raising capital from investors.

---

## Business Problem

Alternative assets — private equity, venture capital, private debt, real estate funds, hedge funds, and infrastructure — do not trade on public exchanges. There is no centralised price feed or public registry. This makes accurate, standardised fund-level data extremely difficult to collect and maintain at scale.

Private markets data platforms solve this by systematically acquiring data from regulatory sources (like SEC Form D filings), standardising inconsistent fields, quality-assuring records, and making the cleaned data available to institutional investors who depend on it for investment decisions.

This project simulates that end-to-end data pipeline.

---

## What This Project Demonstrates

| Capability | Implementation |
|---|---|
| Data acquisition from regulatory filings | SEC Form D filing structure and field mapping |
| Name normalisation and standardisation | Regex-based fund name cleaning, deduplication |
| Field-level data quality audit | Completeness scoring across required and optional fields |
| Record-level issue flagging | 9 quality rules: missing values, duplicates, inconsistencies, outliers |
| Quality scoring framework | Weighted overall quality score with A–D grading |
| SQL-driven analytical queries | DuckDB: quarterly trends, asset class breakdown, Pareto analysis |
| Tableau-ready BI export | 6 analytical datasets + master dataset for dashboard |

---

## What is a Form D?

When a private fund raises capital under SEC Regulation D (the most common exemption from public registration), they file a **Form D** with the SEC. Key fields include:

- **Issuer name** — the fund or company raising capital
- **Federal exemption type** — the regulatory basis (506b, 506c, etc.)
- **Total offering amount** — the target fund size
- **Amount sold** — capital raised to date
- **Number of investors** — how many LPs have committed
- **Date of first sale** — when fundraising began
- **State of incorporation** — jurisdiction

This is the raw material that alternative assets data platforms like Preqin, PitchBook, and Bloomberg use to track private markets fundraising activity globally.

---

## Project Architecture

```
SEC Form D Filing Structure
          ↓
01_generate_data.py       ← Synthetic dataset (8,500 filings, 2022–2024)
          ↓                  mirroring real Form D distributions
02_clean_standardise.py   ← Name normalisation, field standardisation,
          ↓                  fund size buckets, asset class mapping
03_data_quality_audit.py  ← Field completeness scoring, 9 issue flags,
          ↓                  quality tier classification, scorecard
04_sql_analysis.py        ← DuckDB analytical queries, BI export
          ↓
outputs/                  ← Tableau-ready CSVs
```

---

## Data Quality Framework

The audit module applies a structured quality assessment mirroring how data management teams evaluate database integrity:

### Field-Level Completeness

| Field | Type | Completeness |
|---|---|---|
| issuer_name | Required | 100% |
| filing_date | Required | 100% |
| federal_exemption | Required | 98.0% |
| total_offering_amount | Required | 95.0% |
| fund_type | Required | 100% |
| asset_class_group | Required | 100% |
| num_investors | Required | 100% |

### Record-Level Issue Flags

| Issue Type | Records Affected | Severity |
|---|---|---|
| Duplicate filings | 492 (5.8%) | MEDIUM |
| Missing offering amount | 425 (5.0%) | MEDIUM |
| Amount sold exceeds offering | 425 (5.0%) | MEDIUM |
| Zero investors | 170 (2.0%) | LOW |
| Missing exemption type | 170 (2.0%) | LOW |

### Quality Scorecard

| Metric | Value |
|---|---|
| Required field completeness | 99.1% |
| Clean record rate | 85.8% |
| Duplicate-free rate | 94.2% |
| **Overall quality score** | **93.7 / 100 (Grade: A)** |

---

## Key Findings

*(From 8,500 synthetic Form D filings, 2022–2024)*

- **Peak fundraising quarter:** 2023Q4 — 898 filings, ~$364B in total offerings
- **Dominant asset class:** Private Equity & VC — largest share of total capital raised
- **Dominant exemption:** 506(b) accounts for ~60% of all filings — standard for institutional fundraises
- **Capital concentration:** Mega funds (>$1B) represent 8% of filings but 78% of total capital — a classic Pareto distribution
- **Investor concentration:** Mid-sized investor groups (11–50 LPs) drive the highest total capital — consistent with institutional co-investment club dynamics

---

## SQL Analytical Queries

Key queries written in DuckDB-compatible SQL:

```sql
-- Quarterly fundraising trends
SELECT filing_quarter,
       COUNT(*) AS total_filings,
       SUM(total_offering_amount) AS capital_raised,
       ROUND(SUM(amount_sold)/SUM(total_offering_amount)*100,2) AS close_rate_pct
FROM form_d WHERE quality_tier = 'Clean'
GROUP BY filing_quarter ORDER BY filing_quarter;

-- Asset class capital breakdown
SELECT asset_class_group, fund_type,
       COUNT(*) AS fund_count,
       SUM(total_offering_amount) AS total_capital,
       MEDIAN(total_offering_amount) AS median_fund_size
FROM form_d GROUP BY asset_class_group, fund_type
ORDER BY total_capital DESC;

-- Exemption type distribution
SELECT federal_exemption,
       COUNT(*) AS filing_count,
       ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),2) AS share_pct
FROM form_d GROUP BY federal_exemption;

-- Pareto: fund size driving capital concentration
SELECT fund_size_bucket,
       ROUND(SUM(total_offering_amount)*100.0/SUM(SUM(total_offering_amount)) OVER(),2) AS pct_of_capital,
       ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),2) AS pct_of_funds
FROM form_d GROUP BY fund_size_bucket ORDER BY pct_of_capital DESC;
```

---

## Tableau Dashboard Views

Load `outputs/tableau_master.csv` into Tableau. Recommended views:

1. **Fundraising Overview** — quarterly trend of filings and capital raised, YoY comparison
2. **Asset Class Breakdown** — capital by fund type and asset class group
3. **Data Quality Report** — completeness scores, issue type distribution, quality tier breakdown
4. **Fund Size Distribution** — Pareto: % of capital vs % of funds by size bucket
5. **Exemption Type Analysis** — 506(b) vs 506(c) trends over time
6. **Investor Concentration** — capital distribution by investor count bucket

---

## Setup & Usage

```bash
# Clone repository
git clone https://github.com/shubham1502-hue/private-markets-form-d-analytics.git
cd private-markets-form-d-analytics

# Install dependencies
pip install -r requirements.txt

# Run full pipeline
python src/01_generate_data.py
python src/02_clean_standardise.py
python src/03_data_quality_audit.py
python src/04_sql_analysis.py

# Or run all at once
python main.py
```

Outputs generated in `/outputs/`:
- `tableau_master.csv` — full dataset for Tableau
- `fundraising_by_quarter.csv`
- `capital_by_asset_class.csv`
- `exemption_breakdown.csv`
- `fund_size_distribution.csv`
- `investor_concentration.csv`
- `quality_completeness.csv`
- `quality_issues.csv`
- `quality_scorecard.json`

---

## Technologies

- Python (Pandas, NumPy, SciPy)
- SQL (DuckDB)
- Tableau (dashboard)
- SEC Form D regulatory framework

---

## Design Decisions

**Synthetic data over direct API pull:** SEC EDGAR's Form D XML API requires individual filing-level HTTP requests at scale, which is rate-limited and slow for a portfolio project. The synthetic dataset is generated using real Form D field distributions derived from SEC statistics and Preqin market reports — ensuring the data quality patterns, fund size distributions, and exemption type ratios mirror reality.

**Rule-based quality flags over ML anomaly detection:** Data management teams in financial platforms need explainable, auditable quality rules — not black-box anomaly scores. Each flag maps to a specific, documentable business rule that a compliance or data operations team can act on.

**DuckDB for SQL layer:** Lightweight, no-server SQL engine that runs analytical queries on DataFrames in-process — ideal for portfolio projects and local analytics pipelines without requiring a database server setup.

---

**Author:** Shubham Singh
Process Analyst | Business & Data Analytics
Focus: Financial data operations, private markets analytics, revenue operations
