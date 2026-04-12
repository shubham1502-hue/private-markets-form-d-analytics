"""
03_data_quality_audit.py
Runs a structured data quality audit on the cleaned Form D dataset.
Produces field-level completeness scores, issue type breakdown,
and a record-level quality classification — mirroring the kind of
data QA workflows used by private markets data platforms.
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_FIELDS = [
    "issuer_name",
    "filing_date",
    "federal_exemption",
    "fund_type",
    "total_offering_amount",
    "amount_sold",
    "num_investors",
    "asset_class_group",
]

OPTIONAL_FIELDS = [
    "period_of_report",
    "cik",
    "form_type",
    "filing_year",
    "filing_quarter",
    "pct_capital_raised",
    "fund_size_bucket",
    "investor_bucket",
]


def completeness_report(df):
    """Calculate field-level completeness for required and optional fields."""
    records = []
    all_fields = REQUIRED_FIELDS + OPTIONAL_FIELDS

    for field in all_fields:
        if field not in df.columns:
            records.append({
                "field": field,
                "total": len(df),
                "populated": 0,
                "missing": len(df),
                "completeness_pct": 0.0,
                "field_type": "required" if field in REQUIRED_FIELDS else "optional",
                "status": "FIELD MISSING"
            })
            continue

        populated = df[field].notna() & (df[field].astype(str).str.strip() != "") & (df[field].astype(str) != "nan")
        n_populated = populated.sum()
        n_missing = len(df) - n_populated
        pct = round(n_populated / len(df) * 100, 2)

        status = "OK"
        if field in REQUIRED_FIELDS:
            if pct < 80:
                status = "CRITICAL"
            elif pct < 95:
                status = "WARNING"

        records.append({
            "field": field,
            "total": len(df),
            "populated": n_populated,
            "missing": n_missing,
            "completeness_pct": pct,
            "field_type": "required" if field in REQUIRED_FIELDS else "optional",
            "status": status
        })

    return pd.DataFrame(records)


def flag_quality_issues(df):
    """
    Apply rule-based quality flags at the record level.
    Each flag corresponds to a specific data quality issue type.
    """
    flags = pd.DataFrame(index=df.index)

    # Flag 1: Missing issuer name
    flags["missing_issuer_name"] = (
        df["issuer_name"].isna() |
        (df["issuer_name"].astype(str).str.strip() == "")
    )

    # Flag 2: Missing offering amount
    flags["missing_offering_amount"] = df["total_offering_amount"].isna()

    # Flag 3: Zero offering amount
    flags["zero_offering_amount"] = (
        df["total_offering_amount"].fillna(0) == 0
    )

    # Flag 4: Amount sold exceeds offering (data inconsistency)
    flags["amount_exceeds_offering"] = (
        df["amount_sold"].fillna(0) > df["total_offering_amount"].fillna(0)
    )

    # Flag 5: Zero investors
    flags["zero_investors"] = (
        df["num_investors"].fillna(0) == 0
    )

    # Flag 6: Duplicate filing (same fund, same quarter, same exemption)
    flags["duplicate_filing"] = df.get("is_duplicate", False)

    # Flag 7: Statistical outlier (top 1% offering)
    flags["outlier_offering_amount"] = df.get("is_outlier_offering", False)

    # Flag 8: Missing exemption type
    flags["missing_exemption_type"] = (
        df["federal_exemption"].isna() |
        (df["federal_exemption"].astype(str).str.strip() == "")
    )

    # Flag 9: Unclassified fund type
    flags["unclassified_fund_type"] = (
        df["fund_type"].astype(str).isin(["Other", "Unclassified", "nan", ""])
    )

    # Composite: any issue present
    flags["has_any_issue"] = flags.any(axis=1)

    # Quality tier
    issue_count = flags.drop(columns=["has_any_issue"]).sum(axis=1)
    flags["quality_tier"] = pd.cut(
        issue_count,
        bins=[-1, 0, 1, 2, 999],
        labels=["Clean", "Minor Issues", "Multiple Issues", "Critical Issues"]
    ).astype(str)

    return flags


def issue_summary(flags):
    """Summarise issue counts and rates across the dataset."""
    issue_cols = [c for c in flags.columns
                  if c not in ["has_any_issue", "quality_tier"]]
    records = []
    for col in issue_cols:
        count = flags[col].sum()
        rate = round(count / len(flags) * 100, 2)
        records.append({
            "issue_type": col,
            "records_affected": int(count),
            "rate_pct": rate,
            "severity": "HIGH" if rate > 10 else "MEDIUM" if rate > 3 else "LOW"
        })
    return pd.DataFrame(records).sort_values("records_affected", ascending=False)


def quality_scorecard(df, flags, completeness_df):
    """Generate a single-number overall data quality score."""
    required_completeness = completeness_df[
        completeness_df["field_type"] == "required"
    ]["completeness_pct"].mean()

    clean_rate = round(
        (flags["quality_tier"] == "Clean").sum() / len(flags) * 100, 2
    )

    no_duplicate_rate = round(
        (1 - flags["duplicate_filing"].mean()) * 100, 2
    )

    overall_score = round(
        (required_completeness * 0.5) +
        (clean_rate * 0.35) +
        (no_duplicate_rate * 0.15),
        2
    )

    return {
        "total_records": len(df),
        "required_field_completeness_pct": round(required_completeness, 2),
        "clean_record_rate_pct": clean_rate,
        "duplicate_free_rate_pct": no_duplicate_rate,
        "overall_quality_score": overall_score,
        "quality_grade": (
            "A" if overall_score >= 90 else
            "B" if overall_score >= 75 else
            "C" if overall_score >= 60 else "D"
        )
    }


def main():
    clean_path = PROCESSED_DIR / "form_d_clean.csv"
    if not clean_path.exists():
        print("Clean data not found. Run 02_clean_standardise.py first.")
        return

    df = pd.read_csv(clean_path)
    print(f"Running data quality audit on {len(df)} records...\n")

    # --- Completeness ---
    completeness_df = completeness_report(df)
    completeness_df.to_csv(OUTPUTS_DIR / "quality_completeness.csv", index=False)
    print("Field Completeness Report:")
    print(completeness_df[["field", "completeness_pct", "status"]].to_string(index=False))

    # --- Issue flags ---
    flags = flag_quality_issues(df)
    df_flagged = pd.concat([df, flags], axis=1)
    df_flagged.to_csv(PROCESSED_DIR / "form_d_flagged.csv", index=False)

    # --- Issue summary ---
    issue_df = issue_summary(flags)
    issue_df.to_csv(OUTPUTS_DIR / "quality_issues.csv", index=False)
    print("\n\nIssue Type Summary:")
    print(issue_df.to_string(index=False))

    # --- Quality tier distribution ---
    tier_dist = flags["quality_tier"].value_counts().reset_index()
    tier_dist.columns = ["quality_tier", "record_count"]
    tier_dist["pct"] = (tier_dist["record_count"] / len(flags) * 100).round(2)
    tier_dist.to_csv(OUTPUTS_DIR / "quality_tier_distribution.csv", index=False)
    print("\n\nQuality Tier Distribution:")
    print(tier_dist.to_string(index=False))

    # --- Scorecard ---
    scorecard = quality_scorecard(df, flags, completeness_df)
    with open(OUTPUTS_DIR / "quality_scorecard.json", "w") as f:
        json.dump(scorecard, f, indent=2)
    print("\n\nOverall Quality Scorecard:")
    for k, v in scorecard.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
