"""
02_clean_standardise.py
Cleans and standardises raw Form D data.
Handles: name normalisation, null imputation, currency standardisation,
duplicate detection, outlier flagging, and industry classification.
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

FUND_TYPE_GROUPS = {
    "Venture Capital": "Private Equity & VC",
    "Private Equity": "Private Equity & VC",
    "Private Equity Buyout": "Private Equity & VC",
    "Private Equity Growth": "Private Equity & VC",
    "Fund of Funds": "Private Equity & VC",
    "Hedge Fund": "Hedge Funds",
    "Real Estate Fund": "Real Assets",
    "Infrastructure": "Real Assets",
    "Natural Resources": "Real Assets",
    "Private Debt": "Private Debt",
}

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC",
}


def normalise_name(name):
    """Standardise fund/issuer names for deduplication."""
    if not isinstance(name, str):
        return ""
    name = name.upper().strip()
    name = re.sub(r"\b(LLC|LP|LTD|INC|CORP|FUND|PARTNERS|CAPITAL|GROUP|CO)\b\.?", "", name)
    name = re.sub(r"[^A-Z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def standardise_offering(val):
    """Convert offering amount to float, handle edge cases."""
    try:
        val = float(val)
        if val < 0:
            return np.nan
        return val
    except (TypeError, ValueError):
        return np.nan


def classify_fund_size(amount):
    """Segment funds by offering size — mirrors Preqin fund size buckets."""
    if pd.isna(amount):
        return "Unknown"
    if amount < 1_000_000:
        return "Micro (<$1M)"
    elif amount < 10_000_000:
        return "Small ($1M-$10M)"
    elif amount < 50_000_000:
        return "Lower Mid ($10M-$50M)"
    elif amount < 250_000_000:
        return "Mid ($50M-$250M)"
    elif amount < 1_000_000_000:
        return "Large ($250M-$1B)"
    else:
        return "Mega (>$1B)"


def clean(df):
    print(f"Input records: {len(df)}")

    # --- Name normalisation ---
    df["issuer_name"] = df["issuer_name"].astype(str).str.strip()
    df["issuer_name_clean"] = df["issuer_name"].apply(normalise_name)

    # --- Offering amount standardisation ---
    df["total_offering_amount"] = df["total_offering_amount"].apply(standardise_offering)
    df["amount_sold"] = df["amount_sold"].apply(standardise_offering)

    # Logical consistency: amount sold cannot exceed total offering
    inconsistent = df["amount_sold"] > df["total_offering_amount"]
    df.loc[inconsistent, "amount_sold"] = df.loc[inconsistent, "total_offering_amount"]

    # Pct raised
    df["pct_capital_raised"] = (
        df["amount_sold"] / df["total_offering_amount"]
    ).clip(0, 1).round(4)

    # --- Date standardisation ---
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df = df[df["filing_date"].notna()]
    df["filing_year"] = df["filing_date"].dt.year
    df["filing_quarter"] = df["filing_date"].dt.to_period("Q").astype(str)
    df["filing_month"] = df["filing_date"].dt.to_period("M").astype(str)

    # Remove future dates
    df = df[df["filing_date"] <= pd.Timestamp.today()]

    # --- Investor count ---
    df["num_investors"] = pd.to_numeric(df["num_investors"], errors="coerce").fillna(0).astype(int)
    df["investor_bucket"] = pd.cut(
        df["num_investors"],
        bins=[0, 10, 50, 200, 500, 99999],
        labels=["Small (1-10)", "Mid (11-50)", "Large (51-200)", "Institutional (201-500)", "Mass (500+)"],
        right=True
    ).astype(str)

    # --- Industry grouping ---
    df["asset_class_group"] = df["fund_type"].map(FUND_TYPE_GROUPS).fillna("Other")

    # --- Fund size bucket ---
    df["fund_size_bucket"] = df["total_offering_amount"].apply(classify_fund_size)

    # --- Duplicate detection ---
    df["is_duplicate"] = df.duplicated(
        subset=["issuer_name_clean", "filing_quarter", "federal_exemption"],
        keep="first"
    )

    # --- Outlier flag (top 1% offering amounts) ---
    p99 = df["total_offering_amount"].quantile(0.99)
    df["is_outlier_offering"] = df["total_offering_amount"] > p99

    print(f"After cleaning: {len(df)} records")
    print(f"  Duplicates flagged: {df['is_duplicate'].sum()}")
    print(f"  Outliers flagged: {df['is_outlier_offering'].sum()}")

    return df


def main():
    raw_path = RAW_DIR / "form_d_raw.csv"
    if not raw_path.exists():
        print("Raw data not found. Run 01_acquire_data.py first.")
        return

    df = pd.read_csv(raw_path)
    df = clean(df)

    out_path = PROCESSED_DIR / "form_d_clean.csv"
    df.to_csv(out_path, index=False)
    print(f"\nClean data saved: {out_path}")
    print(df[["issuer_name", "filing_date", "fund_type",
              "asset_class_group", "fund_size_bucket",
              "total_offering_amount", "pct_capital_raised"]].head(10).to_string())


if __name__ == "__main__":
    main()
