"""
04_sql_analysis.py
Runs MySQL analytical queries and exports Tableau-ready CSVs.
Requires MySQL running with form_d table loaded (run 05_load_mysql.py first).
Falls back to pandas if MySQL is unavailable.
"""

import pandas as pd
from pathlib import Path

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "@Vegito9616",
    "database": "form_d_analytics"
}


def get_connection():
    try:
        import mysql.connector
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"MySQL not available ({e}). Using pandas fallback.")
        return None


def run_mysql(conn):
    print("Running MySQL queries...\n")

    queries = {
        "fundraising_by_quarter": """
            SELECT filing_quarter,
                COUNT(*) AS total_filings,
                ROUND(SUM(total_offering_amount)/1e9,2) AS capital_raised_bn,
                ROUND(AVG(total_offering_amount)/1e6,2) AS avg_fund_size_mn,
                ROUND(SUM(amount_sold)/1e9,2) AS capital_closed_bn,
                ROUND(SUM(amount_sold)/NULLIF(SUM(total_offering_amount),0)*100,2) AS close_rate_pct
            FROM form_d
            WHERE quality_tier='Clean' AND filing_quarter IS NOT NULL
            GROUP BY filing_quarter ORDER BY filing_quarter
        """,
        "capital_by_asset_class": """
            SELECT asset_class_group, fund_type,
                COUNT(*) AS fund_count,
                ROUND(SUM(total_offering_amount)/1e9,2) AS total_capital_bn,
                ROUND(AVG(total_offering_amount)/1e6,2) AS avg_fund_size_mn,
                ROUND(SUM(total_offering_amount)*100.0/
                    (SELECT SUM(total_offering_amount) FROM form_d
                     WHERE quality_tier='Clean'),2) AS pct_of_total_capital
            FROM form_d WHERE quality_tier='Clean'
            GROUP BY asset_class_group, fund_type
            ORDER BY total_capital_bn DESC
        """,
        "exemption_breakdown": """
            SELECT federal_exemption,
                COUNT(*) AS filing_count,
                ROUND(SUM(total_offering_amount)/1e9,2) AS capital_raised_bn,
                ROUND(AVG(total_offering_amount)/1e6,2) AS avg_offering_mn,
                ROUND(COUNT(*)*100.0/
                    (SELECT COUNT(*) FROM form_d WHERE quality_tier='Clean'),2)
                    AS share_of_filings_pct
            FROM form_d WHERE quality_tier='Clean'
            GROUP BY federal_exemption ORDER BY filing_count DESC
        """,
        "fund_size_distribution": """
            SELECT fund_size_bucket,
                COUNT(*) AS fund_count,
                ROUND(SUM(total_offering_amount)/1e9,2) AS total_capital_bn,
                ROUND(COUNT(*)*100.0/
                    (SELECT COUNT(*) FROM form_d WHERE quality_tier='Clean'),2) AS pct_of_funds,
                ROUND(SUM(total_offering_amount)*100.0/
                    (SELECT SUM(total_offering_amount) FROM form_d
                     WHERE quality_tier='Clean'),2) AS pct_of_capital
            FROM form_d WHERE quality_tier='Clean'
            GROUP BY fund_size_bucket ORDER BY total_capital_bn DESC
        """,
        "investor_concentration": """
            SELECT investor_bucket,
                COUNT(*) AS fund_count,
                ROUND(SUM(total_offering_amount)/1e9,2) AS capital_raised_bn,
                ROUND(AVG(total_offering_amount)/1e6,2) AS avg_fund_size_mn,
                ROUND(SUM(total_offering_amount)*100.0/
                    (SELECT SUM(total_offering_amount) FROM form_d
                     WHERE quality_tier='Clean'),2) AS pct_of_total_capital
            FROM form_d WHERE quality_tier='Clean'
            GROUP BY investor_bucket ORDER BY capital_raised_bn DESC
        """,
        "yoy_by_asset_class": """
            SELECT filing_year, asset_class_group,
                COUNT(*) AS fund_count,
                ROUND(SUM(total_offering_amount)/1e9,2) AS capital_raised_bn,
                ROUND(AVG(total_offering_amount)/1e6,2) AS avg_fund_size_mn
            FROM form_d
            WHERE quality_tier='Clean' AND filing_year BETWEEN 2022 AND 2024
            GROUP BY filing_year, asset_class_group
            ORDER BY filing_year ASC, capital_raised_bn DESC
        """,
        "tableau_master": """
            SELECT issuer_name, filing_date, filing_year, filing_quarter,
                filing_month, fund_type, asset_class_group, federal_exemption,
                state_of_incorporation, total_offering_amount, amount_sold,
                pct_capital_raised, num_investors, investor_bucket,
                fund_size_bucket, quality_tier, is_duplicate, is_outlier_offering
            FROM form_d ORDER BY filing_date DESC
        """
    }

    results = {}
    for name, sql in queries.items():
        df = pd.read_sql(sql, conn)
        df.to_csv(OUTPUTS_DIR / f"{name}.csv", index=False)
        print(f"  {name}.csv — {len(df)} rows")
        results[name] = df
    return results


def run_pandas(df):
    print("Running pandas analysis (MySQL fallback)...\n")
    clean = df[df["quality_tier"] == "Clean"].copy()
    total_cap = clean["total_offering_amount"].sum()
    results = {}

    q1 = clean.groupby("filing_quarter").agg(
        total_filings=("issuer_name","count"),
        capital_raised_bn=("total_offering_amount", lambda x: round(x.sum()/1e9,2)),
        avg_fund_size_mn=("total_offering_amount", lambda x: round(x.mean()/1e6,2)),
        capital_closed_bn=("amount_sold", lambda x: round(x.sum()/1e9,2)),
    ).reset_index().sort_values("filing_quarter")
    q1.to_csv(OUTPUTS_DIR/"fundraising_by_quarter.csv", index=False)
    print(f"  fundraising_by_quarter.csv — {len(q1)} rows")
    results["fundraising_by_quarter"] = q1

    q2 = clean.groupby(["asset_class_group","fund_type"]).agg(
        fund_count=("issuer_name","count"),
        total_capital_bn=("total_offering_amount", lambda x: round(x.sum()/1e9,2)),
        avg_fund_size_mn=("total_offering_amount", lambda x: round(x.mean()/1e6,2)),
    ).reset_index()
    q2["pct_of_total_capital"] = (q2["total_capital_bn"]*1e9/total_cap*100).round(2)
    q2 = q2.sort_values("total_capital_bn", ascending=False)
    q2.to_csv(OUTPUTS_DIR/"capital_by_asset_class.csv", index=False)
    print(f"  capital_by_asset_class.csv — {len(q2)} rows")
    results["capital_by_asset_class"] = q2

    q3 = clean.groupby("federal_exemption").agg(
        filing_count=("issuer_name","count"),
        capital_raised_bn=("total_offering_amount", lambda x: round(x.sum()/1e9,2)),
        avg_offering_mn=("total_offering_amount", lambda x: round(x.mean()/1e6,2)),
    ).reset_index()
    q3["share_of_filings_pct"] = (q3["filing_count"]/len(clean)*100).round(2)
    q3.to_csv(OUTPUTS_DIR/"exemption_breakdown.csv", index=False)
    print(f"  exemption_breakdown.csv — {len(q3)} rows")
    results["exemption_breakdown"] = q3

    q4 = clean.groupby("fund_size_bucket").agg(
        fund_count=("issuer_name","count"),
        total_capital_bn=("total_offering_amount", lambda x: round(x.sum()/1e9,2)),
    ).reset_index()
    q4["pct_of_funds"] = (q4["fund_count"]/len(clean)*100).round(2)
    q4["pct_of_capital"] = (q4["total_capital_bn"]*1e9/total_cap*100).round(2)
    q4.to_csv(OUTPUTS_DIR/"fund_size_distribution.csv", index=False)
    print(f"  fund_size_distribution.csv — {len(q4)} rows")
    results["fund_size_distribution"] = q4

    q5 = clean.groupby("investor_bucket").agg(
        fund_count=("issuer_name","count"),
        capital_raised_bn=("total_offering_amount", lambda x: round(x.sum()/1e9,2)),
        avg_fund_size_mn=("total_offering_amount", lambda x: round(x.mean()/1e6,2)),
    ).reset_index()
    q5["pct_of_total_capital"] = (q5["capital_raised_bn"]*1e9/total_cap*100).round(2)
    q5.to_csv(OUTPUTS_DIR/"investor_concentration.csv", index=False)
    print(f"  investor_concentration.csv — {len(q5)} rows")
    results["investor_concentration"] = q5

    q6 = clean[clean["filing_year"].between(2022,2024)].groupby(
        ["filing_year","asset_class_group"]
    ).agg(
        fund_count=("issuer_name","count"),
        capital_raised_bn=("total_offering_amount", lambda x: round(x.sum()/1e9,2)),
        avg_fund_size_mn=("total_offering_amount", lambda x: round(x.mean()/1e6,2)),
    ).reset_index().sort_values(["filing_year","capital_raised_bn"], ascending=[True,False])
    q6.to_csv(OUTPUTS_DIR/"yoy_by_asset_class.csv", index=False)
    print(f"  yoy_by_asset_class.csv — {len(q6)} rows")
    results["yoy_by_asset_class"] = q6

    cols = ["issuer_name","filing_date","filing_year","filing_quarter",
            "filing_month","fund_type","asset_class_group","federal_exemption",
            "total_offering_amount","amount_sold","pct_capital_raised",
            "num_investors","investor_bucket","fund_size_bucket",
            "quality_tier","is_duplicate","is_outlier_offering"]
    tableau = df[[c for c in cols if c in df.columns]].copy()
    tableau.to_csv(OUTPUTS_DIR/"tableau_master.csv", index=False)
    print(f"  tableau_master.csv — {len(tableau)} rows")
    results["tableau_master"] = tableau

    return results


def print_findings(results):
    print("\n" + "="*60)
    print("KEY FINDINGS")
    print("="*60)
    qt = results.get("fundraising_by_quarter", pd.DataFrame())
    if not qt.empty:
        top = qt.loc[qt["total_filings"].idxmax()]
        print(f"\nPeak quarter: {top['filing_quarter']} — "
              f"{int(top['total_filings'])} filings, ${top['capital_raised_bn']}B raised")
    ac = results.get("capital_by_asset_class", pd.DataFrame())
    if not ac.empty:
        top = ac.iloc[0]
        print(f"Largest asset class: {top['asset_class_group']} — "
              f"${top['total_capital_bn']}B ({int(top['fund_count'])} funds)")
    ex = results.get("exemption_breakdown", pd.DataFrame())
    if not ex.empty:
        top = ex.iloc[0]
        print(f"Dominant exemption: {top['federal_exemption']} "
              f"({top['share_of_filings_pct']}% of filings)")
    fs = results.get("fund_size_distribution", pd.DataFrame())
    if not fs.empty:
        top = fs.iloc[0]
        print(f"Capital concentration: {top['fund_size_bucket']} = "
              f"{top['pct_of_capital']}% of capital, {top['pct_of_funds']}% of funds")


def main():
    flagged_path = PROCESSED_DIR/"form_d_flagged.csv"
    if not flagged_path.exists():
        print("Run 03_data_quality_audit.py first.")
        return
    df = pd.read_csv(flagged_path)
    print(f"Loaded {len(df)} records.")
    conn = get_connection()
    if conn:
        results = run_mysql(conn)
        conn.close()
    else:
        results = run_pandas(df)
    print_findings(results)
    print(f"\nOutputs saved to: {OUTPUTS_DIR}/")


if __name__ == "__main__":
    main()
