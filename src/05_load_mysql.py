"""
05_load_mysql.py
Loads cleaned Form D data into MySQL.
Update DB_CONFIG with your local MySQL credentials.
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
from pathlib import Path

# ── Update these with your MySQL credentials ──────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "@Vegito9616",   # ← change this
    "database": "form_d_analytics"
}
# ─────────────────────────────────────────────────────────────────────────────

PROCESSED_DIR = Path("data/processed")


def get_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("MySQL connection established.")
        return conn
    except Error as e:
        print(f"Connection error: {e}")
        raise


def create_schema(conn):
    cursor = conn.cursor()
    schema_path = Path("sql/schema.sql")
    sql = schema_path.read_text()

    # Execute each statement separately
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            cursor.execute(statement)
    conn.commit()
    cursor.close()
    print("Schema created.")


def load_data(conn, df):
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO form_d (
            accession_number, cik, issuer_name, issuer_name_clean,
            filing_date, filing_year, filing_quarter, filing_month,
            form_type, fund_type, asset_class_group, federal_exemption,
            state_of_incorporation, total_offering_amount, amount_sold,
            pct_capital_raised, num_investors, investor_bucket,
            fund_size_bucket, quality_tier, is_duplicate, is_outlier_offering
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
    """

    rows = []
    for _, row in df.iterrows():
        rows.append((
            str(row.get("accession_number", ""))[:30] or None,
            str(row.get("cik", ""))[:15] or None,
            str(row.get("issuer_name", ""))[:255],
            str(row.get("issuer_name_clean", ""))[:255] or None,
            row.get("filing_date") if pd.notna(row.get("filing_date")) else None,
            int(row["filing_year"]) if pd.notna(row.get("filing_year")) else None,
            str(row.get("filing_quarter", ""))[:8] or None,
            str(row.get("filing_month", ""))[:8] or None,
            str(row.get("form_type", ""))[:5] or None,
            str(row.get("fund_type", ""))[:60],
            str(row.get("asset_class_group", ""))[:60],
            str(row.get("federal_exemption", ""))[:20] if pd.notna(row.get("federal_exemption")) else None,
            str(row.get("state_of_incorporation", ""))[:10] or None,
            float(row["total_offering_amount"]) if pd.notna(row.get("total_offering_amount")) else None,
            float(row["amount_sold"]) if pd.notna(row.get("amount_sold")) else None,
            float(row["pct_capital_raised"]) if pd.notna(row.get("pct_capital_raised")) else None,
            int(row["num_investors"]) if pd.notna(row.get("num_investors")) else None,
            str(row.get("investor_bucket", ""))[:30] or None,
            str(row.get("fund_size_bucket", ""))[:30] or None,
            str(row.get("quality_tier", ""))[:20] or None,
            int(row["is_duplicate"]) if pd.notna(row.get("is_duplicate")) else 0,
            int(row["is_outlier_offering"]) if pd.notna(row.get("is_outlier_offering")) else 0,
        ))

    # Batch insert in chunks of 500
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        cursor.executemany(insert_sql, rows[i:i+batch_size])
        conn.commit()
        print(f"  Inserted {min(i+batch_size, len(rows))}/{len(rows)} records...")

    cursor.close()
    print(f"Load complete: {len(rows)} records inserted into form_d.")


def verify(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*), MIN(filing_date), MAX(filing_date) FROM form_d;")
    count, min_date, max_date = cursor.fetchone()
    print(f"\nVerification: {count} records | {min_date} to {max_date}")
    cursor.execute("""
        SELECT quality_tier, COUNT(*) as n
        FROM form_d GROUP BY quality_tier ORDER BY n DESC;
    """)
    print("Quality tier breakdown:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")
    cursor.close()


def main():
    flagged_path = PROCESSED_DIR / "form_d_flagged.csv"
    if not flagged_path.exists():
        print("Flagged data not found. Run 03_data_quality_audit.py first.")
        return

    df = pd.read_csv(flagged_path)
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce").dt.date
    print(f"Loaded {len(df)} records from CSV.")

    conn = get_connection()
    create_schema(conn)
    load_data(conn, df)
    verify(conn)
    conn.close()


if __name__ == "__main__":
    main()
