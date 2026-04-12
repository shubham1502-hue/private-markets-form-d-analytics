"""
main.py
Runs the full Form D analytics pipeline in sequence.

Step 1: Acquire real SEC EDGAR Form D filings (or generate synthetic if offline)
Step 2: Clean and standardise the raw data
Step 3: Run data quality audit and flag issues
Step 4: Export Tableau-ready CSVs via MySQL (falls back to pandas if MySQL unavailable)

MySQL setup (optional but recommended):
  1. Run sql/schema.sql in MySQL Workbench to create the database and table
  2. Update DB_CONFIG in src/05_load_mysql.py with your credentials
  3. Run src/05_load_mysql.py to load the data
  4. src/04_sql_analysis.py will then use MySQL automatically
"""

import subprocess
import sys

PIPELINE = [
    ("Data Acquisition",    "src/01_acquire_data.py"),
    ("Clean & Standardise", "src/02_clean_standardise.py"),
    ("Data Quality Audit",  "src/03_data_quality_audit.py"),
    ("SQL Analysis",        "src/04_sql_analysis.py"),
]


def run(label, script):
    print(f"\n{'='*60}")
    print(f"STEP: {label}")
    print(f"Script: {script}")
    print("="*60)
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"\nERROR in {script}. Pipeline stopped.")
        sys.exit(1)


if __name__ == "__main__":
    print("Private Markets Fundraising Intelligence")
    print("SEC Form D Analytics Pipeline")
    print("="*60)

    for label, script in PIPELINE:
        run(label, script)

    print("\n" + "="*60)
    print("Pipeline complete.")
    print("Outputs saved to: outputs/")
    print("Next steps:")
    print("  1. Load outputs/tableau_master.csv into Tableau Public")
    print("  2. Optional: run src/05_load_mysql.py to load into MySQL")
    print("  3. Then re-run src/04_sql_analysis.py for MySQL-powered queries")
    print("="*60)
