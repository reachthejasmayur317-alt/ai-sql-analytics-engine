import os
import sqlite3
import pandas as pd

# ---- Paths ----
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_PATH = os.path.join(PROJECT_ROOT, "data", "grocery_sales.csv")
DB_DIR = os.path.join(PROJECT_ROOT, "db")
os.makedirs(DB_DIR, exist_ok=True)

DB_PATH = os.path.join(DB_DIR, "ai_analytics.db")
TABLE_NAME = "grocery_sales"


def main():
    print(f" Loading CSV from: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)

    print(" CSV loaded. Shape:", df.shape)
    print("Columns:", df.columns.tolist())

    # Connect to SQLite
    print(f"\n Creating / connecting to SQLite DB at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    # Write DataFrame to SQL table
    print(f" Writing data to table: {TABLE_NAME}")
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

    # Quick test: count rows
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    row_count = cur.fetchone()[0]
    print(f" Table '{TABLE_NAME}' created with {row_count} rows.")

    # Optional: show 5 rows
    cur.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5;")
    sample_rows = cur.fetchall()
    print("\n Sample rows from DB:")
    for row in sample_rows:
        print(row)

    conn.close()
    print("\n Done! SQLite DB ready for AI SQL engine.")


if __name__ == "__main__":
    main()
