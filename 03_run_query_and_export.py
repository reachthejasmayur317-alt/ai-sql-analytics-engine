import os
import sqlite3
import pandas as pd

from src.ai_sql_engine import generate_sql_from_question, print_schema

# ---- Paths ----
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "ai_analytics.db")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_query_and_export(question: str):
    print(" User question:")
    print("  ", question)

    # 1. Generate SQL from question
    sql_query = generate_sql_from_question(question)

    print("\n Generated SQL query:")
    print(sql_query)

    # 2. Execute SQL on SQLite
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql_query, conn)
    except Exception as e:
        print("\n Error running SQL:", e)
        conn.close()
        return

    conn.close()

    if df.empty:
        print("\n⚠ Query returned no rows.")
        return

    print("\n Query executed. Sample results:")
    print(df.head())

    # 3. Export to CSV for Tableau / Power BI
    # create a simple filename slug
    safe_name = (
        question.lower()
        .replace(" ", "_")
        .replace("?", "")
        .replace("/", "_")
        [:50]
    )
    output_path = os.path.join(OUTPUT_DIR, f"{safe_name}.csv")

    df.to_csv(output_path, index=False)
    print(f"\n Results exported to: {output_path}")


def main():
    # Optional: print schema to understand the table
    print_schema()

    # Try a few demo questions
    questions = [
        "Show me top 10 products by sales",
        "What is the revenue by city?",
        "Show average rating by product line",
        "Sales by day of week",
        "Revenue by customer type"
    ]

    for q in questions:
        print("\n" + "=" * 80)
        run_query_and_export(q)


if __name__ == "__main__":
    main()
