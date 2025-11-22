import os
import sqlite3
from textwrap import dedent

# ---- Paths ----
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "ai_analytics.db")
TABLE_NAME = "grocery_sales"


def get_table_schema():
    """
    Get column names and types from the SQLite table.
    Useful for debugging and for LLM prompts in future.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({TABLE_NAME});")
    rows = cur.fetchall()
    conn.close()

    schema = []
    for row in rows:
        # row: (cid, name, type, notnull, dflt_value, pk)
        schema.append({"name": row[1], "type": row[2]})
    return schema


def print_schema():
    """
    Utility: prints schema nicely.
    """
    schema = get_table_schema()
    print(f"Schema for table '{TABLE_NAME}':")
    for col in schema:
        print(f" - {col['name']} ({col['type']})")


def rule_based_sql_generator(question: str) -> str:
    """
    Very simple rule-based 'AI' that maps
    some natural language questions to SQL queries.

    This lets the pipeline work even without a real LLM.
    You can expand this with more patterns later.
    """

    q = question.lower()

    # 1) Top 10 products by total sales
    if "top" in q and "product" in q and "sales" in q:
        return dedent(f"""
        SELECT "Product line" AS product_line,
               SUM(Sales) AS total_sales
        FROM {TABLE_NAME}
        GROUP BY "Product line"
        ORDER BY total_sales DESC
        LIMIT 10;
        """)

    # 2) Total revenue by city
    if "revenue" in q and "city" in q:
        return dedent(f"""
        SELECT City,
               SUM(Sales) AS total_revenue
        FROM {TABLE_NAME}
        GROUP BY City
        ORDER BY total_revenue DESC;
        """)

    # 3) Average rating by product line
    if "average rating" in q or ("rating" in q and "product" in q):
        return dedent(f"""
        SELECT "Product line" AS product_line,
               AVG(Rating) AS avg_rating
        FROM {TABLE_NAME}
        GROUP BY "Product line"
        ORDER BY avg_rating DESC;
        """)

    # 4) Sales by day of week
    if "day" in q and "week" in q:
        return dedent(f"""
        SELECT "day_of_week",
               SUM(Sales) AS total_sales
        FROM {TABLE_NAME}
        GROUP BY "day_of_week"
        ORDER BY total_sales DESC;
        """)

    # 5) Revenue by customer type
    if "customer type" in q and ("revenue" in q or "sales" in q):
        return dedent(f"""
        SELECT "Customer type" AS customer_type,
               SUM(Sales) AS total_revenue
        FROM {TABLE_NAME}
        GROUP BY "Customer type"
        ORDER BY total_revenue DESC;
        """)

    # Default fallback
    return dedent(f"""
    -- Default fallback query if no rule matched.
    -- You can extend 'rule_based_sql_generator' with more patterns.
    SELECT *
    FROM {TABLE_NAME}
    LIMIT 20;
    """)


def generate_sql_from_question(question: str) -> str:
    """
    Wrapper that currently uses rule-based logic,
    but can be swapped later to a real LLM (OpenAI, Gemini, etc.).
    """

    # 🔹 For now: rule-based generator
    sql_query = rule_based_sql_generator(question)

    # 🔹 In future, you can do something like:
    # prompt = f"Given this table schema: {get_table_schema()}, write a SQL query to answer: {question}"
    # sql_query = call_llm_api(prompt)  # TODO: implement LLM here

    return sql_query


if __name__ == "__main__":
    # Quick debug run
    print_schema()
    q = "Show me top 10 products by sales"
    print("\nQuestion:", q)
    print("\nGenerated SQL:")
    print(generate_sql_from_question(q))
