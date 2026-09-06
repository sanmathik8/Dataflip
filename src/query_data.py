import os
import sys
import duckdb

def run_analytics(parquet_path: str) -> None:
    """Execute SQL analytics on Parquet data using DuckDB."""
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet file not found at: {parquet_path}")

    conn = duckdb.connect(':memory:')
    print("=" * 50 + "\nDATAFLIP LOCAL ANALYTICS REPORT (DuckDB)\n" + "=" * 50)

    queries = [
        ("Total Revenue", "SELECT ROUND(SUM(revenue), 2) AS total_revenue, COUNT(order_id) AS total_orders FROM read_parquet(?)"),
        ("Revenue by Category", "SELECT category, ROUND(SUM(revenue), 2) AS category_revenue, COUNT(order_id) AS total_orders FROM read_parquet(?) GROUP BY category ORDER BY category_revenue DESC"),
        ("Revenue by Region", "SELECT region, ROUND(SUM(revenue), 2) AS region_revenue, COUNT(order_id) AS total_orders FROM read_parquet(?) GROUP BY region ORDER BY region_revenue DESC")
    ]

    for title, sql in queries:
        print(f"\n--- {title} ---")
        print(conn.execute(sql, [parquet_path]).df().to_string(index=False))

    print("=" * 50)

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, base_dir)
    from src.dataflip import DataFlipEngine
    engine = DataFlipEngine(base_dir)

    parquet_file = engine.get_active_dataset_path()
    if not os.path.exists(parquet_file):
        parquet_file = os.path.join(base_dir, "data", "output", "sales.parquet")

    try:
        run_analytics(parquet_file)
    except Exception as e:
        print(f"Analytics Query Failed: {e}", file=sys.stderr)
        sys.exit(1)
