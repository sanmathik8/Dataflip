import os
import sys
import duckdb

def run_analytics(parquet_path: str) -> None:
    """Execute SQL analytics on Parquet data using DuckDB."""
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet file not found at: {parquet_path}")
        
    conn = duckdb.connect(database=':memory:')
    
    print("=" * 50)
    print("DATAFLIP LOCAL ANALYTICS REPORT (DuckDB)")
    print("=" * 50)
    
    # 1. Total Revenue
    total_rev_df = conn.execute("""
        SELECT 
            ROUND(SUM(revenue), 2) AS total_revenue,
            COUNT(order_id) AS total_orders
        FROM read_parquet(?)
    """, [parquet_path]).df()
    
    print("\n--- Total Revenue ---")
    print(total_rev_df.to_string(index=False))
    
    # 2. Revenue by Category
    cat_df = conn.execute("""
        SELECT 
            category,
            ROUND(SUM(revenue), 2) AS category_revenue,
            COUNT(order_id) AS total_orders
        FROM read_parquet(?)
        GROUP BY category
        ORDER BY category_revenue DESC
    """, [parquet_path]).df()
    
    print("\n--- Revenue by Category ---")
    print(cat_df.to_string(index=False))
    
    # 3. Revenue by Region
    region_df = conn.execute("""
        SELECT 
            region,
            ROUND(SUM(revenue), 2) AS region_revenue,
            COUNT(order_id) AS total_orders
        FROM read_parquet(?)
        GROUP BY region
        ORDER BY region_revenue DESC
    """, [parquet_path]).df()
    
    print("\n--- Revenue by Region ---")
    print(region_df.to_string(index=False))
    print("=" * 50)

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    from src.dataflip import DataFlipEngine
    
    engine = DataFlipEngine(base_dir)
    engine.init_environment()
    parquet_file = engine.get_active_dataset_path()
    
    # Fallback to output/sales.parquet if active does not exist yet
    if not os.path.exists(parquet_file):
        parquet_file = os.path.join(base_dir, "data", "output", "sales.parquet")
    
    try:
        run_analytics(parquet_file)
    except Exception as e:
        print(f"Analytics Query Failed: {e}", file=sys.stderr)
        sys.exit(1)
