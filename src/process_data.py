import os
import sys
import pandas as pd

REQUIRED_COLUMNS = [
    'order_id',
    'order_date',
    'product',
    'category',
    'quantity',
    'unit_price',
    'region',
]

def validate_data(df: pd.DataFrame) -> None:
    """Validate schema and data quality rules."""
    # Check required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Check nulls in order_id
    if df['order_id'].isnull().any():
        raise ValueError("Found null values in order_id column")
        
    # Validate order_date parsing
    try:
        pd.to_datetime(df['order_date'], format='%Y-%m-%d', errors='raise')
    except Exception as e:
        raise ValueError(f"Invalid order_date format detected: {e}")
        
    # Check numeric types & constraints
    if not pd.api.types.is_numeric_dtype(df['quantity']):
        raise ValueError("quantity column must be numeric")
    if (df['quantity'] <= 0).any():
        raise ValueError("quantity must be strictly greater than 0")
        
    if not pd.api.types.is_numeric_dtype(df['unit_price']):
        raise ValueError("unit_price column must be numeric")
    if (df['unit_price'] < 0).any():
        raise ValueError("unit_price must be non-negative")

def process_sales_data(input_csv: str, output_parquet: str) -> pd.DataFrame:
    """Read CSV, validate, clean, calculate revenue, and save to Parquet."""
    print(f"Reading raw sales data from: {input_csv}")
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input CSV file not found: {input_csv}")
        
    df = pd.read_csv(input_csv)
    
    # Validate data quality
    validate_data(df)
    
    # Cleaning & Transformation
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['revenue'] = (df['quantity'] * df['unit_price']).round(2)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)
    
    # Save to Parquet
    df.to_parquet(output_parquet, index=False)
    print(f"Successfully processed {len(df)} records. Saved Parquet to: {output_parquet}")
    return df

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_path = os.path.join(base_dir, "data", "raw", "sales.csv")
    output_path = os.path.join(base_dir, "data", "output", "sales.parquet")
    
    try:
        process_sales_data(input_path, output_path)
    except Exception as e:
        print(f"Processing Failed: {e}", file=sys.stderr)
        sys.exit(1)
