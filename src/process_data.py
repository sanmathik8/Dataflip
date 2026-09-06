import os
import sys
import pandas as pd

def validate_data(df: pd.DataFrame) -> None:
    """Validate generic dataset rules (non-empty and contains columns)."""
    if df.empty:
        raise ValueError("Dataset is empty (0 rows)")

    if len(df.columns) == 0:
        raise ValueError("Dataset has no columns")

def process_sales_data(input_csv: str, output_parquet: str) -> pd.DataFrame:
    """Read any CSV, validate generically, and save to Parquet format."""
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"Input CSV file not found: {input_csv}")

    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        raise ValueError(f"CSV could not be read: {e}")

    validate_data(df)

    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)
    df.to_parquet(output_parquet, index=False)
    print(f"Processed {len(df)} records -> {output_parquet}")
    return df

process_dataset = process_sales_data

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    try:
        process_sales_data(os.path.join(base_dir, "data", "raw", "sales.csv"),
                           os.path.join(base_dir, "data", "output", "sales.parquet"))
    except Exception as e:
        print(f"Processing Failed: {e}", file=sys.stderr)
        sys.exit(1)
