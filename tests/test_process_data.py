import pytest
import pandas as pd
from src.process_data import validate_data, process_sales_data

def test_validate_data_customer_schema():
    """Test generic validation on customer dataset."""
    df_customers = pd.DataFrame({
        'customer_id': [1, 2],
        'name': ['Alice', 'Bob'],
        'email': ['alice@example.com', 'bob@example.com']
    })
    validate_data(df_customers)  # Should not raise

def test_validate_data_iot_schema():
    """Test generic validation on IoT sensor dataset."""
    df_iot = pd.DataFrame({
        'temperature': [28, 30],
        'humidity': [65, 60],
        'timestamp': ['2026-08-15', '2026-08-16']
    })
    validate_data(df_iot)  # Should not raise

def test_validate_data_empty_df():
    """Test validation fails on empty dataframe."""
    df_empty = pd.DataFrame()
    with pytest.raises(ValueError, match="Dataset is empty"):
        validate_data(df_empty)

def test_process_generic_dataset(tmp_path):
    """Test reading and converting arbitrary CSV to Parquet."""
    csv_file = tmp_path / "sensor_data.csv"
    parquet_file = tmp_path / "sensor_data.parquet"

    df_raw = pd.DataFrame({
        'temperature': [28.5, 30.1],
        'humidity': [65, 60],
        'timestamp': ['2026-08-15', '2026-08-16']
    })
    df_raw.to_csv(csv_file, index=False)

    processed_df = process_sales_data(str(csv_file), str(parquet_file))

    assert len(processed_df) == 2
    assert 'temperature' in processed_df.columns
    assert parquet_file.exists()

def test_process_missing_file(tmp_path):
    """Test file not found handling."""
    missing_csv = str(tmp_path / "non_existent.csv")
    parquet_out = str(tmp_path / "out.parquet")
    with pytest.raises(FileNotFoundError):
        process_sales_data(missing_csv, parquet_out)
