import pytest
import pandas as pd
from src.process_data import validate_data, process_sales_data

def test_validate_data_success():
    valid_data = pd.DataFrame({
        'order_id': [1, 2],
        'order_date': ['2026-01-01', '2026-01-02'],
        'product': ['A', 'B'],
        'category': ['Cat1', 'Cat2'],
        'quantity': [2, 5],
        'unit_price': [10.0, 20.0],
        'region': ['North', 'South']
    })
    validate_data(valid_data)  # Should not raise

def test_validate_data_missing_column():
    invalid_data = pd.DataFrame({
        'order_id': [1],
        'product': ['A']
    })
    with pytest.raises(ValueError, match="Missing required columns"):
        validate_data(invalid_data)

def test_validate_data_null_order_id():
    invalid_data = pd.DataFrame({
        'order_id': [1, None],
        'order_date': ['2026-01-01', '2026-01-02'],
        'product': ['A', 'B'],
        'category': ['Cat1', 'Cat2'],
        'quantity': [2, 5],
        'unit_price': [10.0, 20.0],
        'region': ['North', 'South']
    })
    with pytest.raises(ValueError, match="null values in order_id"):
        validate_data(invalid_data)

def test_validate_data_invalid_quantity():
    invalid_data = pd.DataFrame({
        'order_id': [1],
        'order_date': ['2026-01-01'],
        'product': ['A'],
        'category': ['Cat1'],
        'quantity': [0],  # Invalid <= 0
        'unit_price': [10.0],
        'region': ['North']
    })
    with pytest.raises(ValueError, match="quantity must be strictly greater than 0"):
        validate_data(invalid_data)

def test_validate_data_negative_unit_price():
    invalid_data = pd.DataFrame({
        'order_id': [1],
        'order_date': ['2026-01-01'],
        'product': ['A'],
        'category': ['Cat1'],
        'quantity': [2],
        'unit_price': [-5.0],  # Invalid < 0
        'region': ['North']
    })
    with pytest.raises(ValueError, match="unit_price must be non-negative"):
        validate_data(invalid_data)

def test_revenue_calculation(tmp_path):
    csv_file = tmp_path / "sample.csv"
    parquet_file = tmp_path / "sample.parquet"
    
    df_raw = pd.DataFrame({
        'order_id': [100],
        'order_date': ['2026-01-10'],
        'product': ['Widget'],
        'category': ['Gadgets'],
        'quantity': [3],
        'unit_price': [15.50],
        'region': ['East']
    })
    df_raw.to_csv(csv_file, index=False)
    
    processed_df = process_sales_data(str(csv_file), str(parquet_file))
    
    assert processed_df.loc[0, 'revenue'] == 46.50
    assert parquet_file.exists()
