import os
import pytest
import pandas as pd
from src.dataflip import DataFlipEngine

@pytest.fixture
def temp_engine(tmp_path):
    """Fixture initializing a clean isolated DataFlip environment."""
    base_dir = str(tmp_path)
    engine = DataFlipEngine(base_dir)
    engine.init_environment()
    
    # Create initial known-good BLUE dataset
    blue_parquet = os.path.join(engine.blue_dir, "sales.parquet")
    df_blue = pd.DataFrame({
        'order_id': [1],
        'order_date': ['2026-01-01'],
        'product': ['Blue Mouse'],
        'category': ['Electronics'],
        'quantity': [1],
        'unit_price': [10.00],
        'region': ['North'],
        'revenue': [10.00]
    })
    df_blue.to_parquet(blue_parquet, index=False)
    
    return engine

def test_1_valid_green_activates_successfully(temp_engine):
    """Test 1: Valid GREEN activates successfully."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_green = pd.DataFrame({
        'order_id': [2],
        'order_date': ['2026-01-02'],
        'product': ['Green Desk'],
        'category': ['Furniture'],
        'quantity': [2],
        'unit_price': [50.00],
        'region': ['East'],
        'revenue': [100.00]
    })
    df_green.to_parquet(green_parquet, index=False)
    
    success, msg = temp_engine.deploy_green(green_parquet)
    assert success is True
    assert "GREEN activated" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'green'
    assert temp_engine.get_active_dataset_path() == green_parquet

def test_2_invalid_schema_keeps_blue_active(temp_engine):
    """Test 2: Invalid schema keeps BLUE active."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_invalid = pd.DataFrame({
        'order_id': [2],
        'product': ['Green Desk']  # Missing required columns
    })
    df_invalid.to_parquet(green_parquet, index=False)
    
    success, msg = temp_engine.deploy_green(green_parquet)
    assert success is False
    assert "Missing required columns" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_3_invalid_quantity_keeps_blue_active(temp_engine):
    """Test 3: Invalid quantity keeps BLUE active."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_invalid = pd.DataFrame({
        'order_id': [2],
        'order_date': ['2026-01-02'],
        'product': ['Green Desk'],
        'category': ['Furniture'],
        'quantity': [-2],  # Invalid quantity <= 0
        'unit_price': [50.00],
        'region': ['East'],
        'revenue': [-100.00]
    })
    df_invalid.to_parquet(green_parquet, index=False)
    
    success, msg = temp_engine.deploy_green(green_parquet)
    assert success is False
    assert "quantity must be strictly > 0" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_4_invalid_revenue_keeps_blue_active(temp_engine):
    """Test 4: Invalid revenue keeps BLUE active."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_invalid = pd.DataFrame({
        'order_id': [2],
        'order_date': ['2026-01-02'],
        'product': ['Green Desk'],
        'category': ['Furniture'],
        'quantity': [2],
        'unit_price': [50.00],
        'region': ['East'],
        'revenue': [-50.00]  # Invalid negative revenue
    })
    df_invalid.to_parquet(green_parquet, index=False)
    
    success, msg = temp_engine.deploy_green(green_parquet)
    assert success is False
    assert "revenue must be >= 0" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_5_empty_dataset_keeps_blue_active(temp_engine):
    """Test 5: Empty dataset keeps BLUE active."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_empty = pd.DataFrame(columns=[
        'order_id', 'order_date', 'product', 'category', 'quantity', 'unit_price', 'region', 'revenue'
    ])
    df_empty.to_parquet(green_parquet, index=False)
    
    success, msg = temp_engine.deploy_green(green_parquet)
    assert success is False
    assert "Dataset is empty" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_6_rollback_changes_active_back_to_blue(temp_engine):
    """Test 6: Rollback changes active dataset back to BLUE."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_green = pd.DataFrame({
        'order_id': [2],
        'order_date': ['2026-01-02'],
        'product': ['Green Desk'],
        'category': ['Furniture'],
        'quantity': [2],
        'unit_price': [50.00],
        'region': ['East'],
        'revenue': [100.00]
    })
    df_green.to_parquet(green_parquet, index=False)
    temp_engine.deploy_green(green_parquet)
    assert temp_engine.get_manifest()['active_dataset'] == 'green'
    
    # Execute Rollback
    rollback_success, msg = temp_engine.rollback()
    assert rollback_success is True
    assert "reverted to BLUE" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'
