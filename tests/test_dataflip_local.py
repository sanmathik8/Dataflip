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

    # Create initial known-good BLUE dataset with generic customer schema
    blue_parquet = os.path.join(engine.blue_dir, "sales.parquet")
    df_blue = pd.DataFrame({
        'customer_id': [1, 2],
        'name': ['Alice', 'Bob'],
        'email': ['alice@example.com', 'bob@example.com']
    })
    df_blue.to_parquet(blue_parquet, index=False)

    return engine

def test_1_valid_green_activates_successfully(temp_engine):
    """Test 1: Valid GREEN candidate dataset activates successfully."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_green = pd.DataFrame({
        'sensor_id': ['SN-100', 'SN-101'],
        'temperature': [28.5, 31.0],
        'humidity': [65, 58],
        'timestamp': ['2026-08-15 10:00:00', '2026-08-15 10:05:00']
    })
    df_green.to_parquet(green_parquet, index=False)

    res = temp_engine.deploy_green(green_parquet)
    success, msg = res[0], res[1]
    assert success is True
    assert "RELEASE SUCCESS" in msg or "GREEN activated" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'green'

def test_2_empty_dataset_keeps_blue_active(temp_engine):
    """Test 2: Empty dataset keeps BLUE active."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_empty = pd.DataFrame()
    df_empty.to_parquet(green_parquet, index=False)

    res = temp_engine.deploy_green(green_parquet)
    success, msg = res[0], res[1]
    assert success is False
    assert "Dataset is empty" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_3_missing_file_keeps_blue_active(temp_engine):
    """Test 3: Non-existent file keeps BLUE active."""
    green_parquet = os.path.join(temp_engine.green_dir, "non_existent.parquet")

    res = temp_engine.deploy_green(green_parquet)
    success, msg = res[0], res[1]
    assert success is False
    assert "File does not exist" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_4_rollback_changes_active_back_to_blue(temp_engine):
    """Test 4: Rollback changes active dataset back to BLUE."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_green = pd.DataFrame({
        'id': [10, 11],
        'val': ['X', 'Y']
    })
    df_green.to_parquet(green_parquet, index=False)
    temp_engine.deploy_green(green_parquet)
    assert temp_engine.get_manifest()['active_dataset'] == 'green'

    res = temp_engine.rollback()
    rollback_success, msg = res[0], res[1]
    assert rollback_success is True
    assert "reverted" in msg or "ROLLBACK SUCCESS" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_5_pandera_schema_validation(tmp_path):
    """Test 5: Verification of declarative Pandera schema validation."""
    from pandera.pandas import Column, Check, DataFrameSchema

    schema = DataFrameSchema({
        "id": Column(int, nullable=False),
        "val": Column(float, Check.gt(0))
    })
    engine = DataFlipEngine(str(tmp_path), schema=schema)
    engine.init_environment()

    # Invalid candidate (negative val)
    green_parquet = os.path.join(engine.green_dir, "sales.parquet")
    df_invalid = pd.DataFrame({'id': [1], 'val': [-10.0]})
    df_invalid.to_parquet(green_parquet, index=False)

    success, msg = engine.deploy_green(green_parquet)
    assert success is False
    assert "RELEASE REJECTED" in msg
    assert "val" in msg

def test_6_rejected_release_preserves_blue_state(temp_engine):
    """Test 6: Verification that rejected releases preserve BLUE manifest state."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_empty = pd.DataFrame()
    df_empty.to_parquet(green_parquet, index=False)

    res = temp_engine.deploy_green(green_parquet)
    success, msg = res[0], res[1]
    assert success is False
    assert "RELEASE REJECTED" in msg
    assert temp_engine.get_manifest()['active_dataset'] == 'blue'

def test_7_simplified_manifest_structure(temp_engine):
    """Test 7: Verification of simplified manifest metadata."""
    manifest = temp_engine.get_manifest()
    assert manifest["active_dataset"] == "blue"
    assert manifest["status"] == "active"
    assert "details" in manifest
    assert "active_path" in manifest
    assert "versions" not in manifest
    assert "active_version" not in manifest

def test_8_rollback_when_already_blue(temp_engine):
    """Test 8: Rollback when BLUE is already active."""
    res = temp_engine.rollback()
    success, msg = res[0], res[1]
    assert success is False
    assert "Rollback" in msg

def test_9_rollback_failed_when_blue_missing(temp_engine):
    """Test 9: Rollback fails gracefully when known-good BLUE dataset is missing."""
    green_parquet = os.path.join(temp_engine.green_dir, "sales.parquet")
    df_green = pd.DataFrame({'a': [1], 'b': [2]})
    df_green.to_parquet(green_parquet, index=False)
    temp_engine.deploy_green(green_parquet)

    blue_parquet = os.path.join(temp_engine.blue_dir, "sales.parquet")
    if os.path.exists(blue_parquet):
        os.remove(blue_parquet)

    res = temp_engine.rollback()
    assert res[0] is False
    assert "missing" in res[1].lower() or "failed" in res[1].lower()
