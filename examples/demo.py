import os
import sys
import pandas as pd
from pandera.pandas import Column, Check, DataFrameSchema

# Add project root to sys.path
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from src.dataflip import DataFlipEngine

# Canonical sales dataset schema definition using Pandera DataFrameSchema
SALES_SCHEMA = DataFrameSchema({
    "order_id": Column(int, nullable=False),
    "order_date": Column(str, nullable=False),
    "product": Column(str, nullable=False),
    "category": Column(str, nullable=False),
    "quantity": Column(int, Check.gt(0)),
    "unit_price": Column(float, Check.ge(0)),
    "region": Column(str, nullable=False),
    "revenue": Column(float, Check.ge(0)),
})


def run_dataflip_demo(root_dir: str = None) -> None:
    """Demonstrate full DataFlip lifecycle: Valid Release, Rollback, and Failed Release."""
    target_dir = root_dir or base_dir

    print("=" * 65)
    print("DATAFLIP SCHEMA-DRIVEN ENGINE DEMONSTRATION")
    print("=" * 65)

    # 1. Initialize schema-driven engine
    engine = DataFlipEngine(target_dir, schema=SALES_SCHEMA, dataset_filename="sales.parquet")
    engine.init_environment()

    # 2. Ensure baseline BLUE exists
    blue_parquet = os.path.join(engine.blue_dir, "sales.parquet")
    df_blue = pd.DataFrame({
        "order_id": [101, 102],
        "order_date": ["2026-01-01", "2026-01-02"],
        "product": ["Blue Mouse", "Blue Keyboard"],
        "category": ["Electronics", "Electronics"],
        "quantity": [1, 2],
        "unit_price": [25.00, 50.00],
        "region": ["North", "East"],
        "revenue": [25.00, 100.00],
    })
    df_blue.to_parquet(blue_parquet, index=False)

    print(f"\n[STEP 1] Baseline State: Active dataset = {(engine.get_manifest().get('active_dataset') or 'blue').upper()}")

    # 3. Demo Valid GREEN Release
    print("\n[STEP 2] Generating Valid GREEN Candidate...")
    green_parquet = os.path.join(engine.green_dir, "sales.parquet")
    df_green_valid = pd.DataFrame({
        "order_id": [201, 202],
        "order_date": ["2026-01-10", "2026-01-11"],
        "product": ["Green Desk", "Green Chair"],
        "category": ["Furniture", "Furniture"],
        "quantity": [2, 1],
        "unit_price": [300.00, 150.00],
        "region": ["West", "South"],
        "revenue": [600.00, 150.00],
    })
    df_green_valid.to_parquet(green_parquet, index=False)

    success, msg = engine.deploy_green(green_parquet)
    print(f" -> Result: {msg}")
    print(f" -> Active Dataset is now: {(engine.get_manifest().get('active_dataset') or 'green').upper()}")

    # 4. Demo Rollback
    print("\n[STEP 3] Triggering Rollback to BLUE...")
    rb_success, rb_msg = engine.rollback()
    print(f" -> Result: {rb_msg}")
    print(f" -> Active Dataset is now: {(engine.get_manifest().get('active_dataset') or 'blue').upper()}")

    # 5. Demo Invalid GREEN Release (fails validation due to quantity = -5)
    print("\n[STEP 4] Generating Broken GREEN Candidate (Invalid quantity = -5)...")
    df_green_broken = pd.DataFrame({
        "order_id": [301],
        "order_date": ["2026-01-15"],
        "product": ["Broken Item"],
        "category": ["Electronics"],
        "quantity": [-5],
        "unit_price": [10.00],
        "region": ["North"],
        "revenue": [-50.00],
    })
    df_green_broken.to_parquet(green_parquet, index=False)

    fail_success, fail_msg = engine.deploy_green(green_parquet)
    print(f" -> Result: {fail_msg}")
    print(f" -> Active Dataset remains: {(engine.get_manifest().get('active_dataset') or 'blue').upper()}")
    print("=" * 65)


if __name__ == "__main__":
    run_dataflip_demo()
