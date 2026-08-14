import os
import json
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
    'revenue'
]

class DataFlipEngine:
    """Core local Blue/Green deployment engine for DataFlip."""

    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.data_dir = os.path.join(base_dir, 'data')
        self.blue_dir = os.path.join(self.data_dir, 'blue')
        self.green_dir = os.path.join(self.data_dir, 'green')
        self.raw_dir = os.path.join(self.data_dir, 'raw')
        self.manifest_path = os.path.join(self.data_dir, 'manifest.json')

    def init_environment(self) -> None:
        """Initialize directory structure and manifest state."""
        os.makedirs(self.blue_dir, exist_ok=True)
        os.makedirs(self.green_dir, exist_ok=True)
        os.makedirs(self.raw_dir, exist_ok=True)

        if not os.path.exists(self.manifest_path):
            self._update_manifest('blue', 'initialized', 'Initial BLUE environment')

    def _update_manifest(self, active_color: str, status: str, details: str) -> None:
        """Write metadata manifest state (Audit log). Glue Table location is primary in AWS."""
        manifest_data = {
            'active_dataset': active_color,
            'status': status,
            'details': details,
            'active_path': os.path.join(self.data_dir, active_color, 'sales.parquet')
        }
        with open(self.manifest_path, 'w') as f:
            json.dump(manifest_data, f, indent=2)

    def get_manifest(self) -> dict:
        """Read state from manifest."""
        if not os.path.exists(self.manifest_path):
            self.init_environment()
        with open(self.manifest_path, 'r') as f:
            return json.load(f)

    def get_active_dataset_path(self) -> str:
        """Return the file path of currently active production dataset."""
        manifest = self.get_manifest()
        return manifest['active_path']

    def validate_dataset(self, parquet_path: str) -> tuple[bool, str]:
        """Validate schema, null constraints, numeric bounds, and row counts."""
        if not os.path.exists(parquet_path):
            return False, f"File does not exist: {parquet_path}"

        try:
            df = pd.read_parquet(parquet_path)
        except Exception as e:
            return False, f"Failed to read Parquet file: {e}"

        # 1. Row count sanity check
        if len(df) == 0:
            return False, "Validation Failed: Dataset is empty (0 rows)"

        # 2. Required columns check
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_cols:
            return False, f"Validation Failed: Missing required columns {missing_cols}"

        # 3. Null check on order_id
        if df['order_id'].isnull().any():
            return False, "Validation Failed: Found null values in order_id"

        # 4. Numeric range validation
        if (df['quantity'] <= 0).any():
            return False, "Validation Failed: quantity must be strictly > 0"

        if (df['unit_price'] < 0).any():
            return False, "Validation Failed: unit_price must be >= 0"

        if (df['revenue'] < 0).any():
            return False, "Validation Failed: revenue must be >= 0"

        return True, "Validation Passed: Dataset is valid"

    def deploy_green(self, green_parquet_path: str) -> tuple[bool, str]:
        """Validate candidate GREEN dataset; activate if PASS, retain BLUE if FAIL."""
        is_valid, message = self.validate_dataset(green_parquet_path)

        if is_valid:
            self._update_manifest('green', 'active', f"GREEN release activated: {message}")
            return True, f"RELEASE SUCCESS: GREEN activated. {message}"
        else:
            current_active = self.get_manifest()['active_dataset']
            self._update_manifest(current_active, 'failed_candidate', f"GREEN rejected. Keeping {current_active}. Reason: {message}")
            return False, f"RELEASE REJECTED: {message}. Active dataset remains {current_active.upper()}."

    def rollback(self) -> tuple[bool, str]:
        """Rollback active dataset pointer back to BLUE."""
        current_active = self.get_manifest()['active_dataset']
        if current_active == 'blue':
            return False, "Rollback Not Needed: BLUE is already active."

        blue_parquet = os.path.join(self.blue_dir, 'sales.parquet')
        if not os.path.exists(blue_parquet):
            return False, "Rollback Failed: Known-good BLUE dataset missing."

        self._update_manifest('blue', 'rolled_back', "Manual rollback executed. BLUE reactivated.")
        return True, "ROLLBACK SUCCESS: Production dataset successfully reverted to BLUE."


def run_dataflip_demo(base_dir: str) -> None:
    """Demonstrate full DataFlip lifecycle: Valid Release, Failed Release, and Rollback."""
    print("=" * 65)
    print("DATAFLIP LOCAL ENGINE DEMONSTRATION")
    print("=" * 65)
    
    engine = DataFlipEngine(base_dir)
    engine.init_environment()
    
    # Ensure baseline BLUE exists
    blue_parquet = os.path.join(engine.blue_dir, 'sales.parquet')
    df_blue = pd.DataFrame({
        'order_id': [101, 102],
        'order_date': ['2026-01-01', '2026-01-02'],
        'product': ['Blue Mouse', 'Blue Keyboard'],
        'category': ['Electronics', 'Electronics'],
        'quantity': [1, 2],
        'unit_price': [25.00, 50.00],
        'region': ['North', 'East'],
        'revenue': [25.00, 100.00]
    })
    df_blue.to_parquet(blue_parquet, index=False)
    
    # 1. Baseline state
    print(f"\n[STEP 1] Baseline State: Active dataset = {engine.get_manifest()['active_dataset'].upper()}")
    
    # 2. Demo Valid GREEN Release
    print("\n[STEP 2] Generating Valid GREEN Candidate...")
    green_parquet = os.path.join(engine.green_dir, 'sales.parquet')
    df_green_valid = pd.DataFrame({
        'order_id': [201, 202],
        'order_date': ['2026-01-10', '2026-01-11'],
        'product': ['Green Desk', 'Green Chair'],
        'category': ['Furniture', 'Furniture'],
        'quantity': [2, 1],
        'unit_price': [300.00, 150.00],
        'region': ['West', 'South'],
        'revenue': [600.00, 150.00]
    })
    df_green_valid.to_parquet(green_parquet, index=False)
    
    success, msg = engine.deploy_green(green_parquet)
    print(f" -> Result: {msg}")
    print(f" -> Active Dataset is now: {engine.get_manifest()['active_dataset'].upper()}")
    
    # 3. Demo Rollback
    print("\n[STEP 3] Triggering Rollback to BLUE...")
    rb_success, rb_msg = engine.rollback()
    print(f" -> Result: {rb_msg}")
    print(f" -> Active Dataset is now: {engine.get_manifest()['active_dataset'].upper()}")

    # 4. Demo Invalid GREEN Release
    print("\n[STEP 4] Generating Broken GREEN Candidate (Invalid quantity = -5)...")
    df_green_broken = pd.DataFrame({
        'order_id': [301],
        'order_date': ['2026-01-15'],
        'product': ['Broken Item'],
        'category': ['Electronics'],
        'quantity': [-5],
        'unit_price': [10.00],
        'region': ['North'],
        'revenue': [-50.00]
    })
    df_green_broken.to_parquet(green_parquet, index=False)
    
    fail_success, fail_msg = engine.deploy_green(green_parquet)
    print(f" -> Result: {fail_msg}")
    print(f" -> Active Dataset remains: {engine.get_manifest()['active_dataset'].upper()}")
    print("=" * 65)

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    run_dataflip_demo(base_dir)
