import os
import json
import pandas as pd


class DataFlipEngine:
    """Schema-driven Blue/Green dataset deployment engine for DataFlip."""

    def __init__(self, base_dir: str, schema=None, dataset_filename: str = "data.parquet"):
        self.base_dir = base_dir
        self.schema = schema
        self.dataset_filename = dataset_filename
        self.data_dir = os.path.join(base_dir, 'data')
        self.blue_dir = os.path.join(self.data_dir, 'blue')
        self.green_dir = os.path.join(self.data_dir, 'green')
        self.raw_dir = os.path.join(self.data_dir, 'raw')
        self.manifest_path = os.path.join(self.data_dir, 'manifest.json')

    def init_environment(self) -> None:
        """Initialize directory structure and manifest state."""
        for d in [self.blue_dir, self.green_dir, self.raw_dir]:
            os.makedirs(d, exist_ok=True)
        if not os.path.exists(self.manifest_path):
            self._create_default_manifest()

    def _create_default_manifest(self) -> dict:
        default_manifest = {
            "active_dataset": "blue",
            "status": "active",
            "details": "Baseline BLUE dataset",
            "active_path": os.path.join(self.blue_dir, self.dataset_filename)
        }
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            json.dump(default_manifest, f, indent=2)
        return default_manifest

    def get_manifest(self) -> dict:
        """Read state from manifest."""
        if not os.path.exists(self.manifest_path):
            return self._create_default_manifest()
        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return self._create_default_manifest()

    def get_active_dataset_path(self) -> str:
        """Return the file path of currently active production dataset."""
        manifest = self.get_manifest()
        active_path = manifest.get('active_path')
        if not active_path or not os.path.exists(active_path):
            color = manifest.get('active_dataset', 'blue')
            active_path = os.path.join(self.data_dir, color, self.dataset_filename)
        return active_path

    def validate_dataset(self, parquet_path: str) -> tuple[bool, str]:
        """Validate candidate dataset structure and schema rules."""
        if not os.path.exists(parquet_path):
            return False, f"Validation Failed: File does not exist: {parquet_path}"

        try:
            df = pd.read_parquet(parquet_path)
        except Exception as e:
            return False, f"Validation Failed: File could not be read: {e}"

        if df.empty or len(df) == 0:
            return False, "Validation Failed: Dataset is empty (0 rows)."

        if len(df.columns) == 0:
            return False, "Validation Failed: Dataset has no columns."

        if self.schema is not None:
            if hasattr(self.schema, "validate"):
                try:
                    self.schema.validate(df)
                except Exception as e:
                    err_msg = str(e).splitlines()[0]
                    return False, f"Validation Failed: {err_msg}"
            elif isinstance(self.schema, dict):
                for col in self.schema.get("required_columns", []):
                    if col not in df.columns:
                        return False, f"Validation Failed: Missing required column {col}"
                for col in self.schema.get("not_null", []):
                    if col in df.columns and df[col].isnull().any():
                        return False, f"Validation Failed: Found null values in {col}"
                for col in self.schema.get("positive", []):
                    if col in df.columns and (df[col] <= 0).any():
                        return False, f"Validation Failed: {col} must be strictly > 0"
                for col in self.schema.get("non_negative", []):
                    if col in df.columns and (df[col] < 0).any():
                        return False, f"Validation Failed: {col} must be >= 0"

        return True, "Validation Passed: Dataset is valid"

    def deploy_green(self, green_parquet_path: str) -> tuple[bool, str]:
        """Validate candidate GREEN dataset; activate if PASS, retain BLUE if FAIL."""
        is_valid, message = self.validate_dataset(green_parquet_path)
        if not is_valid:
            return False, f"RELEASE REJECTED: {message}"

        manifest = {
            "active_dataset": "green",
            "status": "active",
            "details": message,
            "active_path": os.path.abspath(green_parquet_path)
        }
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)

        return True, f"RELEASE SUCCESS: GREEN activated. {message}"

    def rollback(self) -> tuple[bool, str]:
        """Rollback active dataset pointer back to BLUE."""
        manifest = self.get_manifest()
        current_active = manifest.get('active_dataset', 'blue')
        if current_active == 'blue':
            return False, "Rollback Not Needed: BLUE is already active."

        blue_parquet = os.path.join(self.blue_dir, self.dataset_filename)
        if not os.path.exists(blue_parquet):
            blue_files = [f for f in os.listdir(self.blue_dir) if f.endswith('.parquet')] if os.path.exists(self.blue_dir) else []
            if blue_files:
                blue_parquet = os.path.join(self.blue_dir, blue_files[0])
            else:
                return False, "Rollback Failed: Known-good BLUE dataset missing."

        manifest = {
            "active_dataset": "blue",
            "status": "rolled_back",
            "details": "Production dataset successfully reverted to BLUE.",
            "active_path": os.path.abspath(blue_parquet)
        }
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2)

        return True, "ROLLBACK SUCCESS: Production dataset successfully reverted to BLUE."
