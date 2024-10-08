import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq


class HFValidationStorage:
    def __init__(self, storage_path):
        self.file_path = storage_path
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        if not os.path.exists(self.file_path):
            self._create_empty_dataframe()

    def _create_empty_dataframe(self):
        df = pd.DataFrame(columns=['hotkey', 'repo_name', 'block'])
        self._safe_write_parquet(df)

    def _safe_write_parquet(self, df):
        temp_file = f"{self.file_path}.temp"
        try:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, temp_file)
            os.replace(temp_file, self.file_path)
        except Exception as e:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise e

    def _safe_read_parquet(self):
        try:
            return pd.read_parquet(self.file_path)
        except Exception as e:
            print(f"Error reading Parquet file: {e}")
            print("Attempting to recover data...")
            return self._recover_data()

    def _recover_data(self):
        try:
            table = pq.read_table(self.file_path)
            return table.to_pandas()
        except Exception as e:
            print(f"Recovery failed: {e}")
            print("Creating a new empty dataframe.")
            return pd.DataFrame(columns=['hotkey', 'repo_name', 'block'])

    def get_validation_info(self, hotkey):
        df = self._safe_read_parquet()
        matching_rows = df[df['hotkey'] == hotkey]
        return matching_rows.to_dict('records')[0] if not matching_rows.empty else None

    def update_validation_info(self, hotkey, repo_name, block):
        df = self._safe_read_parquet()
        new_row = pd.DataFrame({'hotkey': [hotkey], 'repo_name': [repo_name], 'block': [block]})
        df = pd.concat([df[df['hotkey'] != hotkey], new_row], ignore_index=True)
        self._safe_write_parquet(df)

    def get_all_validations(self):
        return self._safe_read_parquet()