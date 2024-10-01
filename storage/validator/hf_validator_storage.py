import pandas as pd
import os


class HFValidationStorage:
    def __init__(self, storage_path):
        self.storage_path = storage_path
        self.file_path = os.path.join(storage_path, 'hf_validation.parquet')
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        if not os.path.exists(self.file_path):
            df = pd.DataFrame(columns=['hotkey', 'repo_name', 'block'])
            df.to_parquet(self.file_path, index=False)

    def get_validation_info(self, hotkey):
        df = pd.read_parquet(self.file_path)
        return df[df['hotkey'] == hotkey].to_dict('records')[0] if not df[df['hotkey'] == hotkey].empty else None

    def update_validation_info(self, hotkey, repo_name, block):
        df = pd.read_parquet(self.file_path)
        new_row = pd.DataFrame({'hotkey': [hotkey], 'repo_name': [repo_name], 'block': [block]})
        df = pd.concat([df[df['hotkey'] != hotkey], new_row], ignore_index=True)
        df.to_parquet(self.file_path, index=False)

    def get_all_validations(self):
        return pd.read_parquet(self.file_path)