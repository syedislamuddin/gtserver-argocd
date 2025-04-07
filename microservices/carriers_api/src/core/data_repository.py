import os
import pandas as pd


class DataRepository:
    @staticmethod
    def read_csv(file_path: str, **kwargs) -> pd.DataFrame:
        """Read data from CSV file"""
        return pd.read_csv(file_path, **kwargs)
    
    @staticmethod
    def write_csv(data: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Write data to CSV file"""
        data.to_csv(file_path, **kwargs)
    
    @staticmethod
    def remove_file(file_path: str) -> None:
        """Remove a file"""
        if os.path.exists(file_path):
            os.remove(file_path)
