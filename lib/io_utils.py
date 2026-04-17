import pandas as pd
from pathlib import Path

def load_data(file_path):
    """Load data from various formats."""
    path = Path(file_path)
    if path.suffix == '.csv':
        return pd.read_csv(path)
    elif path.suffix in ['.xlsx', '.xls']:
        return pd.read_excel(path)
    elif path.suffix == '.parquet':
        return pd.read_parquet(path)
    else:
        raise ValueError("Unsupported file format")

def save_processed_data(df, filename):
    """Save processed data to processed folder."""
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)
    file_path = processed_dir / f"{filename}.parquet"
    df.to_parquet(file_path)
    return str(file_path)

def save_output(data, filename, format='csv'):
    """Save output data."""
    output_dir = Path("data/output")
    output_dir.mkdir(parents=True, exist_ok=True)
    if format == 'csv':
        file_path = output_dir / f"{filename}.csv"
        data.to_csv(file_path, index=False)
    elif format == 'excel':
        file_path = output_dir / f"{filename}.xlsx"
        data.to_excel(file_path, index=False)
    return str(file_path)