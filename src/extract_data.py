import pandas as pd
from pathlib import Path

def extract_data(schema: str) -> pd.DataFrame:
    """
    Reads the CSV data.
    """
    input_path = Path(__file__).parent.parent / "data" / f"raw/{schema}" / f"raw_{schema}.csv"
    return pd.read_csv(input_path)