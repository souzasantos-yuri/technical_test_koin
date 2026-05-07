import pandas as pd
from pathlib import Path

schema = "customers"

def extract_data(schema: str) -> pd.DataFrame:
    input_path = Path(__file__).parent.parent / "data" / f"raw/{schema}" / f"raw_{schema}.csv"
    return pd.read_csv(input_path)