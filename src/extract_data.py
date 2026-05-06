import pandas as pd
from pathlib import Path

schema = "customers"
input_path = Path(__file__).parent.parent / "data" / f"raw/{schema}" / f"raw_{schema}.csv"

def extract_data(input_path: str) -> pd.DataFrame:
    df = pd.read_csv(input_path)