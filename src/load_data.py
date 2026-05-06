import pandas as pd
from pathlib import Path

schema = "customers"
output_path = Path(__file__).parent.parent / "data" / f"bronze/{schema}" / f"bronze_{schema}.csv"

def save_data(df: pd.DataFrame, output_path: str):
    return df.to_csv(output_path, sep=",", index=False, encoding="utf-8")