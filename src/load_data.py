import pandas as pd
from pathlib import Path

schema = "customers"

def save_data(df: pd.DataFrame, schema: str):
    output_path = Path(__file__).parent.parent / "data" / f"bronze/{schema}" / f"bronze_{schema}.csv"
    df.to_csv(output_path, sep=",", index=False, encoding="utf-8")
    return True