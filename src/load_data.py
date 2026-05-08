import pandas as pd
from pathlib import Path

def load_data(df: pd.DataFrame, schema: str) -> None:
    """
    Saves the new transformed data in a CSV file.
    """
    output_path = Path(__file__).parent.parent / "data" / f"bronze/{schema}" / f"bronze_{schema}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, sep=",", index=False, encoding="utf-8")