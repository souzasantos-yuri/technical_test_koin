import pandas as pd
from pathlib import Path

base_path = Path(__file__).parent.parent / "data"

def build_path(layer: str, domain: str, filename: str) -> Path:
    """
    Auxilia a criação dos paths de leitura e escrita.
    """    
    return base_path / layer / domain / filename

def save_csv(df: pd.DataFrame, path: Path) -> None:
    """
    Salva os modelos em CSV.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep=",", index=False)