import pandas as pd
from pathlib import Path
import hashlib

def hash_value(value: str) -> str:
    if pd.isna(value):
        return None
    
    return hashlib.sha256(value.encode()).hexdigest()

def anonymize_data(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """
    Applies SHA256 anonymization to sensitive customer fields.
    """

    required_columns = ['email', 'phone', 'name']
    missing_columns = [c for c in required_columns if c not in df.columns]

    if schema == "orders":
        return df

    elif schema == "customers" and missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")
    else:
        df['email_hash'] = df['email'].apply(hash_value)
        df['phone_hash'] = df['phone'].apply(hash_value)
        df['name_hash'] = df['name'].apply(hash_value)
        df = df.drop(columns=['email', 'phone', 'name'])

    return df

def transform_customers_data(df: pd.DataFrame) -> pd.DataFrame:
    df["phone"] = df["phone"].astype(str)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce") #Transforma para data hora
    df["city"] = df["city"].str.title()
    df.fillna({ #Retira os NaN e troca por outro valor
        "city": "unknown", 
        "state": "unknown", 
        "created_at": "unknown", 
        "status": "unknown",
        "phone": "unknown",
        "name": "unknown",
        "email": "unknown"
    }, inplace=True)
    
    return df 

def transform_orders_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()
    df["amount"] = df["amount"].str.replace(',', ".")
    df["amount"] = pd.to_numeric(df["amount"], errors='coerce')
    df['amount_is_invalid'] = df['amount'] < 0 
    #Negative revenue will be kept because it can be analyzed as per why it happens
    df.fillna({
            "order_date": "unknown", 
            "amount": 0, 
            "payment_method": "unknown", 
            "status": "unknown"
        }, inplace=True)
    return df 


def clean_data(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    if schema == "customers":
       return transform_customers_data(df)
    
    elif schema == "orders":
       return transform_orders_data(df)

def rename_columns(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    if schema == "customers":
        return df.rename(columns={ "created_at": "ingestion_date" })
    elif schema == "orders":
        return df.rename(columns={ "amount": "total_amount" })
    else:
        raise Exception(f"{schema} does not exists")
    
def data_transformation(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    df = clean_data(df=df, schema=schema)
    df = anonymize_data(df=df, schema=schema)
    df = rename_columns(df=df, schema=schema)
    return df