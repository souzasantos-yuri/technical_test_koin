import pandas as pd
from pathlib import Path
import hashlib

schema = "customers"

def hash_value(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()

def anonymize_data(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    #quality check here

    if schema == "customers":
        df['email_hash'] = df['email'].apply(hash_value)
        df['phone_hash'] = df['phone'].apply(hash_value)
        df['name_hash'] = df['name'].apply(hash_value)
        df = df.drop(columns=['email', 'phone', 'name'])

    return df

def transform_customers_data(df: pd.DataFrame) -> pd.DataFrame:
    df["name"] = df["name"].str.title() #Capitaliza as primeiras letras
    df["phone"] = df["phone"].astype("str") #Remove a notação científica do telefone
    df["created_at"] = df["created_at"].astype("datetime64[s]") #Transforma para data
    df["city"] = df["city"].str.title() #Capitaliza as primeiras letras 
    df.fillna({ #Retira os NaN e troca por outro valor
            "email": "unknown", 
            "phone": "unknown", 
            "city": "unknown", 
            "state": "unknown", 
            "created_at": "unknown", 
            "status": "unknown",
            "name": "unknown"
        }, 
        inplace=True)
    return df 

def transform_orders_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(keep=False)
    df["amount"] = df["amount"].str.replace(',', ".")
    df["amount"] = pd.to_numeric(df["amount"], errors='coerce')
    df['amount_is_invalid'] = df['amount'] < 0
    df.fillna({
                "order_date": "unknown", 
                "amount": 0, 
                "payment_method": "unknown", 
                "status": "unknown"
            }, 
            inplace=True)
    return df 


def clean_data(df: pd.DataFrame, schema) -> pd.DataFrame:
    #quality check here

    if schema == "customers":
        transform_customers_data(df)
    
    elif schema == "orders":
        transform_orders_data(df)
    
    return df

def rename_columns(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    if schema == "customers":
        return df.rename(columns={ "created_at": "ingestion_date" })
    elif schema == "orders":
        return df.rename(columns={ "amount": "total_amount" })
    else:
        raise Exception(f"{schema} does not exists")
    
def data_transformation(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    df = clean_data(df, schema=schema)
    df = anonymize_data(df, schema=schema)
    df = rename_columns(df, schema=schema)
    return df