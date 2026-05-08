import pandas as pd
from pathlib import Path
import hashlib

def hash_value(value: str) -> str:
    if pd.isna(value):
        return None
    
    return hashlib.sha256(value.encode()).hexdigest()

def anonymize_data(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """
    Aplica anonimização de dados sensíveis via SHA256.
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
    """
    Transforma os dados de cliente e limpa os campos nulos.
    """
    df["phone"] = df["phone"].astype(str)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["city"] = df["city"].str.title()
    df.fillna({
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
    """
    Transforma os dados de transações, deleta colunas desnecessárias.
    """
    df = df.drop_duplicates()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
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
    """
    Faz o processo de limpeza dos dados com esquema dinâmico.
    """
    if schema == "customers":
       return transform_customers_data(df)
    
    elif schema == "orders":
       return transform_orders_data(df)

def rename_columns(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """
    Renomeia as colunas.
    """    
    if schema == "customers":
        return df.rename(columns={ 
            "created_at": "date",
            "status": "customer_status"
            })
    
    elif schema == "orders":
        return df.rename(columns={ 
            "amount": "total_amount" ,
            "order_date": "date",
            "status": "order_status"
            })
    else:
        raise Exception(f"{schema} does not exists")
    
def data_transformation(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """
    Finaliza o processo de transformação fazendo um chain das funções criadas acima.
    """    
    df = clean_data(df=df, schema=schema)
    df = anonymize_data(df=df, schema=schema)
    df = rename_columns(df=df, schema=schema)
    return df