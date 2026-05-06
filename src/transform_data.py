import pandas as pd
from pathlib import Path
import hashlib

schema = "customers"

def hash_value(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()

def clean_data(df: pd.DataFrame, schema) -> pd.DataFrame:
    #quality check here

    if schema == "customers":
        df["name"] = df["name"].str.title() #Capitaliza as primeiras letras
        df["phone"] = df["phone"].astype("str") #Remove a notação científica do telefone
        df["created_at"] = df["created_at"].astype("datetime64[s]") #Transforma para data
        df["city"] = df["city"].str.title() #Capitaliza as primeiras letras 
        df.fillna({ #Retira os NaN e troca por vazio
                "email": "", 
                "phone": "", 
                "city": "", 
                "state": "", 
                "created_at": "", 
                "status": ""
            }, 
            inplace=True)
    
    elif schema == "orders":
        xxxx
    
    return df

def anonymize_data(df: pd.DataFrame) -> pd.DataFrame:
    #quality check here
        df['email_hash'] = df['email'].apply(hash_value)
        df['phone_hash'] = df['phone'].apply(hash_value)
        df['name_hash'] = df['name'].apply(hash_value)
        df = df.drop(columns=['email', 'phone', 'name'])


def rename_columns(df: pd.Dataframe, schema: dict) -> pd.DataFrame:
    if schema == "customers":
        return df.rename({ "created_at": "ingestion_date" })
    elif schema == "orders":
        return df.rename({ "amount": "total_amount" })
    else:
        raise Exception(f"{schema} does not exists")
    
def data_transformation() -> pd.Dataframe:
    print("\n Initiating transformations...")
    df = clean_data(df, schema=schema)
    df = anonymize_data(df)
    df = rename_columns(df, schema=schema)
    return df