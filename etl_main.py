from src.extract_data import extract_data
from src.transform_data import data_transformation
from src.load_data import load_data
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

schemas = ["customers", "orders"]

def pipeline(schema: str):
    try:
        logging.info(f"First stage: Extracting <{schema}>")
        df = extract_data(schema=schema)
        logging.info(f"Rows extracted: {len(df)}")

        logging.info(f"Second stage: Transforming <{schema}>")
        before = len(df)
        df = data_transformation(df, schema=schema)
        after = len(df)
        logging.info(f"Rows removed: {before - after}")

        logging.info(f"Last stage: Loading <{schema}> \n")
        load_data(df, schema=schema)

        logging.info(f"Pipeline for <{schema}> fully completed!")
        logging.info(f"Saved rows: {after}\n")

    except Exception as e:
        logging.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

for item in schemas:
    pipeline(item)