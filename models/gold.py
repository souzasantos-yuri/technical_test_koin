import pandas as pd
from pathlib import Path
from utils import build_path
from utils import save_csv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

base_path = Path(__file__).parent.parent / "data"

customer_input_path = build_path("silver", "customers", "dim_customers.csv")
order_input_path = build_path("silver", "orders", "dim_orders.csv")
transction_input_path = build_path("silver", "transactions", "fact_transactions.csv")

datamart_output_path = build_path("gold", "datamart", "datamart_transactions.csv")

dim_customer = pd.read_csv(customer_input_path)
dim_order = pd.read_csv(order_input_path)
fact_transactions = pd.read_csv(transction_input_path)

datamart = (
    fact_transactions
    .merge(dim_order, on=['order_id', "date"], how='inner')
    .merge(dim_customer, on=["customer_id"], how='left')
    
)

logging.info(f"Saving in Gold <datamart>")
save_csv(datamart, datamart_output_path)
logging.info(f"Saved rows: {len(datamart)}")