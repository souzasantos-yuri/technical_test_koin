import pandas as pd
from pathlib import Path
from utils import build_path
from utils import save_csv

base_path = Path(__file__).parent.parent / "data"

customer_input_path = build_path("bronze", "customers", "bronze_customers.csv")
orders_input_path = build_path("bronze", "orders", "bronze_orders.csv")

customer_output_path = build_path("silver", "customers", "dim_customers.csv")
order_output_path = build_path("silver", "orders", "dim_orders.csv")
transaction_output_path = build_path("silver", "transactions", "fact_transactions.csv")

cdf = pd.read_csv(customer_input_path)
odf = pd.read_csv(orders_input_path)

dim_customers = cdf.drop(
    columns=["email_hash", "phone_hash", "name_hash", "date"], 
    errors='ignore'
)

dim_orders = odf.drop(
    columns=["customer_id", "total_amount"], 
    errors="ignore"
)

fact_transactions = odf.drop(
    columns=["payment_method", "status", "amount_is_invalid", "order_status"],
    errors='ignore'
)

fact_transactions = fact_transactions.rename(
    columns={"order_date": "date"}
)

save_csv(dim_customers, customer_output_path)
save_csv(dim_orders, order_output_path)
save_csv(fact_transactions, transaction_output_path)