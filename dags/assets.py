# include/assets.py
from airflow.sdk import Asset

# Define your pipeline assets here
ENRICHED_ORDERS = Asset("enriched-orders")
PROCESSED_ORDERS = Asset("processed-orders")