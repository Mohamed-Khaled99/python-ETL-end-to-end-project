import pandas as pd
from pathlib import Path
from datetime import datetime

# =========================
# Paths
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]  
CONSOLIDATED_DIR = BASE_DIR / '1_Extraction' / 'consolidated'
STAGING_1_DIR = BASE_DIR / '2_Staging' / 'staging_1'
STAGING_2_DIR = BASE_DIR / '2_Staging' / 'staging_2'

STAGING_1_DIR.mkdir(parents=True, exist_ok=True)
STAGING_2_DIR.mkdir(parents=True, exist_ok=True)

## exchange_rates_df = pd.read_csv(CONSOLIDATED_DIR / 'exchange_rates.csv')
## print(exchange_rates_df.duplicated())

# =========================
orders_df = pd.read_csv(CONSOLIDATED_DIR / 'orders.csv')
print(orders_df.head())
orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
orders_df['required_date'] = pd.to_datetime(orders_df['required_date'], errors='coerce')
orders_df['shipped_date'] = pd.to_datetime(orders_df['shipped_date'], errors='coerce')
print(orders_df.head())