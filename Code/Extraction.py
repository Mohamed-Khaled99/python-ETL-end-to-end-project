import pandas as pd
import requests
import mysql.connector
from mysql.connector import Error
import json
from pathlib import Path
from datetime import datetime

# =========================
# Paths
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / '1_Extraction'     
API_DIR = RAW_DIR / 'api_data'
DB_DIR = RAW_DIR / 'db_data'
LAKE_DIR = RAW_DIR / 'data_lake'
CONSOLIDATED_DIR = RAW_DIR / 'consolidated'

# إنشاء المجلدات لو مش موجودة
for p in [API_DIR, DB_DIR, LAKE_DIR, CONSOLIDATED_DIR]:
    p.mkdir(parents=True, exist_ok=True)

DATA_LAKE_SOURCE = BASE_DIR / 'Datalake Source'

# =========================
# 1) Extract from API
# =========================
def extract_api(consolidated_dir):
    App_id = "3da15a39cf8d4527aa5a8f302d6ff936"
    url = f"https://openexchangerates.org/api/latest.json?app_id={App_id}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Metadata
    extracted_at = datetime.now().isoformat()
    data['extracted_at'] = extracted_at
    data['source'] = 'API'

    # Save raw JSON
    api_json_file = API_DIR / 'exchange_rates.json'
    with open(api_json_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    # Transform to DataFrame & consolidate
    rates_df = pd.DataFrame(list(data['rates'].items()), columns=['currency', 'rate'])
    rates_df['extracted_at'] = extracted_at
    rates_df['source'] = 'API'

    csv_file = consolidated_dir / 'exchange_rates.csv'
    rates_df.to_csv(csv_file, index=False)
    print('API data extracted and consolidated ->', csv_file)

# =========================
# 2) Extract from Database
# =========================
def extract_mysql_table(table_name):
    try:
        # 1) Connect
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Asd@123456789",
            database="pyproject_orders"
        )

        # 2) Read data
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, conn)

        # 3) Add metadata
        df["extracted_at"] = datetime.now().isoformat()
        df["source"] = "MYSQL"

        # 4) Save to CSV
        csv_file = CONSOLIDATED_DIR / f"{table_name}.csv"
        df.to_csv(csv_file, index=False)

        print(f"✅ Table '{table_name}' extracted -> {csv_file}")

    except Error as err:
        print(f"❌ MySQL Error for table '{table_name}': {err}")

    except Exception as err:
        print(f"❌ General Error for table '{table_name}': {err}")

    finally:
        # 5) Close safely
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("🔒 MySQL connection closed")
# =========================
# 3) Extract from Data Lake
# =========================
def extract_data_lake():
    if not DATA_LAKE_SOURCE.exists():
        print('No external data lake folder, skipping Data Lake extraction')
        return

    for file in DATA_LAKE_SOURCE.glob('*.csv'):
        df = pd.read_csv(file)
        df['extracted_at'] = datetime.now().isoformat()
        df['source'] = 'DataLake'

        out_file = CONSOLIDATED_DIR / file.name
        df.to_csv(out_file, index=False)
        print(f'Data Lake file {file.name} extracted and consolidated -> {out_file}')

# =========================
# Main
# =========================
if __name__ == '__main__':
    print('--- EXTRACT + CONSOLIDATION PHASE STARTED ---')

    # 1) API
    try:
        extract_api(CONSOLIDATED_DIR)
    except Exception as e:
        print('API extraction failed:', e)

    # 2) MySQL
    for table in ["orders", "order_items"]:
        extract_mysql_table(table)

    # 3) Data Lake
    extract_data_lake()

    print('--- EXTRACT + CONSOLIDATION PHASE FINISHED ---')
