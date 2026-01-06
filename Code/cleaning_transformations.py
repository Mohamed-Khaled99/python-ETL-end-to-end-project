import pandas as pd
from pathlib import Path
from datetime import datetime

# =========================
#------------- Paths -------------
Base_Directory = Path(__file__).resolve().parents[1]
Consolidated_Directory = Base_Directory / "1_Extraction" / "consolidated"
Cleaned_Directory = Base_Directory / "2_Staging" / "staging_1"
Transformed_Directory = Base_Directory / "2_Staging" / "staging_2"

Cleaned_Directory.mkdir(parents=True, exist_ok=True)
Transformed_Directory.mkdir(parents=True, exist_ok=True)

#----------------------------------------
# Functions
#----------------------------------------
# 1) Remove Nulls
def remove_nulls(df, essential_columns):
    return df.dropna(subset=essential_columns)

# 2) Remove Duplicates
def remove_duplicates(df, essential_columns):
    return df.drop_duplicates(subset=essential_columns)

def validate_positive(df, column):
    return df[df[column] > 0]

def validate_dates(df, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df[df[date_col] <= pd.Timestamp.today()]

# =========================
# Phase 2: Data Quality
# =========================
#--------------orders------------
orders = pd.read_csv(Consolidated_Directory / "orders.csv")
orders = remove_nulls(orders, ["order_id", "customer_id", "order_date", "required_date"])
orders = remove_duplicates(orders, "order_id")
orders = validate_dates(orders, "order_date")
orders = validate_dates(orders, "required_date")
orders = validate_dates(orders, "shipped_date")
orders = validate_dates(orders, "Extraction_Date")
orders = validate_dates(orders, "extracted_at")
orders.to_csv(Cleaned_Directory / "cleaned_orders.csv", index=False)

#--------------order_items------------
order_items = pd.read_csv(Consolidated_Directory / "order_items.csv")
order_items = remove_nulls(order_items, ["item_id", "order_id", "product_id", "quantity", "list_price"])
order_items = remove_duplicates(order_items, ["item_id", "order_id", "product_id", "quantity", "list_price"])
order_items = validate_positive(order_items, "list_price")
order_items = validate_dates(order_items, "Extraction_Date")
order_items = validate_dates(order_items, "extracted_at")
order_items.to_csv(Cleaned_Directory / "cleaned_order_items.csv", index=False)

#--------------brands------------
brands = pd.read_csv(Consolidated_Directory / "brands.csv")
brands = remove_nulls(brands, ["brand_id", "brand_name"])
brands = remove_duplicates(brands, "brand_id")
brands = validate_dates(brands, "extracted_at")
brands.to_csv(Cleaned_Directory / "cleaned_brands.csv", index=False)

#--------------categories------------
categories = pd.read_csv(Consolidated_Directory / "categories.csv")
categories = remove_nulls(categories, ["category_id", "category_name"])
categories = remove_duplicates(categories, "category_id")
categories = validate_dates(categories, "extracted_at")
categories.to_csv(Cleaned_Directory / "cleaned_categories.csv", index=False)

#--------------customers------------
customers = pd.read_csv(Consolidated_Directory / "customers.csv")
customers.fillna("Not Available", inplace=True)
customers = remove_duplicates(customers, "customer_id")
customers = validate_dates(customers, "extracted_at")
customers.to_csv(Cleaned_Directory / "cleaned_customers.csv", index=False)

#--------------products------------
products = pd.read_csv(Consolidated_Directory / "products.csv")
products = remove_nulls(products, ["product_id", "product_name", "list_price"])
products = remove_duplicates(products, "product_id")
products = validate_positive(products, "list_price")
products = validate_dates(products, "extracted_at")
products.to_csv(Cleaned_Directory / "cleaned_products.csv", index=False)

#--------------staffs------------
staffs = pd.read_csv(Consolidated_Directory / "staffs.csv")
staffs["last_name"] = staffs["last_name"].fillna(staffs["email"].str.split(".").str[1].str.split("@").str[0])
staffs["email"] = staffs["email"].fillna(staffs["first_name"].str.lower() + "." + staffs["last_name"].str.lower() + "@bikes.shop")
staffs[["phone","store_id","manager_id"]] = staffs[["phone","store_id","manager_id"]].fillna("Not Available")
staffs = remove_duplicates(staffs, "staff_id")
staffs = validate_dates(staffs, "extracted_at")
staffs.to_csv(Cleaned_Directory / "cleaned_staffs.csv", index=False)

#--------------stores------------
stores = pd.read_csv(Consolidated_Directory / "stores.csv")
stores["email"] = stores["email"].fillna(stores["store_name"].str.rsplit(pat=" ", n=1).str[0].str.replace(" ","").str.lower() + "@bikes.shop")
stores["zip_code"] = stores["zip_code"].fillna("Not Available")
stores = validate_dates(stores, "extracted_at")
stores.to_csv(Cleaned_Directory / "cleaned_stores.csv", index=False)

#--------------stocks------------
stocks = pd.read_csv(Consolidated_Directory / "stocks.csv")
stocks.replace({"quantity": {0: stocks["quantity"].mean()}}, inplace=True)
stocks = validate_positive(stocks, "quantity")
stocks = validate_dates(stocks, "extracted_at")
stocks.to_csv(Cleaned_Directory / "cleaned_stocks.csv", index=False)

#--------------exchange_rates------------
exchange_rates = pd.read_csv(Consolidated_Directory / "exchange_rates.csv")
exchange_rates = validate_dates(exchange_rates, "extracted_at")
exchange_rates = validate_positive(exchange_rates, "rate")
exchange_rates.to_csv(Cleaned_Directory / "cleaned_exchange_rates.csv", index=False)

# -------------Transformations------------
# Convert list_price to local currency in order_items table
Transformed_order_items = pd.read_csv(Cleaned_Directory / "cleaned_order_items.csv")
Transformed_exchange_rates = pd.read_csv(Cleaned_Directory / "cleaned_exchange_rates.csv")
Transformed_order_items["Currency"] = "USD"
Transformed_order_items["Target_Currency"] = "EGP"
Transformed_exchange_rates = Transformed_exchange_rates.rename(columns={"currency":"Target_Currency","rate":"Target_Rate"})
Transformed_order_items = Transformed_order_items.merge(Transformed_exchange_rates[["Target_Currency","Target_Rate"]], on="Target_Currency", how="left")
Transformed_order_items["list_price_local"] = Transformed_order_items["list_price"] * Transformed_order_items["Target_Rate"]
Transformed_order_items.to_csv(Transformed_Directory / "Transformed_order_items.csv", index=False)

# Delivery metrics calculation
Transformed_orders = pd.read_csv(Cleaned_Directory / "cleaned_orders.csv")
Transformed_orders["delivery_time_days"] = (pd.to_datetime(Transformed_orders["shipped_date"]) - pd.to_datetime(Transformed_orders["order_date"])).dt.days
Transformed_orders["late_delivery_days"] = (pd.to_datetime(Transformed_orders["shipped_date"]) - pd.to_datetime(Transformed_orders["required_date"])).dt.days
Transformed_orders["late_delivery_days"] = Transformed_orders["late_delivery_days"].apply(lambda x: x if x > 0 else 0)
Transformed_orders["late_flag"] = Transformed_orders["late_delivery_days"].apply(lambda x: "Late" if x > 0 else "On Time")
Transformed_orders.to_csv(Transformed_Directory / "Transformed_orders.csv", index=False)

# Order status lookup
order_status_lkp = pd.DataFrame({
    'order_status': [1, 2, 3, 4, 5],
    'status_priority': ['Shipped', 'Cancelled', 'On Hold', 'Released', 'Disputed']
})
Transformed_orders = Transformed_orders.merge(order_status_lkp, on='order_status', how='left')
order_status_lkp.to_csv(Transformed_Directory / "order_status_lookup.csv", index=False)
Transformed_orders.to_csv(Transformed_Directory / "Transformed_orders.csv", index=False)

# Customer local flag
Transformed_customers = pd.read_csv(Cleaned_Directory / "cleaned_customers.csv")
Transformed_customers["local_flag"] = Transformed_customers["city"].apply(lambda x: "Local" if x in stores["city"].values else "Non-Local")
Transformed_customers.to_csv(Transformed_Directory / "Transformed_customers.csv", index=False)
