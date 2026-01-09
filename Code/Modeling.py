import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine


def run_modeling():
    print("🔹 Modeling started")
    # =========================
    #Database Connection

    engine = create_engine("postgresql+psycopg://postgres:572257@localhost:5432/PyProject_DWH")

    #-------------
    #Paths
    #-------------
    Base_Directory = Path(__file__).resolve().parents[1]
    Cleaned_Directory = Base_Directory / "2_Staging" / "staging_1"
    Transformed_Directory = Base_Directory / "2_Staging" / "staging_2"
    DWH_Directory = Base_Directory / "3_Modeling" / "data_warehouse"
    Cleaned_Directory.mkdir(parents=True, exist_ok=True)
    Transformed_Directory.mkdir(parents=True, exist_ok=True)  
    DWH_Directory.mkdir(parents=True, exist_ok=True)  
    #----------------------------------------
    #Functions
    #----------------------------------------
    date_sources = {
        "orders": {
            "df": pd.read_csv(Transformed_Directory / "Transformed_orders.csv"),
            "cols": ["order_date", "required_date", "shipped_date"]
        }
    }
        
    def build_dim_date(date_sources):
        all_dates = []

        for source in date_sources.values():
            df = source["df"]
            for col in source["cols"]:
                dates = pd.to_datetime(df[col], errors="coerce")
                all_dates.append(dates)
                
        all_dates = pd.concat(all_dates, ignore_index=True).dropna()
        dim_date = pd.DataFrame()
        dim_date["date_id"] = all_dates.dt.strftime("%Y%m%d").astype(int) 
        dim_date["date"] = all_dates.dt.date
        dim_date["day_name"] = all_dates.dt.day_name()
        dim_date["month"] = all_dates.dt.month_name()
        dim_date["year"] = all_dates.dt.year
        dim_date["quarter"] = all_dates.dt.quarter
        dim_date = dim_date.drop_duplicates().sort_values("date").reset_index(drop=True)
        return dim_date

    def build_dim_product():
        products = pd.read_csv(Cleaned_Directory / "cleaned_products.csv")
        category = pd.read_csv(Cleaned_Directory / "cleaned_categories.csv")
        brands = pd.read_csv(Cleaned_Directory / "cleaned_brands.csv")
        products = ( products.merge(category[["category_id","category_name"]], on='category_id', how='left').merge(brands[["brand_id","brand_name"]], on='brand_id', how='left'))
        dim_product = products[['product_id', 'product_name', 'category_name', 'brand_name','model_year' ,'list_price']].drop_duplicates().reset_index(drop=True)
        return dim_product


    def build_dim_customer():
        customers = pd.read_csv(Transformed_Directory / "transformed_customers.csv")
        customers = customers.merge(dim_region, left_on=['city', 'state', 'zip_code'],
                right_on=['city', 'state', 'zip_code'], how='left')
        dim_customer = customers[['customer_id',"region_id",'first_name', 'last_name', 'phone', 'email',"local_flag"]].drop_duplicates().reset_index(drop=True)
        return dim_customer     # region id unique for each city,state,zip_code combination


    def build_dim_store():
        stores = pd.read_csv(Cleaned_Directory / "cleaned_stores.csv")
        stores = stores.merge(dim_region, left_on=['city', 'state', 'zip_code'],
                right_on=['city', 'state', 'zip_code'], how='left')
        dim_store = stores[['store_id', "region_id", 'store_name', 'phone', 'email']].drop_duplicates().reset_index(drop=True)
        return dim_store



    def build_dim_staff():
        staff = pd.read_csv(Cleaned_Directory / "cleaned_staffs.csv")
        dim_staff = staff[['staff_id', 'first_name', 'last_name', 'email', 'phone',"active" ]].drop_duplicates().reset_index(drop=True)
        return dim_staff


    def build_dim_region():
        customers = pd.read_csv(Transformed_Directory / "transformed_customers.csv").drop_duplicates().reset_index(drop=True)
        store = pd.read_csv(Cleaned_Directory / "cleaned_stores.csv").drop_duplicates().reset_index(drop=True)
        dim_region = pd.concat([customers[[ 'city', 'state', 'zip_code']], store[[ 'city', 'state', 'zip_code']]],ignore_index=True).drop_duplicates().reset_index(drop=True)
        dim_region['region_id'] = dim_region.index + 1
        dim_region = dim_region[['region_id', 'city', 'state', 'zip_code']]
        return dim_region
    #----------------------------------------
    #Build Fact Table
    #----------------------------------------
    def build_fact_sales():
        orders = pd.read_csv(Transformed_Directory / "transformed_orders.csv")
        order_items = pd.read_csv(Transformed_Directory / "transformed_order_items.csv")

        orders = orders.merge(order_items, on='order_id', how='inner')
        
        orders["order_date"] = pd.to_datetime(
        orders["order_date"],
        errors="coerce"
        ).dt.date

        orders["required_date"] = pd.to_datetime(
            orders["required_date"],
            errors="coerce"
        ).dt.date

        orders["shipped_date"] = pd.to_datetime(
            orders["shipped_date"],
            errors="coerce"
        ).dt.date
    

            
        orders = orders.merge(
            dim_date[['date_id', 'date']],
            left_on='order_date',
            right_on='date',
            how='inner'
        ).rename(columns={'date_id': 'order_date_id'}).drop(columns=['date'])
   

        orders = orders.merge(
            dim_date[['date_id', 'date']],
            left_on='required_date',
            right_on='date',
            how='left'
        ).rename(columns={'date_id': 'required_date_id'}).drop(columns=['date'])

        orders = orders.merge(
            dim_date[['date_id', 'date']],
            left_on='shipped_date',
            right_on='date',
            how='left'
        ).rename(columns={'date_id': 'shipped_date_id'}).drop(columns=['date'])
   
        orders = orders.merge(dim_product, on='product_id', how='inner')
       
        
        orders = orders.merge(
            dim_customer,
            on='customer_id',
            how='inner'
        ).rename(columns={'region_id': 'customer_region_id'})
        

        
        orders = orders.merge(
            dim_store,
            on='store_id',
            how='inner'
        ).rename(columns={'region_id': 'store_region_id'})


        
        orders = orders.merge(
            dim_staff[['staff_id', 'first_name', 'last_name', 'email', 'phone', 'active']],
            on='staff_id',  
            how='inner'
        )
      
    
        fact_sales = orders[[
            'order_id',
            'product_id',
            'customer_id',
            'store_id',
            'customer_region_id',
            'store_region_id',
            'staff_id',
            'order_date_id',
            'required_date_id',
            'shipped_date_id',
            'discount',
            'delivery_time_days',
            'late_delivery_days',
            'late_flag',
            'status_priority',
            'quantity',
            'list_price_local',
        ]].copy()
   
        fact_sales.reset_index(drop=True, inplace=True)
        fact_sales.insert(0, 'sales_key', fact_sales.index + 1)

        fact_sales['total_sales'] = (
            fact_sales['quantity']
            * fact_sales['list_price_local']
            * (1 - fact_sales['discount'])
        )

        return fact_sales
    #----------------------------------------



    #Build and Save Dimension Tables
    #Date Dimension
    dim_date = build_dim_date(date_sources)
    dim_date.to_csv(DWH_Directory / "dim_date.csv", index=False)
    dim_date.to_sql('dim_date', engine, if_exists='replace', index=False)


    #Region Dimension

    dim_region = build_dim_region()
    dim_region.to_csv(DWH_Directory / "dim_region.csv", index=False)
    dim_region.to_sql('dim_region', engine, if_exists='replace', index=False)

    #Product Dimension

    dim_product = build_dim_product()
    dim_product.to_csv(DWH_Directory / "dim_product.csv", index=False)
    dim_product.to_sql('dim_product', engine, if_exists='replace', index=False)

    #Customer Dimension

    dim_customer = build_dim_customer()
    dim_customer.to_csv(DWH_Directory / "dim_customer.csv", index=False)
    dim_customer.to_sql('dim_customer', engine, if_exists='replace', index=False)

    #Store Dimension

    dim_store = build_dim_store()
    dim_store.to_csv(DWH_Directory / "dim_store.csv", index=False)
    dim_store.to_sql('dim_store', engine, if_exists='replace', index=False)

    #Staff Dimension

    dim_staff = build_dim_staff()
    dim_staff.to_csv(DWH_Directory / "dim_staff.csv", index=False)
    dim_staff.to_sql('dim_staff', engine, if_exists='replace', index=False)

    #----------------------------------------

    #Build and Save Fact Table
    fact_sales = build_fact_sales()
    fact_sales.to_csv(DWH_Directory / "fact_sales.csv", index=False)
    fact_sales.to_sql('fact_sales', engine, if_exists='replace', index=False)  
    #print(fact_sales)

    print("✅ Modeling finished")
