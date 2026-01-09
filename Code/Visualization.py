import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

def run_visualization():
    print("🔹 Visualization started")

    engine = create_engine("postgresql+psycopg://postgres:572257@localhost:5432/PyProject_DWH")

    fact_sales = pd.read_sql("SELECT * FROM fact_sales", engine)
    dim_date = pd.read_sql("SELECT * FROM dim_date", engine)
    dim_product = pd.read_sql("SELECT * FROM dim_product", engine)
    dim_customer = pd.read_sql("SELECT * FROM dim_customer", engine)
    dim_region = pd.read_sql("SELECT * FROM dim_region", engine)
    dim_staff = pd.read_sql("SELECT * FROM dim_staff", engine)
    # Merging dataframes to create a comprehensive dataset

    sales_time = (
        fact_sales
        .merge(dim_date, left_on="order_date_id", right_on="date_id")
        .groupby([ "month"], as_index=False)
        .agg(total_sales=("total_sales", "sum"))
    )

    sales_time["month"] =  sales_time["month"]

    plt.figure()
    plt.plot(sales_time["month"], sales_time["total_sales"])
    plt.xticks(rotation=45)
    plt.title("Sales Over Months")
    plt.xlabel("Time")
    plt.ylabel("Total Sales")
    plt.tight_layout()
    plt.show()


    sales_time = (
        fact_sales
        .merge(dim_date, left_on="order_date_id", right_on="date_id")
        .groupby([ "year"], as_index=False)
        .agg(total_sales=("total_sales", "sum"))
    )

    sales_time["year"] =  sales_time["year"]

    plt.figure()
    plt.plot(sales_time["year"], sales_time["total_sales"])
    plt.xticks(rotation=45)
    plt.title("Sales Over Years")
    plt.xlabel("Time")
    plt.ylabel("Total Sales")
    plt.tight_layout()
    plt.show()

    sales_time = (
        fact_sales
        .merge(dim_date, left_on="order_date_id", right_on="date_id")
        .groupby([ "day_name"], as_index=False)
        .agg(total_sales=("total_sales", "sum"))
    )

    sales_time["day_name"] =  sales_time["day_name"]
    plt.figure()
    plt.plot(sales_time["day_name"], sales_time["total_sales"])
    plt.xticks(rotation=45)
    plt.title("Sales Over Days")
    plt.xlabel("Time")
    plt.ylabel("Total Sales")
    plt.tight_layout()
    plt.show()


    top_products = (
        fact_sales
        .merge(dim_product, on="product_id")
        .groupby("product_name", as_index=False)
        .agg(total_sales=("total_sales", "sum"))
        .sort_values("total_sales", ascending=False)
        .head(10)
    )

    plt.figure(figsize=(12, 8))  # حجم الشكل (width, height)

    bars = plt.bar(
        top_products['product_name'], 
        top_products['total_sales'], 
        color='skyblue', 
        edgecolor='black'
    )

    plt.title("Top 10 Best Selling Products", fontsize=18, fontweight='bold')
    plt.xlabel("Product", fontsize=14)
    plt.ylabel("Total Sales", fontsize=14)
    plt.xticks(rotation=45, ha='right', fontsize=12)  # تدوير أسماء المنتجات
    plt.yticks(fontsize=12)

    # إضافة قيم المبيعات فوق الأعمدة
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.05*yval, f'{int(yval):,}', 
                ha='center', va='bottom', fontsize=10)

    plt.tight_layout()  # لتجنب تداخل العناصر
    plt.show()

    region_sales = (
        fact_sales
        .merge(dim_region, left_on="store_region_id", right_on="region_id")
        .groupby("state", as_index=False)
        .agg(total_sales=("total_sales", "sum"))
        .sort_values("total_sales", ascending=False)
    )

    plt.figure(figsize=(8,8))
    plt.pie(
        region_sales["total_sales"],
        labels=region_sales["state"],
        autopct='%1.1f%%',
        startangle=140,
        shadow=False
    )
    plt.title("Sales by Store State")
    plt.axis('equal')  # دائره مثاليه
    plt.show()
    staff_sales = (
        fact_sales
        .merge(dim_staff, left_on="staff_id", right_on="staff_id")
        .groupby("first_name", as_index=False)
        .agg(total_sales=("total_sales", "sum"))
        .sort_values("total_sales", ascending=False)
    )

    plt.figure()
    plt.bar(staff_sales["first_name"], staff_sales["total_sales"])
    plt.title("Sales by Staff")
    plt.xlabel("Staff")
    plt.ylabel("Total Sales")
    plt.tight_layout()
    plt.show()

    print("✅ Visualization finished")