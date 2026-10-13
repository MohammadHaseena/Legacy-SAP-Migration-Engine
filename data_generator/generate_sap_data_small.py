import pandas as pd
from faker import Faker
import random

fake = Faker()

customers = []

regions = ["North", "South", "East", "West"]
countries = ["India", "USA", "Germany", "UK"]

for customer_id in range(100001, 100501):
    customers.append({
        "KUNNR": customer_id,
        "NAME": fake.company(),
        "REGION": random.choice(regions),
        "COUNTRY": random.choice(countries)
    })

customer_df = pd.DataFrame(customers)

customer_df.to_csv(
    "datasets/sap_customer_master.csv",
    index=False
)

print("Customer Master Generated Successfully!")

sales_orders = []

currencies = ["INR", "USD", "EUR"]

for i in range(5000):
    date = fake.date_between(start_date="-2y", end_date="today")

    sales_orders.append({
        "SALES_DOC": random.randint(5000000000, 5999999999),
        "ITEM_NUM": random.choice(["000010", "000020", "000030"]),
        "KUNNR": random.randint(100001, 100500),
        "MATERIAL": None if random.random() < 0.03 else f"MAT-{random.randint(1000,9999)}",
        "NET_PRICE": round(random.uniform(-500,5000),2),
        "CURRENCY": random.choice(currencies),


        "CREATED_ON": (
            date.strftime("%Y-%m-%d")
            if random.random() < 0.5
            else date.strftime("%d/%m/%Y")
        )
    })

sales_df = pd.DataFrame(sales_orders)
duplicates = sales_df.sample(50, random_state=42)

sales_df = pd.concat([sales_df, duplicates], ignore_index=True)

sales_df.to_csv(
    "datasets/sap_sales_orders.csv",
    index=False
)

print("Sales Orders Generated Successfully!")
