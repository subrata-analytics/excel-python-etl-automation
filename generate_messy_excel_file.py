# Script for generating messy excel 
# file with fake sales data
import os
import numpy as np
import pandas as pd
import random
from faker import Faker
from datetime import date, datetime, timedelta


fake = Faker()

# Configuration
ROWS = 50_000
SHEET_NAME = "SalesData"
WORKBOOK_NAME = "sales_transactions_2024.xlsx"
OUTPUT_FILE_PATH = os.path.join(os.getcwd(), "data", "raw", WORKBOOK_NAME)

# Reference Data
stores = ["store A", "Store B", "Store C", "Store D", "Store E"]
store_variants = [
    "Store a", "STORE A", "STORE-A", "Store-A",
    "Store b", "STORE B", "STORE-B", "Store-B",
    "Store c", "STORE C", "STORE-C", "Store-C",
    "Store d", "STORE D", "STORE-D", "Store-D",
    "Store e", "STORE E", "STORE-E", "Store-E"
]

regions = regions = ["North", "South", "East", "West"]
region_variants = [
    "north", "NORTH", "Nort",
    "south", "SOUTH", "Sout",
    "east",  "EAST",  "Est", 
    "west",  "WEST",  "Wes",  
    "", None
]

products = [
    "Laptop Pro 15", "Laptop Pro15", "Laptop Pro - 15", "LTP Pro 15",
    "Smartphone X", "Smart phone X", "Smartfone X",
    "Tablet Plus", "Tablet+"
]

categories = ["Electronics", "Accessories"]
category_variants = [
    "Electronic", "electronics", "electronic",
    "Accessory", "accessories", "accessories", ""
]

notes_pool = [
    "manual override", "promo applied", "check later",
    "urgent", "???", "", None
]

# Helper functions
def random_sale_date():
    if random.random() < 0.03:
        return random.choice(["", "N/A", None])

    date_range = dict(
        start_date = date(2024, 1, 1), 
        end_date = date(2024, 12, 31)
    )
    sale_date = fake.date_between(**date_range)
    return random.choice([
        sale_date.strftime("%Y-%m-%d"),
        sale_date.strftime("%m/%d/%Y"),
        sale_date.strftime("%d/%b/%Y"),
        sale_date.strftime("%Y/%m/%d"),
    ])

def random_price():
    if random.random() < 0.02:
        return random.choice(["FREE", "$1299.99", "1,299.99"])
    
    price = round(random.uniform(10, 2000), 2)
    return random.choice([price, f"${price}", f"${price:,}"])

def random_quantity():
    if random.random() < 0.01:
        return random.choice([0, -1, 99])
    
    return random.randint(1, 10)

def random_total_sales(price, quantity):
    if random.random() < 0.4:
        return round(random.uniform(20, 5000), 2)
    try:
        return round(
            (float(
                    str(price)
                    .replace("$", "")
                    .replace(",", "")
                )
            ) * quantity, 2
        )

    except:
        return None

def random_last_updated():
    if random.random() < 0.1:
        return None
    
    date_range = dict(
        start_date = date(2024, 1, 1), 
    end_date = date(2024, 12, 31)
    )
    dt = fake.date_between(**date_range)
    return random.choice([
        dt.strftime("%Y-%m-%d %H:%M"),
        dt.strftime("%m/%d/%Y %I:%M %p")
    ])

def generate_data():
    # Generate data
    data = []
    for _ in range(ROWS):
        unit_price = random_price()
        quantity = random_quantity()
        row = {
            "Sale Date": random_sale_date(),
            "Store": random.choice(stores + store_variants),
            "Region": random.choice(regions + region_variants),
            "Product Name": random.choice(products),
            "Category": random.choice(categories + category_variants),
            "Unit Price": unit_price,
            "Quanty": quantity,
            "Total Sales": random_total_sales(unit_price, quantity),
            "Notes": random.choice(notes_pool),
            "Last Update": random_last_updated()
        }
        data.append(row)

    df = pd.DataFrame(data)

    # Inject duplicate rows (~1.5%)
    dup_count = int(ROWS * 0.015)
    df = pd.concat([df, df.sample(dup_count)], ignore_index=True)

    # Write Excel
    with pd.ExcelWriter(OUTPUT_FILE_PATH, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=SHEET_NAME)

    return df

if __name__ == "__main__":
    df = generate_data()
    print(f"Generated messy Excel file: {WORKBOOK_NAME}")
    print(f"Rows written: {len(df)}")
    