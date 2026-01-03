import pandas as pd
import re

def clean_text_safe(value):
    if pd.isna(value):
        return None

    value = str(value)
    value = value.strip()
    value = re.sub(r"\s+", " ", value)      # collapse whitespace
    value = re.sub(r"[^\w\s\-&/]", "", value)  # remove junk chars
    return value

def clean_numeric_data(value):
    if pd.isna(value):
        return None
    
    value = (str(value)
             .strip()
             .replace("$", "")
             .replace(",", "")
    )
    return value

def clean_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply column specific normalization
    
    :param df: raw data
    :type df: pd.DataFrame
    :return: normalized data 
    :rtype: DataFrame
    """

    # Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # normalize store
    df["store"] = (
        df["store"]
        .apply(clean_text_safe)
        .str.title()
    )

    # region (force canonical)
    region_map = { 
        "east": "EAST",
        "est": "EAST",
        "nort": "NORTH",
        "north": "NORTH",
        "sout": "SOUTH",
        "south": "SOUTH",
        "wes": "WEST", 
        "west": "WEST" 
    }
    df["region"] = (
        df["region"]
        .apply(clean_text_safe)
        .str.lower()
        .map(region_map)
    )

    # product name
    df["product_name"] = (
        df["product_name"]
        .apply(clean_text_safe)
        .str.title()
    )

    # categories
    category_map = { 
        "accessories": "ACCESSORIES",
        "electronics": "ELECTRONICS",
        "accessory": "ACCESSORIES",
        "electronic": "ELECTRONICS" 
    }
    df["category"] = (
        df["category"]
        .apply(clean_text_safe)
        .str.lower()
        .map(category_map)
    )

    df["notes"] = (
        df["notes"]
        .apply(clean_text_safe)
    )

    return df

def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the raw sales data  
    
    :param df: raw data to be transformed
    :type df: pd.DataFrame
    :return: transformed data
    :rtype: DataFrame
    """
    df = df.copy()

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # clean dimensions
    df = clean_dimensions(df)

    # Normalize numeric colums
    numeric_cols = ["quantity", "unit_price", "total_sales"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(
                (
                    df[col].apply(clean_numeric_data)
                ),
                errors="coerce"
            )

    # Feature engineering
    df["total_sales"] = df["quantity"] * df["unit_price"]
    
    # Handle missing values
    df = df[df["quantity"] > 0]
    df["unit_price"] = df["unit_price"].fillna(0)


    # Convert data types
    df["sale_date"] = pd.to_datetime(
        df["sale_date"],
        errors="coerce"
    )

    df["sale_year"] = df["sale_date"].dt.year
    df["sale_month"] = df["sale_date"].dt.month
    df["sale_quarter"] = df["sale_date"].dt.quarter
    df["weekday"] = df["sale_date"].dt.day_name()

    # Filter invalid records
    df = df[df["total_sales"] >= 0]

    return df

