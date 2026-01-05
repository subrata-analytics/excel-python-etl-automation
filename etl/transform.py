import re
import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

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
    df = df.copy()

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

    # notes
    df["notes"] = (
        df["notes"]
        .apply(clean_text_safe)
    )

    return df

def enforce_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    df = df.copy()

    for col, dtype in schema.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype, errors="ignore")
    
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

    logger.info("Cleaning and transforming data ...")

    # Remove duplicates
    logger.info("Removing duplicates")
    df.drop_duplicates(inplace=True)

    # clean dimensions
    logger.info("Cleaning dimensions")
    df = clean_dimensions(df)

    # Normalize numeric colums
    logger.info("Normalizing numeric columns")
    numeric_cols = ["quantity", "unit_price", "total_sales"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(
                (
                    df[col].apply(clean_numeric_data)
                ),
                errors="coerce"
            )
    
    # Drop rows for NULL values in certain columns
    logger.info("Drop rows for NULL values")
    df = df.dropna(
        subset=[
            "sale_date",
            "category",
            "product_name",
            "region",
            "quantity",
            "unit_price"
        ],
        # how="all"
    )

    # Feature engineering
    logger.info("Calculating total sales")
    df["total_sales"] = df["quantity"] * df["unit_price"]
    
    # Handle missing values
    df = df[df["quantity"] > 0]
    df["unit_price"] = df["unit_price"].fillna(0)

    # Convert data types
    logger.info("Convert date data types")
    sale_date = pd.to_datetime(
        df["sale_date"],
        errors="coerce",
        format='mixed',
        dayfirst=True   
    )
    df["sale_date"] = sale_date

    # Derive important date components from sale_date
    logger.info("Deriving important date components")
    df["sale_year"] = sale_date.dt.year
    df["sale_month"] = sale_date.dt.month
    df["sale_quarter"] = sale_date.dt.quarter
    df["weekday"] = sale_date.dt.day_name()

    # Filter invalid records
    logger.info("Filter invalid total sales")
    df = df[df["total_sales"] >= 0]

    # Schema enforement
    logger.info("Enforcing schema")
    schema = {
        "sale_date": "datetime64[ns]",
        "quantity": "float",
        "unit_price": "float",
        "total_sales": "float"
    }
    df = enforce_schema(df, schema)

    # validation
    logger.info("Validating the cleaned data")
    assert df["store"].str.contains(r"\s{2,}").sum() == 0
    assert df["region"].isna().sum() == 0
    assert df["sale_date"].isna().sum() == 0
    assert df["region"].str.isupper().all()
    assert df["category"].str.isupper().all()
    logger.info("Validation successful!")

    return df

