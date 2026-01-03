from etl.extract import extract_sales_data
from etl.transform import transform_sales_data
from etl.load import load_sales_data


def run_etl():
    print("==> Starting ETL pipline ...")

    df_raw = extract_sales_data()
    print(f"==> Extracted {len(df_raw)} rows.")

    df_clean = transform_sales_data(df_raw)
    print(f"==> Transformed to {len(df_clean)} rows.")
    
    load_sales_data(df_clean)
    print("==> ETL pipeline completed successfully.")
