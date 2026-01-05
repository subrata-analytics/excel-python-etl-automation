from etl.extract import extract_sales_data
from etl.transform import transform_sales_data
from etl.load import load_sales_data
from utils.logger import get_logger

logger = get_logger(__name__)


def run_etl():
    logger.info("Starting ETL pipline ...")

    # Extracting
    df_raw = extract_sales_data()

    # Cleaning and transforming
    df_clean = transform_sales_data(df_raw)

    # Loading
    load_sales_data(df_clean)
    logger.info("ETL pipeline completed successfully.")
