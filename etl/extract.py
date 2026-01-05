import pandas as pd
from config.settings import RAW_DATA_PATH
from utils.logger import get_logger

logger = get_logger(__name__)

def extract_sales_data() -> pd.DataFrame:
    """
    Extract raw data from Excel file
    
    :return: extracted raw data
    :rtype: DataFrame
    """
    logger.info("Extracting data ...")

    df = pd.read_excel(
        RAW_DATA_PATH, 
        engine="openpyxl"
    )
    logger.info(f"Extracted {len(df)} rows")

    return df