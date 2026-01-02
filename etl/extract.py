import pandas as pd
from config.settings import RAW_DATA_PATH


def extract_sales_data() -> pd.DataFrame:
    """
    Extract raw data from Excel file
    
    :return: extracted raw data
    :rtype: DataFrame
    """
    df = pd.read_excel(
        RAW_DATA_PATH, 
        engine="openpyxl"
    )
    return df