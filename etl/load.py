from config.settings import OUTPUT_PATH
from utils.logger import get_logger

logger = get_logger(__name__)


def load_sales_data(df):
    """
    Load cleaned data to CSV
    
    :param df: Description
    """
    logger.info("Loading data to CSV")
    try:
        df.to_csv(
            OUTPUT_PATH,
            index=False
        )
    except:
        logger.warning("Failed to load data to CSV")
    else:
        logger.info("Load data to CSV successful")