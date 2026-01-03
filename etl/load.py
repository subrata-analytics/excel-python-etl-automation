from config.settings import OUTPUT_PATH


def load_sales_data(df):
    """
    Load cleaned data to CSV
    
    :param df: Description
    """
    df.to_csv(
        OUTPUT_PATH,
        index=False
    )