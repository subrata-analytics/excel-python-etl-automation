import pandas as pd
from yaml import safe_load


def get_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        config = safe_load(f)
    return config

def get_raw_data(excel_path: str, sheetname: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheetname, engine="openpyxl")
    return df