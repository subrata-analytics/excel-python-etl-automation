import pandas as pd
from logging import Logger
from typing import Any, Dict, List


def parse_date(
        df: pd.DataFrame,
        date_parsing_cfg: Dict[str, Any],
        logger: Logger,
    ) -> pd.DataFrame:
    """
    Parse date columns to datetime.
    """
    logger.info("Applying date parsing.")

    df = df.copy()

    columns: List[str] = date_parsing_cfg.get("columns", [])
    log_enabled = date_parsing_cfg.get("log", False)

    for col in columns:
        if col not in df.columns:
            continue

        nan_before = int(df[col].isna().sum())
        df[col] = pd.to_datetime(df[col], errors="coerce")
        nan_after = int(df[col].isna().sum())

        if log_enabled:
            logger.info(
                f"Column '{col}' NaNs before: {nan_before}, after: {nan_after}."
            )
    
    return df
