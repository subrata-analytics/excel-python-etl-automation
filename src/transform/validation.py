import pandas as pd
from logging import Logger
from typing import Any, Dict, List


def run_validation(
        df: pd.DataFrame,
        validation_cfg: Dict[str, Any],
        logger: Logger,
    ) -> None:
    """
    Run validation rules. Log anomalies, do not drop rows.
    """
    logger.info("Running validation rules (log-only).")

    # no_double_spaces_in_store
    if (validation_cfg.get("no_double_spaces_in_store", False) 
        and "store" in df.columns):
        mask = df["store"].astype("string").str.contains(r"  ", regex=False)
        count = int(mask.sum())
        if count > 0:
            logger.warning(
                "Validation: %d rows have double spaces in 'store'.", count
            )

    # region_not_null
    if (validation_cfg.get("region_not_null", False) 
        and "region" in df.columns):
        mask = df["region"].isna()
        count = int(mask.sum())
        if count > 0:
            logger.warning("Validation: %d rows have null 'region'.", count)

    # sale_date_not_null
    if (validation_cfg.get("sale_date_not_null", False) 
        and "sale_date" in df.columns):
        mask = df["sale_date"].isna()
        count = int(mask.sum())
        if count > 0:
            logger.warning("Validation: %d rows have null 'sale_date'.", count)

    # region_uppercase
    if (validation_cfg.get("region_uppercase", False) 
        and "region" in df.columns):
        series = df["region"].dropna().astype("string")
        mask = series != series.str.upper()
        count = int(mask.sum())
        if count > 0:
            logger.warning(
                "Validation: %d non-null 'region' values are not uppercase.", 
                count
            )

    # category_uppercase
    if (validation_cfg.get("category_uppercase", False) 
        and "category" in df.columns):
        series = df["category"].dropna().astype("string")
        mask = series != series.str.upper()
        count = int(mask.sum())
        if count > 0:
            logger.warning(
                "Validation: %d non-null 'category' values are not uppercase.", 
                count
            )