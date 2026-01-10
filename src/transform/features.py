import pandas as pd
from logging import Logger
from typing import Any, Dict, List


def add_feature(
        df: pd.DataFrame,
        feature_eng_cfg: Dict[str, Any],
        logger: Logger,
    ) -> pd.DataFrame:
    """
    Apply feature engineering: compute total_sales, derive date parts.
    """
    logger.info("Applying feature engineering.")

    df = df.copy()

    compute_total_sales = feature_eng_cfg.get("compute_total_sales", False)
    derive_date_parts = feature_eng_cfg.get("derive_date_parts", False)
    date_parts: List[str] = feature_eng_cfg.get("date_parts", [])
    log_enabled = feature_eng_cfg.get("log", False)

    if compute_total_sales:
        if "total_sales" not in df.columns and {"quantity", "unit_price"}.issubset(df.columns):
            df["total_sales"] = df["quantity"] * df["unit_price"]
            if log_enabled:
                logger.info("Computed 'total_sales' = quantity * unit_price.")

    if derive_date_parts and "sale_date" in df.columns:
        sale_date: pd.Series[pd.Timestamp]  = df["sale_date"]
        if "sale_year" in date_parts:
            df["sale_year"] = sale_date.dt.year
        if "sale_month" in date_parts:
            df["sale_month"] = sale_date.dt.month
        if "sale_quarter" in date_parts:
            df["sale_quarter"] = sale_date.dt.quarter
        if "weekday" in date_parts:
            df["weekday"] = sale_date.dt.day_name()

        if log_enabled:
            logger.info(f"Derived date parts: {", ".join(date_parts)}")

    return df

