import pandas as pd
from logging import Logger
from typing import Any, Dict


def clean_text(
        df: pd.DataFrame,
        text_cleaning_cfg: Dict[str, Any],
        logger: Logger
        ) -> pd.DataFrame:
    """
    Apply text cleaning operations: collapse whitespace, 
    remove special characters, strip. Applies to all object columns.
    """
    logger.info("Applying text cleaning.")

    df = df.copy()

    text_cols = text_cleaning_cfg.get("columns", [])
    text_cleaning_cfg = text_cleaning_cfg.get("cleaning", {})
    log_enabled = text_cleaning_cfg.get("log", False)

    collapse_ws = text_cleaning_cfg.get("collapse_whitespace", False)
    remove_special = text_cleaning_cfg.get("remove_special_characters", False)
    strip = text_cleaning_cfg.get("strip", False)

    for col in text_cols:
        if col in df.columns:
            series = df[col].astype("string")

            if strip:
                series = series.str.strip()

            if collapse_ws:
                series = series.str.replace(r"\s+", " ", regex=True)

            if remove_special:
                # Keep letters, numbers, basic punctuation, and spaces
                series = series.str.replace(r"[^\w\s\.\-/,]", "", regex=True)

            df[col] = series
    
        if log_enabled:
            logger.info("Applied text cleaning in {col}.")
    
    return df


def standardize_text(
        df: pd.DataFrame,
        text_std_cfg: Dict[str, Any],
        logger: Logger,
    ) -> pd.DataFrame:
    """
    Apply text standardization based on config keys (e.g., title, strip).
    Only for explicitly configured columns.
    """
    logger.info("Applying text standardization.")

    df = df.copy()

    log_enabled = text_std_cfg.get("log", False)

    # We don't want to treat 'log' as a column rule
    column_rules = {k: v for k, v in text_std_cfg.items() if k != "log"}

    for col, rule in column_rules.items():
        if col not in df.columns:
            continue

        series = df[col].astype("string")

        match rule:
            case "title":
                series = series.str.title()
            case "upper":
                series = series.str.upper()
            case "lower":
                series = series.str.lower()
            case "strip":
                series = series.str.strip()

        df[col] = series

        if log_enabled:
            logger.debug(f"Standardized column {col} with rule {rule}.")
        
    return df
