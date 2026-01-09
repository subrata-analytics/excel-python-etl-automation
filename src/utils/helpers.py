# =============================================================================
# Helper functions
# =============================================================================
import json
from logging import Logger
import pandas as pd
from typing import Any, Dict, List
from src.utils.lineage import LineageWriter, log_lineage


def save_profile_report(report: dict, json_path: str):
    with open(json_path, "w") as f:
        json.dump(report, f, indent=4)


def rename_columns(
        df: pd.DataFrame,
        column_mapping: Dict[str, str],
        lineage_writer: LineageWriter,
        logger: Logger
        ) -> None:
    """
    Rename columns according to column_mapping and log column-rename lineage.

    We log column-level lineage using a special row_id = -1, where:
    - old_value = old column name
    - new_value = new column name
    - column = "column_name"
    - rule = "column_rename"
    """
    logger.info("Renaming columns.")

    for old_col, new_col in column_mapping.items():
        if old_col in df.columns and old_col != new_col:
            # Log column-level lineage (row_id = -1 meaning header)
            log_lineage(
                lineage_writer=lineage_writer,
                row_id=-1,
                column="column_name",
                old_value=old_col,
                new_value=new_col,
                rule="column_rename",
            )

    df.rename(columns=column_mapping, inplace=True)


def drop_duplicate_rows(
        df: pd.DataFrame,
        duplicates_cfg: Dict[str, Any],
        lineage_writer: LineageWriter,
        logger: Logger
        ) -> None:
    """
    Drop duplicate rows and log drop-row lineage for removed rows.
    """
    log_enabled = duplicates_cfg.get("log", False)
    logger.info("Dropping duplicate rows.")

    # Use all columns except internal __row_id__ for duplicate detection
    cols_for_dupes = [col for col in df.columns if col != "__row_id__"]
    dup_mask = df.duplicated(subset=cols_for_dupes, keep="first")

    if dup_mask.any():
        dropped = df.loc[dup_mask, ["__row_id__"]].copy()
        df = df[~dup_mask]

        if log_enabled:
            for _, row in dropped.iterrows():
                row_id = int(row["__row_id__"])
                log_lineage(
                    lineage_writer=lineage_writer,
                    row_id=row_id,
                    column="__all__",
                    old_value=None,
                    new_value=None,
                    rule="drop_duplicates",
                )

        logger.info(f"Dropped {int(dup_mask.sum())} duplicate rows.", )
    else:
        logger.info("No duplicate rows found.")


def clean_text(
        df: pd.DataFrame,
        text_cleaning_cfg: Dict[str, Any],
        logger: Logger
        ) -> None:
    """
    Apply text cleaning operations: collapse whitespace, 
    remove special characters, strip. Applies to all object columns.
    """
    logger.info("Applying text cleaning.")

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
            logger.info("Applied text cleaning.")


def standardize_text(
        df: pd.DataFrame,
        text_std_cfg: Dict[str, Any],
        logger: Logger,
    ) -> None:
    """
    Apply text standardization based on config keys (e.g., title, strip).
    Only for explicitly configured columns.
    """
    logger.info("Applying text standardization.")

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
