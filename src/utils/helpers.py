# =============================================================================
# Helper functions
# =============================================================================
import json
import numpy as np
import pandas as pd
from logging import Logger
from typing import Any, Dict, List, SupportsInt, cast
from src.utils.lineage import LineageWriter, log_lineage


def save_profile_report(report: dict, json_path: str):
    with open(json_path, "w") as f:
        json.dump(report, f, indent=4)


def rename_columns(
        df: pd.DataFrame,
        column_mapping: Dict[str, str],
        lineage_writer: LineageWriter,
        logger: Logger
        ) -> pd.DataFrame:
    """
    Rename columns according to column_mapping and log column-rename lineage.

    We log column-level lineage using a special row_id = -1, where:
    - old_value = old column name
    - new_value = new column name
    - column = "column_name"
    - rule = "column_rename"
    """
    logger.info("Renaming columns.")

    df = df.copy()

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

    return df


def drop_duplicate_rows(
        df: pd.DataFrame,
        duplicates_cfg: Dict[str, Any],
        lineage_writer: LineageWriter,
        logger: Logger
        ) -> pd.DataFrame:
    """
    Drop duplicate rows and log for removed rows.
    """
    log_enabled = duplicates_cfg.get("log", False)
    logger.info("Dropping duplicate rows.")

    df = df.copy()

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
                    column="all",
                    old_value=None,
                    new_value="N/A",
                    rule="drop_duplicates",
                )

        logger.info(f"Dropped {int(dup_mask.sum())} duplicate rows.", )
    else:
        logger.info("No duplicate rows found.")
    
    return df


def drop_empty_rows(
        df: pd.DataFrame, 
        empty_rows_cfg: Dict[str, Any],
        lineage_writer: LineageWriter, 
        logger: Logger
        ) -> pd.DataFrame:
    """
    Drop empty rows and log for removed rows.
    """
    log_enabled = empty_rows_cfg.get("log", False)
    logger.info("Dropping empty rows.")

    df = df.copy()

    # Use all columns except internal __row_id__ for empty detection
    cols_for_empty = [col for col in df.columns if col != "__row_id__"]
    empty_mask = df[cols_for_empty].isna().all(axis=1)

    if empty_mask.any():
        dropped = df.loc[empty_mask, ["__row_id__"]].copy()
        df = df[~empty_mask]
        if log_enabled:
            for  _, row in dropped.iterrows():
                row_id = int(row["__row_id__"])
                log_lineage(
                    lineage_writer=lineage_writer,
                    row_id=row_id,
                    column="all",
                    old_value="",
                    new_value="N/A",
                    rule="drop_empty",
                )

            logger.info(f"Dropped {int(empty_mask.sum())} empty rows.")

    return df


def drop_rows_missing_required(
        df: pd.DataFrame,
        drop_missing_cfg: Dict[str, Any],
        lineage_writer: LineageWriter,
        logger: Logger,
    ) -> pd.DataFrame:
    """
    Drop rows missing required columns, log drop-row lineage.
    """
    required_columns: List[str] = drop_missing_cfg.get("required_columns", [])
    log_enabled = drop_missing_cfg.get("log", False)

    df = df.copy()

    if not required_columns:
        return df

    logger.info(
        "Dropping rows with missing required columns: %s",
        ", ".join(required_columns),
    )

    missing_mask = df[required_columns].isna().any(axis=1)

    if missing_mask.any():
        dropped = df.loc[missing_mask, ["__row_id__"]].copy()
        # df = df[~missing_mask]
        # another way of doing the above
        df.drop(index=dropped.index, inplace=True)


        if log_enabled:
            for _, row in dropped.iterrows():
                row_id = int(row["__row_id__"])
                log_lineage(
                    lineage_writer=lineage_writer,
                    row_id=row_id,
                    column=",".join(required_columns),
                    old_value="",
                    new_value="N/A",
                    rule="drop_missing_required",
                )

        logger.info("Dropped %d rows due to missing required fields.", int(missing_mask.sum()))
    else:
        logger.info("No rows with missing required fields.")
    
    return df



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


def normalize_with_reference_data(
        df: pd.DataFrame,
        ref_cfg: Dict[str, Any],
        lineage_writer: LineageWriter,
        logger: Logger,
    ) -> pd.DataFrame:
    """
    Normalize region and category using reference maps.
    Log lineage when values change.
    """

    df = df.copy()

    enabled = ref_cfg.get("enabled", False)
    if not enabled:
        logger.info("Reference normalization disabled.")
        return df

    region_map = ref_cfg.get("region_map", {})
    category_map = ref_cfg.get("category_map", {})

    logger.info("Applying reference-based normalization for region and category.")

    # Normalize region
    if "region" in df.columns:
        original = df["region"].copy()
        cleaned = (
            df["region"]
            .astype("string")
            .str.strip()
            .str.lower()
            .map(region_map)
            .fillna(df["region"])  # keep original if no match
        )
        df["region"] = cleaned

        # Log lineage
        for idx in df.index:
            old = original.loc[idx]
            new = cleaned.loc[idx]
            if old != new:
                row_id = cast(SupportsInt, df.at[idx, "__row_id__"])
                log_lineage(
                    lineage_writer=lineage_writer,
                    row_id=int(row_id),
                    column="region",
                    old_value=old,
                    new_value=new,
                    rule="reference_region_normalization",
                )

    # Normalize category
    if "category" in df.columns:
        original = df["category"].copy()
        cleaned = (
            df["category"]
            .astype("string")
            .str.strip()
            .str.lower()
            .map(category_map)
            .fillna(df["category"])
        )
        df["category"] = cleaned

        # Log lineage
        for idx in df.index:
            old = original.loc[idx]
            new = cleaned.loc[idx]
            if old != new:
                row_id = cast(SupportsInt, df.loc[idx, "__row_id__"])
                log_lineage(
                    lineage_writer=lineage_writer,
                    row_id=int(row_id),
                    column="category",
                    old_value=old,
                    new_value=new,
                    rule="reference_category_normalization",
                )

    logger.info("Applied reference-based normalization.")

    return df


def clean_numeric_values(
        df: pd.DataFrame,
        numeric_cleaning_cfg: Dict[str, Any],
        lineage_writer: LineageWriter,
        logger: Logger,
    ) -> pd.DataFrame:
    """
    Clean numeric and currency columns and log numeric-cleaning lineage
    when values change.
    """
    logger.info("Applying numeric cleaning.")

    df = df.copy()

    currency_columns: List[str] = numeric_cleaning_cfg.get("currency_columns", [])
    currency_symbols: List[str] = numeric_cleaning_cfg.get("currency_symbols", [])
    numeric_columns: List[str] = numeric_cleaning_cfg.get("numeric_columns", [])
    log_enabled = numeric_cleaning_cfg.get("log", False)

    # Clean currency columns: strip symbols and commas
    for col in numeric_columns:
        if col not in df.columns:
            continue

        original_values = df[col].copy()
        series = df[col].astype("string")

        if col in currency_columns:
            for sym in currency_symbols:
                series = series.str.replace(sym, "", regex=False)
        
        # Also handle standard thousands separator in all numeric columns
        series = series.str.replace(",", "", regex=False)

        # Attempt conversion to float; invalid becomes NaN
        cleaned = pd.to_numeric(series, errors="coerce")
        df[col] = cleaned

        if log_enabled:
            _log_numeric_changes_for_column(
                df=df,
                original_series=original_values,
                cleaned_series=cleaned,
                column=col,
                lineage_writer=lineage_writer,
                rule="numeric_cleaning_currency",
            )
            logger.info("Applied numeric cleaning.")
    
    return df

def _log_numeric_changes_for_column(
        df: pd.DataFrame,
        original_series: pd.Series,
        cleaned_series: pd.Series,
        column: str,
        lineage_writer: LineageWriter,
        rule: str,
    ) -> None:
    """
    For a given column, log lineage for rows where the value actually changed.
    """
    # Align indices
    original_series = original_series.reindex(df.index)
    cleaned_series = cleaned_series.reindex(df.index)

    changed_mask = ~(
        (original_series.isna() & cleaned_series.isna())
        | (original_series == cleaned_series)
    )

    changed_indices = df.index[changed_mask]

    for idx in changed_indices:
        row = df.loc[idx]
        row_id = int(row["__row_id__"])

        old_value = original_series.loc[idx]
        new_value = cleaned_series.loc[idx]

        log_lineage(
            lineage_writer=lineage_writer,
            row_id=row_id,
            column=column,
            old_value=old_value,
            new_value=new_value,
            rule=rule,
        )


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


def filters_with_lineage(
        df: pd.DataFrame,
        filters_cfg: Dict[str, Any],
        lineage_writer: LineageWriter,
        logger: Logger,
    ) -> pd.DataFrame:
    """
    Apply filtering rules and log drop-row lineage for removed rows.
    """
    logger.info("Applying filtering rules.")

    df = df.copy()

    quantity_gt_zero = filters_cfg.get("quantity_greater_than_zero", False)
    total_sales_non_negative = filters_cfg.get("total_sales_non_negative", False)
    log_enabled = filters_cfg.get("log", False)

    if not (quantity_gt_zero or total_sales_non_negative):
        return df

    keep_mask = pd.Series(True, index=df.index)

    if quantity_gt_zero and "quantity" in df.columns:
        keep_mask &= df["quantity"] > 0

    if total_sales_non_negative and "total_sales" in df.columns:
        keep_mask &= df["total_sales"] >= 0

    dropped_mask = ~keep_mask

    if dropped_mask.any():
        dropped = df.loc[dropped_mask, ["__row_id__"]].copy()
        df.drop(index=dropped.index, inplace=True)

        if log_enabled:
            for _, row in dropped.iterrows():
                row_id = int(row["__row_id__"])
                log_lineage(
                    lineage_writer=lineage_writer,
                    row_id=row_id,
                    column="quantity, total_sales",
                    old_value="zero or negative numbers",
                    new_value="N/A",
                    rule="filters",
                )
        
        logger.info(
            f"Applied filters. Dropped {int(dropped_mask.sum())} rows; \
                remaining {len(df)} rows.")
    else:
        logger.info("Filters applied. No rows dropped. \
                    Total rows: %d.", len(df))
    
    return df


def enforce_schema(
    df: pd.DataFrame,
    schema_cfg: Dict[str, Any],
    logger: Logger,
) -> pd.DataFrame:
    """
    Enforce dtypes as specified in schema config.
    """
    logger.info("Enforcing schema.")

    df = df.copy()

    # 'log' is a control flag, not a column
    log_enabled = schema_cfg.get("log", False)
    column_types = {k: v for k, v in schema_cfg.items() if k != "log"}

    for col, dtype_str in column_types.items():
        if col not in df.columns:
            continue

        try:
            if dtype_str.startswith("datetime"):
                df[col] = (
                    pd.to_datetime(df[col], errors="coerce", format="mixed")
                )
            else:
                df[col] = df[col].astype(dtype_str)
            if log_enabled:
                logger.info(
                    f"Enforced schema for column '{col}' as '{dtype_str}'."
                )
        except Exception as e:
            logger.warning(
                f"Failed to enforce schema for column '{col}' as '{dtype_str}': {e}."
            )
    
    return df


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
    if validation_cfg.get("no_double_spaces_in_store", False) and "store" in df.columns:
        mask = df["store"].astype("string").str.contains(r"  ", regex=False)
        count = int(mask.sum())
        if count > 0:
            logger.warning("Validation: %d rows have double spaces in 'store'.", count)

    # region_not_null
    if validation_cfg.get("region_not_null", False) and "region" in df.columns:
        mask = df["region"].isna()
        count = int(mask.sum())
        if count > 0:
            logger.warning("Validation: %d rows have null 'region'.", count)

    # sale_date_not_null
    if validation_cfg.get("sale_date_not_null", False) and "sale_date" in df.columns:
        mask = df["sale_date"].isna()
        count = int(mask.sum())
        if count > 0:
            logger.warning("Validation: %d rows have null 'sale_date'.", count)

    # region_uppercase
    if validation_cfg.get("region_uppercase", False) and "region" in df.columns:
        series = df["region"].dropna().astype("string")
        mask = series != series.str.upper()
        count = int(mask.sum())
        if count > 0:
            logger.warning("Validation: %d non-null 'region' values are not uppercase.", count)

    # category_uppercase
    if validation_cfg.get("category_uppercase", False) and "category" in df.columns:
        series = df["category"].dropna().astype("string")
        mask = series != series.str.upper()
        count = int(mask.sum())
        if count > 0:
            logger.warning("Validation: %d non-null 'category' values are not uppercase.", count)