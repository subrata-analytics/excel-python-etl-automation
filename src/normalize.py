# =============================================================================
# Normalizer
# =============================================================================
import logging
import pandas as pd

from typing import Any, Dict

from .utils.lineage import LineageWriter
from .transform import (
    rename_columns,
    drop_duplicate_rows,
    drop_empty_rows,
    drop_rows_missing_required,
    clean_text,
    standardize_text,
    normalize_with_reference_data,
    clean_numeric_values,
    parse_date,
    add_feature,
    filters_with_lineage,
    enforce_schema,
    run_validation,
)


def normalize_data(
    df: pd.DataFrame,
    pipeline_cfg: Dict[str, Any],
    logger: logging.Logger,
    lineage_writer: LineageWriter,
) -> pd.DataFrame:
    """
    Normalize the raw sales data according to the pipeline configuration.

    This function:
    - Preserves a stable row identifier for lineage
    - Applies column renaming, text cleaning, numeric cleaning
    - Drops duplicates and invalid rows with lineage logging
    - Parses dates, engineers features, enforces schema
    - Runs validation rules (log-only)
    - Flushes lineage_writer at the end
    """

    # Ensure stable row identifier for lineage
    if "__row_id__" not in df.columns:
        df = df.copy()
        df["__row_id__"] = df.index
        logger.info("Ensured stable row identifier.")

    # Column standardization (rename columns)
    column_mapping = pipeline_cfg.get("column_mapping", {})
    if column_mapping:
        df = rename_columns(df, column_mapping, lineage_writer, logger)

    # Duplicate handling: drop-rows lineage
    duplicates_cfg = pipeline_cfg.get("duplicates", {})
    if duplicates_cfg.get("drop", False):
        df = drop_duplicate_rows(df, duplicates_cfg, lineage_writer, logger)
    
     # Empty rows handling: drop-rows lineage
    empty_rows_cfg = pipeline_cfg.get("empty_rows", {})
    if duplicates_cfg.get("drop", False):
        df = drop_empty_rows(df, empty_rows_cfg, lineage_writer, logger)

    # Text normalization
    text_cleaning_cfg = pipeline_cfg.get("text_cleaning", {})
    if text_cleaning_cfg:
        df = clean_text(df, text_cleaning_cfg, logger)

    text_standardization_cfg = pipeline_cfg.get("text_standardization", {})
    if text_standardization_cfg:
        df = standardize_text(df, text_standardization_cfg, logger)

    # Reference data normalization (region & category)
    ref_cfg = pipeline_cfg.get("use_reference_data", {})
    if ref_cfg.get("enabled", False):
        df = normalize_with_reference_data(df, ref_cfg, lineage_writer, logger)

    # Numeric cleaning
    numeric_cleaning_cfg = pipeline_cfg.get("numeric_cleaning", {})
    if numeric_cleaning_cfg:
        df = clean_numeric_values(df, numeric_cleaning_cfg, lineage_writer, logger)

    # Drop rows with missing required fields
    drop_missing_cfg = pipeline_cfg.get("drop_missing", {})
    if drop_missing_cfg:
        df = drop_rows_missing_required(df, drop_missing_cfg, lineage_writer, logger)

    # Parse date
    date_parsing_cfg = pipeline_cfg.get("date_parsing", {})
    if date_parsing_cfg:
        df = parse_date(df, date_parsing_cfg, logger)

    # Feature engineering
    feature_eng_cfg = pipeline_cfg.get("feature_engineering", {})
    if feature_eng_cfg:
       df = add_feature(df, feature_eng_cfg, logger)

    # Filtering rules + drop-row lineage
    filters_cfg = pipeline_cfg.get("filters", {})
    if filters_cfg:
        df = filters_with_lineage(df, filters_cfg, lineage_writer, logger)

    # Schema enforcement
    schema_cfg = pipeline_cfg.get("schema", {})
    if schema_cfg:
        df = enforce_schema(df, schema_cfg, logger)

    # Validation rules (log anomalies, no drops)
    validation_cfg = pipeline_cfg.get("validation", {})
    if validation_cfg:
        run_validation(df, validation_cfg, logger)

    # Final lineage flush
    lineage_writer.flush()
    logger.info("Lineage flushed to disk.")
    logger.info("Normalization complete.")

    # Drop the internal row id before returning
    if "__row_id__" in df.columns:
        df = df.drop(columns=["__row_id__"])

    return df