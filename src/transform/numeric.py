import pandas as pd
from logging import Logger
from typing import Any, Dict, List
from src.utils.lineage import LineageWriter, log_lineage


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