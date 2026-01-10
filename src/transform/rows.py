import pandas as pd
from logging import Logger
from typing import Any, Dict, List
from ..utils.lineage import LineageWriter, log_lineage


__all__ = [
    "drop_duplicate_rows",
    "drop_empty_rows",
    "drop_rows_missing_required",
]


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