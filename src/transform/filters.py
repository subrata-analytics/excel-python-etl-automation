import pandas as pd
from logging import Logger
from typing import Any, Dict
from ..utils.lineage import LineageWriter, log_lineage


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
            f"Applied filters. Dropped {int(dropped_mask.sum())} rows; remaining {len(df)} rows.")
    else:
        logger.info("Filters applied. No rows dropped. Total rows: %d.", len(df))
    
    return df

