import pandas as pd
from logging import Logger
from typing import Any, Dict, SupportsInt, cast
from ..utils.lineage import LineageWriter, log_lineage


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
