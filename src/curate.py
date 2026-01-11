import pandas as pd
from logging import Logger

def curate_data(df: pd.DataFrame, 
                curation_cfg: dict,
                logger: Logger):
    logger.info("Preparing curated dataset")

    # Column selection
    df = df[curation_cfg["columns"]].copy()

    # Business validation
    df = df[
        (df["quantity"] >= 1) &
        (df["unit_price"] > 0) &
        (df["total_sales"] >= 0)
    ]

    # Reference enforcement
    reference_data = curation_cfg["reference"]
    df = df[
        df["region"].isin(reference_data["regions"]) &
        df["category"].isin(reference_data["categories"])
    ]

    # Deduplicate (business grain)
    df = df.drop_duplicates(
        subset=["store", "product_name", "sale_date"],
        keep="last"
    )

    # Final type enforcement
    df["quantity"] = df["quantity"].astype("int64")

    logger.info(f"Curated rows={len(df)}")

    return df
