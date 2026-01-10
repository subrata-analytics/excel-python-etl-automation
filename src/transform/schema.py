import pandas as pd
from logging import Logger
from typing import Any, Dict


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
