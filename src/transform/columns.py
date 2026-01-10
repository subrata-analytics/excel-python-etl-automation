import pandas as pd
from logging import Logger
from typing import Dict
from ..utils.lineage import LineageWriter, log_lineage


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