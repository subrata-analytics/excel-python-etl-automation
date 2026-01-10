# =============================================================================
# Lineage
# =============================================================================
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Any


class LineageWriter:

    def __init__(self, output_file: str, buffer_size: int = 5000):
        self.output_file = Path(output_file)
        self.buffer_size = buffer_size
        self.buffer = []

        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        if not self.output_file.exists():
            pd.DataFrame(columns=[
                "row_id",
                "column",
                "old_value",
                "new_value",
                "rule",
                "timestamp"
            ]).to_csv(self.output_file, index=False)
    
    def write(self, record):
        self.buffer.append(record)
        if len(self.buffer) >= self.buffer_size:
            self.flush()
    
    def flush(self):
        if not self.buffer:
            return
        
        pd.DataFrame(self.buffer).to_csv(
            self.output_file,
            mode="a",
            header=False,
            index=False
        )
        self.buffer.clear()


def log_lineage(
        lineage_writer: LineageWriter,
        row_id: int,
        column: str,
        old_value: Any,
        new_value: Any,
        rule: str
    ):
    
    if _equal(old_value, new_value):
        return
    
    lineage_writer.write({
        "row_id": int(row_id),
        "column": column,
        "old_value": old_value,
        "new_value": new_value,
        "rule": rule,
        "timestamp": datetime.now(timezone.utc)
    })


def _equal(a: Any, b: Any) -> bool:
    # Treat both missing values as equal
    if pd.isna(a) and pd.isna(b):
        return True

    # If only one is missing, not equal
    if pd.isna(a) or pd.isna(b):
        return False

    # For numpy scalars, convert to Python scalars
    if isinstance(a, (np.generic,)) and isinstance(b, (np.generic,)):
        return a.item() == b.item()

    # For pandas Timestamp or Timedelta
    if (isinstance(a, (pd.Timestamp, pd.Timedelta)) 
        or 
        isinstance(b, (pd.Timestamp, pd.Timedelta))):
        try:
            return a == b
        except Exception:
            return False

    # Fallback: safe equality check
    try:
        return a == b
    except Exception:
        return False
