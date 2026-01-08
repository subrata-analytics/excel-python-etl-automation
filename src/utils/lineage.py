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
        lineage_write: LineageWriter,
        row_id: int,
        column: str,
        old_value: Any,
        new_value: Any,
        rule: str
):
    if old_value == new_value:
        return
    
    lineage_write.write({
        "row_id": int(row_id),
        "column": column,
        "old_value": old_value,
        "new_value": new_value,
        "rule": rule,
        "timestamp": datetime.now(timezone.utc)
    })