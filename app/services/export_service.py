import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import settings


def export_to_csv(records: list[dict[str, Any]], layer: str, table: str, partition_date: str | None = None) -> str:
    date_str = partition_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = Path(settings.output_dir) / layer / table / f"date={date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / "part-0001.csv"

    if not records:
        file_path.write_text("", encoding="utf-8")
        return str(file_path)

    fieldnames = list(records[0].keys())
    with file_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    return str(file_path)
