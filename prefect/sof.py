from __future__ import annotations

import json
from pathlib import Path


def read_auto_sof(results_dir: str | Path):
    directory = Path(results_dir)
    rows = []

    for file in sorted(directory.glob("auto_*.json")):
        try:
            with file.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except (json.JSONDecodeError, OSError):
            continue

        if not isinstance(payload, dict):
            continue

        summary = payload.get("summary")
        if not isinstance(summary, dict):
            continue

        batch_time = summary.get("batch_total_runtime_seconds")
        ideal_time = summary.get("ideal_parallel_runtime_seconds")
        total_runs = summary.get("total_runs")

        if None in (batch_time, ideal_time, total_runs):
            continue

        sof = batch_time / ideal_time if ideal_time else None
        rows.append(
            {
                "file": file.name,
                "total_runs": total_runs,
                "batch_time": batch_time,
                "ideal_parallel_runtime_seconds": ideal_time,
                "sof_factor": round(sof, 4) if sof is not None else None,
            }
        )

    return rows


if __name__ == "__main__":
    results_dir = Path(__file__).resolve().parent / "results"
    sof_list = read_auto_sof(results_dir)

    print("SOF-Liste aus auto_* JSONs:")
    for item in sof_list:
        print(item)

    print("\nNur Werte:")
    print([item["sof_factor"] for item in sof_list])
