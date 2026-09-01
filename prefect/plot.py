from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def load_runs(results_dir: Path) -> list[dict]:
    runs = []
    for path in sorted(results_dir.glob("auto_run_*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        results = data["results"]
        total_runs = data["summary"]["total_runs"]

        queue_waits = [
            r["queue_wait_seconds"]
            for r in results
            if r["queue_wait_seconds"] is not None
        ]
        work_durations = [r["work_duration_seconds"] for r in results]

        runs.append(
            {
                "total_runs": total_runs,
                "avg_queue_wait": statistics.mean(queue_waits) if queue_waits else 0.0,
                "std_queue_wait": (
                    statistics.stdev(queue_waits) if len(queue_waits) > 1 else 0.0
                ),
                "avg_work_duration": statistics.mean(work_durations),
                "std_work_duration": (
                    statistics.stdev(work_durations) if len(work_durations) > 1 else 0.0
                ),
            }
        )

    runs.sort(key=lambda r: r["total_runs"])
    return runs


def plot(runs: list[dict], output_path: Path | None) -> None:
    n_values = [r["total_runs"] for r in runs]
    avg_overhead = [r["avg_queue_wait"] for r in runs]
    std_overhead = [r["std_queue_wait"] for r in runs]
    avg_work = [r["avg_work_duration"] for r in runs]
    std_work = [r["std_work_duration"] for r in runs]

    fig, ax1 = plt.subplots(figsize=(10, 5))

    # Overhead line + std band
    color_overhead = "#d62728"
    ax1.plot(
        n_values,
        avg_overhead,
        color=color_overhead,
        linewidth=2,
        marker="o",
        markersize=4,
        label="Ø Overhead (queue_wait)",
    )
    ax1.fill_between(
        n_values,
        [a - s for a, s in zip(avg_overhead, std_overhead)],
        [a + s for a, s in zip(avg_overhead, std_overhead)],
        color=color_overhead,
        alpha=0.15,
        label="±1σ Overhead",
    )
    ax1.set_xlabel("Anzahl paralleler Flows (N)", fontsize=11)
    ax1.set_ylabel("Ø Overhead / Queue-Wait (s)", color=color_overhead, fontsize=11)
    ax1.tick_params(axis="y", labelcolor=color_overhead)
    ax1.xaxis.set_major_locator(ticker.FixedLocator([1, 5, 10, 15, 20]))
    ax1.set_xlim(min(n_values) - 0.5, max(n_values) + 0.5)
    ax1.set_ylim(bottom=0)

    # Work duration on secondary axis
    ax2 = ax1.twinx()
    color_work = "#1f77b4"
    ax2.plot(
        n_values,
        avg_work,
        color=color_work,
        linewidth=2,
        linestyle="--",
        marker="s",
        markersize=4,
        label="Ø Flow-Ausführungszeit (work)",
    )
    ax2.fill_between(
        n_values,
        [a - s for a, s in zip(avg_work, std_work)],
        [a + s for a, s in zip(avg_work, std_work)],
        color=color_work,
        alpha=0.12,
        label="±1σ Work",
    )
    ax2.set_ylabel("Ø Flow-Ausführungszeit (s)", color=color_work, fontsize=11)
    ax2.tick_params(axis="y", labelcolor=color_work)
    ax2.set_ylim(0, 2)

    # Combined legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)

    plt.title(
        "Prefect Skalierungstest: Overhead und Ausführungszeit bei parallelen Flows",
        fontsize=12,
        pad=12,
    )
    fig.tight_layout()

    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=150)
        print(f"Plot saved to: {output_path.resolve()}")
    else:
        plt.show()


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot Prefect scale test results.")
    parser.add_argument(
        "--results-dir",
        type=str,
        default="prefect-scale/results",
        help="Directory containing auto_run_*.json files.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Save plot to this file (e.g. plot.png). If omitted, shows interactively.",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    runs = load_runs(results_dir)

    if not runs:
        print(f"No auto_run_*.json files found in {results_dir}")
        return

    print(
        f"Loaded {len(runs)} run files ({runs[0]['total_runs']}..{runs[-1]['total_runs']} flows)"
    )
    plot(runs, Path(args.output) if args.output else None)


if __name__ == "__main__":
    main()
