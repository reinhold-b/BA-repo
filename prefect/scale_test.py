from __future__ import annotations

import os

# Point to the already running Prefect server. Must be set before Prefect imports.
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")
os.environ.setdefault("PREFECT_SERVER_ALLOW_EPHEMERAL_MODE", "false")

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger


@flow(name="scale-test-flow")
def scale_test_flow(
    run_id: int,
    runtime_seconds: float = 2.0,
    submitted_at: float | None = None,
) -> dict[str, Any]:
    logger = get_run_logger()
    flow_started_at = time.perf_counter()
    queue_wait = None if submitted_at is None else flow_started_at - submitted_at

    logger.info("Flow run %s started", run_id)
    if queue_wait is not None:
        logger.info("Flow run %s queue_wait=%.4fs", run_id, queue_wait)

    work_start = time.perf_counter()
    for step in range(1, 4):
        logger.info("Flow run %s step %s/3", run_id, step)
        time.sleep(runtime_seconds / 3)
    work_duration = time.perf_counter() - work_start

    total_elapsed = time.perf_counter() - (
        submitted_at if submitted_at is not None else flow_started_at
    )
    logger.info("Flow run %s finished, total=%.4fs", run_id, total_elapsed)

    return {
        "run_id": run_id,
        "queue_wait_seconds": round(queue_wait, 4) if queue_wait is not None else None,
        "work_duration_seconds": round(work_duration, 4),
        "total_elapsed_seconds": round(total_elapsed, 4),
    }


def run_scale_test(
    total_runs: int, max_parallel: int, runtime_seconds: float
) -> dict[str, Any]:
    if total_runs < 1:
        raise ValueError("total_runs must be >= 1")
    if max_parallel < 1:
        raise ValueError("max_parallel must be >= 1")
    if max_parallel > total_runs:
        max_parallel = total_runs

    results: list[dict[str, Any]] = []
    batch_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = [
            executor.submit(
                scale_test_flow,
                run_id=run_id,
                runtime_seconds=runtime_seconds,
                submitted_at=time.perf_counter(),
            )
            for run_id in range(total_runs)
        ]
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda item: item["run_id"])
    batch_total = time.perf_counter() - batch_start

    queue_waits = [
        r["queue_wait_seconds"] for r in results if r["queue_wait_seconds"] is not None
    ]
    work_durations = [r["work_duration_seconds"] for r in results]
    sum_work = sum(work_durations)

    # SOF (Startup-Overhead-Faktor) als Laufzeitfaktor:
    # SOF = T_n / T_ideal
    # mit T_n = gemessene Gesamtlaufzeit des Batches (batch_total)
    # und T_ideal = ideale Parallelzeit ohne Orchestrierungs-Overhead (sum_work / max_parallel).
    # Interpretation: SOF = 1 ist ideal, SOF > 1 bedeutet zusätzlichen Overhead.
    ideal_parallel_time = sum_work / max_parallel
    sof_runtime_factor = (
        batch_total / ideal_parallel_time if ideal_parallel_time > 0 else None
    )
    summary: dict[str, Any] = {
        "total_runs": total_runs,
        "max_parallel": max_parallel,
        "runtime_seconds": runtime_seconds,
        "batch_total_runtime_seconds": round(batch_total, 4),
        "ideal_parallel_runtime_seconds": round(ideal_parallel_time, 4),
        "sof_runtime_factor": (
            round(sof_runtime_factor, 4) if sof_runtime_factor is not None else None
        ),
        "sum_work_duration_seconds": round(sum_work, 4),
        "average_queue_wait_seconds": (
            round(sum(queue_waits) / len(queue_waits), 4) if queue_waits else None
        ),
        "max_queue_wait_seconds": round(max(queue_waits), 4) if queue_waits else None,
        "estimated_overhead_seconds": round(batch_total - ideal_parallel_time, 4),
    }

    return {"results": results, "summary": summary}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prefect concurrency scale test (ThreadPoolExecutor, no server needed)."
    )
    parser.add_argument("--total-runs", type=int, default=10, help="Total flow runs.")
    parser.add_argument(
        "--max-parallel", type=int, default=5, help="Max concurrent flow runs."
    )
    parser.add_argument(
        "--runtime-seconds", type=float, default=2.0, help="Work time per flow run."
    )
    parser.add_argument(
        "--print-summary", action="store_true", help="Print summary to stdout."
    )
    parser.add_argument(
        "--output-file", type=str, default=None, help="Save results to this JSON file."
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Run all steps from 1 to 20 total runs, with max_parallel = total_runs + 5. "
        "Saves each step to --output-dir.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="prefect-scale/results",
        help="Output directory for --auto mode JSON files.",
    )
    args = parser.parse_args()

    if args.auto:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        all_summaries = []

        for total_runs in [1, 5, 10, 15, 20]:
            max_parallel = total_runs + 5
            print(
                f"\n--- auto: total_runs={total_runs}, max_parallel={max_parallel} ---"
            )
            payload = run_scale_test(
                total_runs=total_runs,
                max_parallel=max_parallel,
                runtime_seconds=args.runtime_seconds,
            )
            out_path = output_dir / f"auto_run_{total_runs:02d}.json"
            out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(f"  saved -> {out_path}")
            all_summaries.append(payload["summary"])

        combined_path = output_dir / "auto_summary.json"
        combined_path.write_text(json.dumps(all_summaries, indent=2), encoding="utf-8")
        print(f"\nAll done. Combined summary: {combined_path.resolve()}")
        return

    print(
        f"Starting scale test: total_runs={args.total_runs}, "
        f"max_parallel={args.max_parallel}, runtime_seconds={args.runtime_seconds}"
    )

    payload = run_scale_test(
        total_runs=args.total_runs,
        max_parallel=args.max_parallel,
        runtime_seconds=args.runtime_seconds,
    )

    if args.print_summary:
        print("\nSummary:")
        for key, value in payload["summary"].items():
            print(f"  {key}={value}")
        print("\nPer-run:")
        for item in payload["results"]:
            print(
                f"  run_id={item['run_id']}"
                f"  queue_wait={item['queue_wait_seconds']}s"
                f"  work={item['work_duration_seconds']}s"
                f"  total={item['total_elapsed_seconds']}s"
            )

    if args.output_file:
        output_path = Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nResults saved to: {output_path.resolve()}")

    print(f"\nCompleted {len(payload['results'])} flow runs.")


if __name__ == "__main__":
    main()


DEFAULT_POOL_NAME = "scale-test-pool"
DEFAULT_DEPLOYMENT_NAME = "scale-test-deployment"
TERMINAL_STATES = {"COMPLETED", "FAILED", "CRASHED", "CANCELLED"}
PREFECT_CMD = [sys.executable, "-m", "prefect"]


@flow(name="scale-test-flow")
def scale_test_flow(
    run_id: int, runtime_seconds: float = 2.0, submitted_epoch: float | None = None
) -> dict[str, Any]:
    logger = get_run_logger()
    started_epoch = time.time()
    queue_wait_seconds = (
        None if submitted_epoch is None else started_epoch - submitted_epoch
    )

    logger.info("Flow run %s started", run_id)
    if queue_wait_seconds is not None:
        logger.info("Flow run %s queue wait %.4fs", run_id, queue_wait_seconds)

    work_started = time.perf_counter()
    for step in range(1, 4):
        logger.info("Flow run %s step %s/3", run_id, step)
        time.sleep(runtime_seconds / 3)
    work_duration_seconds = time.perf_counter() - work_started

    logger.info("Flow run %s finished", run_id)
    return {
        "run_id": run_id,
        "submitted_epoch": submitted_epoch,
        "started_epoch": started_epoch,
        "queue_wait_seconds": (
            round(queue_wait_seconds, 4) if queue_wait_seconds is not None else None
        ),
        "work_duration_seconds": round(work_duration_seconds, 4),
    }


def build_prefect_env(api_url: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PREFECT_API_URL"] = api_url.rstrip("/") + "/api"
    return env


def extract_port(api_url: str) -> int:
    port_fragment = api_url.rsplit(":", maxsplit=1)[-1]
    return int(port_fragment.rstrip("/"))


def launch_background_process(
    command: list[str], env: dict[str, str], log_path: Path
) -> subprocess.Popen[str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("w", encoding="utf-8")
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    return subprocess.Popen(
        command,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        creationflags=creationflags,
    )


def wait_for_server(api_url: str, timeout_seconds: float) -> None:
    health_url = api_url.rstrip("/") + "/api/health"
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(1)

    raise RuntimeError(
        f"Prefect server at {health_url} was not ready within {timeout_seconds} seconds."
    )


def start_prefect_server(
    api_url: str, env: dict[str, str], log_dir: Path
) -> subprocess.Popen[str]:
    command = [
        *PREFECT_CMD,
        "server",
        "start",
        "--host",
        "127.0.0.1",
        "--port",
        str(extract_port(api_url)),
    ]
    process = launch_background_process(command, env, log_dir / "prefect-server.log")
    wait_for_server(api_url=api_url, timeout_seconds=90)
    return process


def run_prefect_cli(command: list[str], env: dict[str, str]) -> str:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise subprocess.CalledProcessError(
            completed.returncode,
            completed.args,
            output=completed.stdout,
            stderr=completed.stderr,
        )
    return completed.stdout.strip()


def ensure_work_pool(pool_name: str, env: dict[str, str]) -> None:
    inspect_command = [*PREFECT_CMD, "work-pool", "inspect", pool_name]
    inspect_result = subprocess.run(
        inspect_command, capture_output=True, text=True, env=env
    )
    if inspect_result.returncode == 0:
        return

    create_command = [
        *PREFECT_CMD,
        "work-pool",
        "create",
        pool_name,
        "--type",
        "process",
    ]
    run_prefect_cli(create_command, env)


def start_prefect_workers(
    worker_count: int, pool_name: str, env: dict[str, str], log_dir: Path
) -> list[subprocess.Popen[str]]:
    processes: list[subprocess.Popen[str]] = []
    for index in range(1, worker_count + 1):
        command = [
            *PREFECT_CMD,
            "worker",
            "start",
            "--pool",
            pool_name,
            "--type",
            "process",
        ]
        process = launch_background_process(
            command, env, log_dir / f"worker-{index}.log"
        )
        processes.append(process)
    return processes


async def register_deployment(pool_name: str, deployment_name: str) -> str:
    deployment = await scale_test_flow.to_deployment(
        name=deployment_name,
        work_pool_name=pool_name,
    )
    deployment_id = await deployment.apply()
    return str(deployment_id)


async def submit_flow_runs(
    deployment_id: str, total_runs: int, runtime_seconds: float
) -> list[dict[str, Any]]:
    submitted_runs: list[dict[str, Any]] = []

    async with get_client() as client:
        for run_id in range(total_runs):
            submitted_epoch = time.time()
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters={
                    "run_id": run_id,
                    "runtime_seconds": runtime_seconds,
                    "submitted_epoch": submitted_epoch,
                },
            )
            submitted_runs.append(
                {
                    "run_id": run_id,
                    "flow_run_id": str(flow_run.id),
                    "submitted_epoch": submitted_epoch,
                    "submitted_at": datetime.fromtimestamp(submitted_epoch).isoformat(),
                }
            )

        pending_ids = {item["flow_run_id"] for item in submitted_runs}
        while pending_ids:
            for item in submitted_runs:
                flow_run_id = item["flow_run_id"]
                if flow_run_id not in pending_ids:
                    continue

                flow_run = await client.read_flow_run(flow_run_id)
                state_name = (flow_run.state_name or "UNKNOWN").upper()
                if state_name not in TERMINAL_STATES:
                    continue

                item["state_name"] = state_name
                item["started_at"] = (
                    flow_run.start_time.isoformat() if flow_run.start_time else None
                )
                item["ended_at"] = (
                    flow_run.end_time.isoformat() if flow_run.end_time else None
                )
                item["queue_wait_seconds"] = (
                    round(flow_run.start_time.timestamp() - item["submitted_epoch"], 4)
                    if flow_run.start_time
                    else None
                )
                item["work_duration_seconds"] = (
                    round(
                        flow_run.end_time.timestamp() - flow_run.start_time.timestamp(),
                        4,
                    )
                    if flow_run.start_time and flow_run.end_time
                    else None
                )
                item["total_elapsed_seconds"] = (
                    round(flow_run.end_time.timestamp() - item["submitted_epoch"], 4)
                    if flow_run.end_time
                    else None
                )
                pending_ids.remove(flow_run_id)

            if pending_ids:
                await asyncio.sleep(1)

    return submitted_runs


def build_summary(
    results: list[dict[str, Any]], runtime_seconds: float, worker_count: int
) -> dict[str, Any]:
    if not results:
        return {
            "total_runs": 0,
            "worker_count": worker_count,
            "runtime_seconds": runtime_seconds,
        }

    queue_waits = [
        item["queue_wait_seconds"]
        for item in results
        if item["queue_wait_seconds"] is not None
    ]
    work_durations = [
        item["work_duration_seconds"]
        for item in results
        if item["work_duration_seconds"] is not None
    ]
    total_elapsed = [
        item["total_elapsed_seconds"]
        for item in results
        if item["total_elapsed_seconds"] is not None
    ]

    batch_total_runtime_seconds = max(total_elapsed) if total_elapsed else None
    expected_serial_runtime_seconds = round(len(results) * runtime_seconds, 4)
    startup_overhead_factor = None
    if batch_total_runtime_seconds is not None and expected_serial_runtime_seconds > 0:
        startup_overhead_factor = round(
            batch_total_runtime_seconds / expected_serial_runtime_seconds, 4
        )

    return {
        "total_runs": len(results),
        "worker_count": worker_count,
        "runtime_seconds": runtime_seconds,
        "batch_total_runtime_seconds": (
            round(batch_total_runtime_seconds, 4)
            if batch_total_runtime_seconds is not None
            else None
        ),
        "expected_serial_runtime_seconds": expected_serial_runtime_seconds,
        "average_queue_wait_seconds": (
            round(sum(queue_waits) / len(queue_waits), 4) if queue_waits else None
        ),
        "max_queue_wait_seconds": round(max(queue_waits), 4) if queue_waits else None,
        "average_work_duration_seconds": (
            round(sum(work_durations) / len(work_durations), 4)
            if work_durations
            else None
        ),
        "estimated_overhead_seconds": (
            round(batch_total_runtime_seconds - expected_serial_runtime_seconds, 4)
            if batch_total_runtime_seconds is not None
            else None
        ),
        "startup_overhead_factor": startup_overhead_factor,
    }


def terminate_processes(processes: list[subprocess.Popen[str]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Prefect scale test with one server and multiple workers."
    )
    parser.add_argument(
        "--total-runs",
        type=int,
        default=10,
        help="How many flow runs to start in total.",
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        default=5,
        help="How many Prefect workers should be started.",
    )
    parser.add_argument(
        "--runtime-seconds",
        type=float,
        default=2.0,
        help="Approximate runtime per flow run.",
    )
    parser.add_argument(
        "--pool-name",
        type=str,
        default=DEFAULT_POOL_NAME,
        help="Prefect work pool name.",
    )
    parser.add_argument(
        "--deployment-name",
        type=str,
        default=DEFAULT_DEPLOYMENT_NAME,
        help="Prefect deployment name.",
    )
    parser.add_argument(
        "--api-url",
        type=str,
        default="http://127.0.0.1:4200",
        help="Prefect server base URL.",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="Optional path to save the results as JSON.",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default="logs",
        help="Directory for Prefect server and worker logs.",
    )
    parser.add_argument(
        "--reuse-existing-server",
        action="store_true",
        help="Reuse an already running Prefect server instead of starting a new one.",
    )
    parser.add_argument(
        "--reuse-existing-workers",
        action="store_true",
        help="Do not start new workers if they are already managed externally.",
    )
    parser.add_argument(
        "--keep-alive",
        action="store_true",
        help="Keep the started server and workers running after the test.",
    )
    parser.add_argument(
        "--print-summary", action="store_true", help="Print a summary after completion."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_dir = Path(args.log_dir)
    api_url = args.api_url.rstrip("/")
    env = build_prefect_env(api_url)

    server_processes: list[subprocess.Popen[str]] = []
    worker_processes: list[subprocess.Popen[str]] = []
    keep_alive = args.keep_alive

    try:
        if args.reuse_existing_server:
            wait_for_server(api_url=api_url, timeout_seconds=15)
        else:
            print(f"Starting one Prefect server at {api_url}...")
            server_processes.append(
                start_prefect_server(api_url=api_url, env=env, log_dir=log_dir)
            )

        ensure_work_pool(pool_name=args.pool_name, env=env)

        if not args.reuse_existing_workers:
            print(
                f"Starting {args.worker_count} Prefect workers for pool '{args.pool_name}'..."
            )
            worker_processes = start_prefect_workers(
                worker_count=args.worker_count,
                pool_name=args.pool_name,
                env=env,
                log_dir=log_dir,
            )
            time.sleep(5)

        print("Registering deployment...")
        deployment_id = asyncio.run(
            register_deployment(args.pool_name, args.deployment_name)
        )

        print(
            f"Starting scale test with total_runs={args.total_runs}, worker_count={args.worker_count}, "
            f"runtime_seconds={args.runtime_seconds}"
        )
        results = asyncio.run(
            submit_flow_runs(
                deployment_id=deployment_id,
                total_runs=args.total_runs,
                runtime_seconds=args.runtime_seconds,
            )
        )
        summary = build_summary(
            results=results,
            runtime_seconds=args.runtime_seconds,
            worker_count=args.worker_count,
        )
        payload = {"results": results, "summary": summary}

        if args.print_summary:
            print("\nSummary:")
            for key, value in summary.items():
                print(f"  {key}={value}")

        if args.output_file:
            output_path = Path(args.output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(f"\nResults saved to: {output_path.resolve()}")

        print(f"\nCompleted {len(results)} flow runs.")
        if keep_alive:
            print(
                "Server and worker processes are still running because --keep-alive was set."
            )
            server_processes = []
            worker_processes = []
    finally:
        terminate_processes(worker_processes)
        terminate_processes(server_processes)


if __name__ == "__main__":
    main()
