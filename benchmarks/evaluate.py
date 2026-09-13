"""Run isolated queue benchmarks and persist every pilot and measured trial.

python -m benchmarks.evaluate --output docs/performance/raw.json
Each child measures only one trial; the parent enforces a hard process timeout.
"""

from __future__ import annotations

import argparse
import cProfile
import gc
import hashlib
import json
import math
import os
import platform
import pstats
import random
import subprocess
import sys
import threading
import time
import tracemalloc
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from queue import Empty, Queue, SimpleQueue

import psutil

from lockfree_queue import LockFreeQueue
from lockfree_queue._atomic import AtomicUInt64

ROOT = Path(__file__).resolve().parents[1]
FACTORIES = {cls.__name__: cls for cls in (LockFreeQueue, SimpleQueue, Queue)}


@dataclass(frozen=True)
class Scenario:
    name: str
    producers: int = 4
    consumers: int = 4
    mode: str = "live"
    switch_interval: float = 0.005
    backoff: float = 0.0
    producer_pause: float = 0.0
    consumer_pause: float = 0.0
    payload_size: int = 0
    sample_every: int = 0
    fixed_items: int = 0


SCENARIOS = [
    *(Scenario(f"scale_{n}p{n}c", n, n) for n in (1, 2, 4, 8, 16)),
    Scenario("skew_8p1c", 8, 1),
    Scenario("skew_1p8c", 1, 8),
    Scenario("fill_1p", 1, 0, "fill"),
    Scenario("fill_8p", 8, 0, "fill"),
    Scenario("drain_1c", 0, 1, "drain"),
    Scenario("drain_8c", 0, 8, "drain"),
    Scenario("switch_0.1ms", switch_interval=0.0001),
    Scenario("backoff_0.5ms", backoff=0.0005),
    Scenario("payload_64KiB", payload_size=65536),
    Scenario("latency_burst", sample_every=4, fixed_items=4096),
    Scenario("latency_sparse", 1, 1, producer_pause=0.001, sample_every=1, fixed_items=400),
    Scenario("latency_slow_consumer", consumer_pause=0.001, sample_every=1, fixed_items=1024),
]


def percentile(values, percent):
    """Nearest-rank percentile (no interpolation of timing samples)."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)]


def latency_summary(values):
    return {
        "samples": len(values),
        **{f"p{p}_us": percentile(values, p) for p in (50, 95, 99)},
        "max_us": max(values, default=None),
    }


def verify_delivery(batches, total, producers):
    """Exact identity coverage plus per-producer order in each consumer's log."""
    seen = bytearray(total)
    for batch in batches:
        last = {}
        for token in batch:
            if not isinstance(token, int) or not 0 <= token < total or seen[token]:
                raise AssertionError(f"Invalid or duplicate message ID: {token}")
            seen[token] = 1
            producer = token % max(1, producers)
            if token <= last.get(producer, -1):
                raise AssertionError("Per-producer FIFO violation")
            last[producer] = token
    if not all(seen):
        raise AssertionError(f"Missing {total - sum(seen)} messages")


def transfer(queue_name, scenario, total):
    factory = FACTORIES[queue_name]
    sys.setswitchinterval(scenario.switch_interval)
    queue = factory()
    payload = bytes(scenario.payload_size)  # Shared reference; no byte copying benchmark.
    messages = [(token, payload, None) for token in range(total)]
    streams = [messages[p :: scenario.producers] for p in range(scenario.producers)]
    sentinel = object()
    if scenario.mode == "drain":
        for item in messages:
            queue.put_nowait(item)
        for _ in range(scenario.consumers):
            queue.put_nowait(sentinel)

    ready = threading.Barrier(scenario.producers + scenario.consumers + 1)
    go = threading.Event()
    cancel = threading.Event()
    errors = []
    results = [None] * scenario.consumers
    put_times = [[] for _ in range(scenario.producers)]
    finish_times = [0.0] * scenario.producers

    def produce(index):
        ready.wait(timeout=15)
        go.wait()
        for sequence, item in enumerate(streams[index]):
            if cancel.is_set():
                return
            sampled = scenario.sample_every and sequence % scenario.sample_every == 0
            if sampled:
                start = time.perf_counter_ns()
                item = (item[0], item[1], start)
            queue.put_nowait(item)
            if sampled:
                put_times[index].append((time.perf_counter_ns() - start) / 1000)
            if scenario.producer_pause:
                time.sleep(scenario.producer_pause)
        finish_times[index] = time.perf_counter()

    def consume(index):
        received, get_times, end_to_end = [], [], []
        polls, calls = 0, 0
        ready.wait(timeout=15)
        go.wait()
        while not cancel.is_set():
            # Sample call latency by attempt, independently of message ID.
            sampled = scenario.sample_every and calls % scenario.sample_every == 0
            calls += 1
            if sampled:
                start = time.perf_counter_ns()
            try:
                item = queue.get_nowait()
            except Empty:
                polls += 1
                time.sleep(scenario.backoff)
                continue
            end = time.perf_counter_ns() if sampled or item is not sentinel and item[2] else 0
            if item is sentinel:
                break
            if sampled:
                get_times.append((end - start) / 1000)
            if item[2] is not None:
                end_to_end.append((end - item[2]) / 1000)
            if item[1] is not payload:
                raise AssertionError("Payload reference changed")
            received.append(item[0])
            if scenario.consumer_pause:
                time.sleep(scenario.consumer_pause)
        results[index] = (received, polls, get_times, end_to_end)

    def guarded(fn, index):
        try:
            fn(index)
        except BaseException as exc:
            errors.append(repr(exc))
            cancel.set()
            ready.abort()

    producers = [
        threading.Thread(target=guarded, args=(produce, p), daemon=True)
        for p in range(scenario.producers)
    ]
    consumers = [
        threading.Thread(target=guarded, args=(consume, c), daemon=True)
        for c in range(scenario.consumers)
    ]
    for thread in producers + consumers:
        thread.start()
    ready.wait(timeout=15)
    process = psutil.Process()
    ctx_before = process.num_ctx_switches()
    gc_before = [generation["collections"] for generation in gc.get_stats()]
    cpu_start = time.process_time()
    start = time.perf_counter()
    go.set()
    # The parent kills this entire subprocess on timeout, including stuck workers.
    for thread in producers:
        thread.join()
    if scenario.mode == "live" and not errors:
        for _ in consumers:
            queue.put_nowait(sentinel)
    for thread in consumers:
        thread.join()
    elapsed = time.perf_counter() - start
    cpu_seconds = time.process_time() - cpu_start
    ctx_after = process.num_ctx_switches()
    gc_after = [generation["collections"] for generation in gc.get_stats()]
    if errors:
        raise RuntimeError(errors)
    if scenario.mode == "fill":
        batches = [[queue.get_nowait()[0] for _ in range(total)]]
    else:
        batches = [result[0] for result in results]
    verify_delivery(batches, total, scenario.producers)
    if not queue.empty():
        raise AssertionError("Queue is not empty after verification")
    counts = [len(batch) for batch in batches] if scenario.consumers else []
    jain = sum(counts) ** 2 / (len(counts) * sum(n * n for n in counts)) if counts else None
    return {
        "kind": "transfer",
        "queue": queue_name,
        "scenario": asdict(scenario),
        "messages": total,
        "seconds": elapsed,
        "messages_per_second": total / elapsed,
        "cpu_seconds": cpu_seconds,
        "cpu_cores_used": cpu_seconds / elapsed,
        "context_switches": sum(ctx_after) - sum(ctx_before),
        "gc_collections": [b - a for a, b in zip(gc_before, gc_after, strict=True)],
        "empty_polls": sum(result[1] for result in results),
        "consumer_counts": counts,
        "consumer_jain": jain,
        "producer_finish_seconds": [finish - start for finish in finish_times],
        "put_latency": latency_summary([t for batch in put_times for t in batch]),
        "get_latency": latency_summary([t for result in results for t in result[2]]),
        "end_to_end_latency": latency_summary([t for result in results for t in result[3]]),
        "delivery_verified": True,
    }


def memory_measure(queue_name, depth, traced):
    factory = FACTORIES[queue_name]
    payload = object()
    gc.collect()
    process = psutil.Process()
    if traced:
        tracemalloc.start()

    def snapshot():
        gc.collect()
        if traced:
            return {"python_bytes": tracemalloc.get_traced_memory()[0]}
        info = process.memory_info()
        return {"rss_bytes": info.rss, "private_bytes": getattr(info, "private", None)}

    before = snapshot()
    queue = factory()
    empty = snapshot()
    for _ in range(depth):
        queue.put_nowait(payload)
    filled = snapshot()
    for _ in range(depth):
        if queue.get_nowait() is not payload:
            raise AssertionError("Memory run payload mismatch")
    drained = snapshot()
    registry_bytes = sys.getsizeof(queue._nodes) if isinstance(queue, LockFreeQueue) else None
    registry_nodes = len(queue._nodes) if isinstance(queue, LockFreeQueue) else None
    del queue
    destroyed = snapshot()
    if traced:
        tracemalloc.stop()
    return {
        "kind": "memory",
        "queue": queue_name,
        "depth": depth,
        "traced": traced,
        "before": before,
        "empty": empty,
        "filled": filled,
        "drained": drained,
        "destroyed": destroyed,
        "registry_bytes_after_drain": registry_bytes,
        "registry_nodes_after_drain": registry_nodes,
        "delivery_verified": True,
    }


def contention_measure(scenario, total):
    """Diagnostic only: counters perturb scheduling, never use this as throughput."""
    counters = {}
    local = threading.local()
    original_load = AtomicUInt64.load
    original_cas = AtomicUInt64.compare_exchange

    def counter():
        if not hasattr(local, "counts"):
            local.counts = {"loads": 0, "cas_attempts": 0, "cas_failures": 0}
            counters[threading.get_ident()] = local.counts
        return local.counts

    def load(self):
        counter()["loads"] += 1
        return original_load(self)

    def compare_exchange(self, expected, desired):
        counts = counter()
        counts["cas_attempts"] += 1
        success = original_cas(self, expected, desired)
        counts["cas_failures"] += not success
        return success

    AtomicUInt64.load = load
    AtomicUInt64.compare_exchange = compare_exchange
    try:
        result = transfer("LockFreeQueue", scenario, total)
    finally:
        AtomicUInt64.load = original_load
        AtomicUInt64.compare_exchange = original_cas
    result["kind"] = "contention"
    result["atomic_counters"] = {
        key: sum(counts[key] for counts in counters.values())
        for key in ("loads", "cas_attempts", "cas_failures")
    }
    return result


def primitive_measure(iterations):
    atomic = AtomicUInt64()
    queue = LockFreeQueue()

    def cycle():
        queue.put_nowait(None)
        queue.get_nowait()

    def empty_exception():
        try:
            queue.get_nowait()
        except Empty:
            pass

    cases = {
        "noop": lambda: None,
        "atomic_construct": AtomicUInt64,
        "atomic_load": atomic.load,
        "atomic_cas_success": lambda: atomic.compare_exchange(0, 0),
        "atomic_cas_failure": lambda: atomic.compare_exchange(1, 2),
        "queue_put_get_pair": cycle,
        "empty_get_exception": empty_exception,
        "empty_try_get": queue.try_get,
        "empty_snapshot": queue.empty,
    }
    rows = []
    for name, fn in cases.items():
        for _ in range(100):
            fn()
        start = time.perf_counter_ns()
        for _ in range(iterations):
            fn()
        rows.append(
            {"operation": name, "ns_per_call": (time.perf_counter_ns() - start) / iterations}
        )
    return {"kind": "primitives", "iterations": iterations, "operations": rows}


def profile_measure(iterations):
    queue = LockFreeQueue()
    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(iterations):
        queue.put_nowait(None)
        queue.get_nowait()
    profiler.disable()
    stats = pstats.Stats(profiler)
    rows = []
    for (filename, line, function), (primitive, calls, own, cumulative, _) in stats.stats.items():
        # No personal machine paths in the public artifact.
        rows.append(
            {
                "function": f"{Path(filename).name}:{line}:{function}",
                "primitive_calls": primitive,
                "calls": calls,
                "self_seconds": own,
                "cumulative_seconds": cumulative,
            }
        )
    return {
        "kind": "profile",
        "iterations": iterations,
        "total_seconds": stats.total_tt,
        "top_cumulative": sorted(rows, key=lambda row: -row["cumulative_seconds"])[:25],
        "top_self": sorted(rows, key=lambda row: -row["self_seconds"])[:25],
    }


def child(request):
    kind = request["kind"]
    queue_name = request.get("queue", "LockFreeQueue")
    # Warm imports, native operation selection, and allocator paths outside timing.
    queue = FACTORIES[queue_name]()
    for _ in range(100):
        queue.put_nowait(None)
        queue.get_nowait()
    del queue
    gc.collect()
    if kind == "transfer":
        return transfer(queue_name, Scenario(**request["scenario"]), request["messages"])
    if kind == "memory":
        return memory_measure(queue_name, request["depth"], request["traced"])
    if kind == "contention":
        return contention_measure(Scenario(**request["scenario"]), request["messages"])
    if kind == "primitives":
        return primitive_measure(request["iterations"])
    if kind == "profile":
        return profile_measure(request["iterations"])
    raise ValueError(f"Unknown benchmark kind: {kind}")


def run_child(request, timeout):
    completed = subprocess.run(
        [sys.executable, "-m", "benchmarks.evaluate", "--child"],
        input=json.dumps(request),
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=ROOT,
        timeout=timeout,
        check=True,
    )
    return json.loads(completed.stdout)


def metadata(args):
    cpu_name = platform.processor()
    if sys.platform == "win32":
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0"
        ) as key:
            cpu_name = winreg.QueryValueEx(key, "ProcessorNameString")[0].strip()
    tracked = [*sorted((ROOT / "src/lockfree_queue").glob("*.py")), Path(__file__)]
    return {
        "utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "platform": platform.platform(),
        "cpu": cpu_name,
        "logical_cpus": os.cpu_count(),
        "physical_cores": psutil.cpu_count(logical=False),
        "affinity_cpus": len(psutil.Process().cpu_affinity()),
        "memory_bytes": psutil.virtual_memory().total,
        "system_cpu_percent_before": psutil.cpu_percent(interval=0.5),
        "system_cpu_percent_after": None,
        "versions": {name: version(name) for name in ("atomics", "cffi", "psutil")},
        "base_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "source_sha256": {
            str(path.relative_to(ROOT)).replace("\\", "/"): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in tracked
        },
        "settings": vars(args),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="docs/performance/raw.json")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--target-seconds", type=float, default=0.4)
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--seed", type=int, default=20260913)
    parser.add_argument("--only", help="Comma-separated scenario names (omit for the entire suite)")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        print(json.dumps(child(json.loads(sys.stdin.read()))))
        return
    if args.rounds < 1 or args.target_seconds <= 0 or args.timeout <= 0:
        parser.error("rounds, target-seconds, and timeout must be positive")
    selected = args.only.split(",") if args.only else [scenario.name for scenario in SCENARIOS]
    if set(selected) - {scenario.name for scenario in SCENARIOS}:
        parser.error("Unknown scenario in --only")
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    document = {"schema_version": 1, "metadata": metadata(args), "pilots": [], "results": []}

    def save():
        output.write_text(
            json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )

    save()
    rng = random.Random(args.seed)
    jobs = []
    for scenario in SCENARIOS:
        if scenario.name not in selected:
            continue
        for queue_name in FACTORIES:
            if scenario.fixed_items:
                count = scenario.fixed_items
            else:
                count = 256 if queue_name == "LockFreeQueue" else 4096
                divisor = max(1, scenario.producers)
                for _ in range(3):
                    request = {
                        "kind": "transfer",
                        "queue": queue_name,
                        "scenario": asdict(scenario),
                        "messages": count,
                    }
                    pilot = run_child(request, args.timeout)
                    document["pilots"].append(pilot)
                    save()
                    if pilot["seconds"] >= args.target_seconds * 0.8 or count >= 500_000:
                        break
                    count = min(
                        500_000,
                        max(64, math.ceil(count * args.target_seconds / pilot["seconds"])),
                    )
                    count = math.ceil(count / divisor) * divisor
            for round_number in range(1, args.rounds + 1):
                jobs.append(
                    {
                        "kind": "transfer",
                        "queue": queue_name,
                        "scenario": asdict(scenario),
                        "messages": count,
                        "round": round_number,
                    }
                )
            print(f"calibrated {scenario.name} {queue_name}: {count} messages", flush=True)
    if not args.only:
        for scenario in SCENARIOS:
            if scenario.name in {"scale_1p1c", "scale_4p4c", "scale_16p16c", "switch_0.1ms"}:
                for round_number in range(1, args.rounds + 1):
                    jobs.append(
                        {
                            "kind": "contention",
                            "queue": "LockFreeQueue",
                            "scenario": asdict(scenario),
                            "messages": 1024,
                            "round": round_number,
                        }
                    )
        for queue_name in FACTORIES:
            for depth in (1000, 10_000):
                for traced in (False, True):
                    for round_number in range(1, args.rounds + 1):
                        jobs.append(
                            {
                                "kind": "memory",
                                "queue": queue_name,
                                "depth": depth,
                                "traced": traced,
                                "round": round_number,
                            }
                        )
        for round_number in range(1, args.rounds + 1):
            jobs.append({"kind": "primitives", "iterations": 5000, "round": round_number})
        jobs.append({"kind": "profile", "iterations": 1000, "round": 1})
    rng.shuffle(jobs)
    for index, request in enumerate(jobs, start=1):
        try:
            result = run_child(request, args.timeout)
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as exc:
            document["error"] = {
                "request": request,
                "reason": str(exc),
                "stderr": getattr(exc, "stderr", None),
            }
            save()
            raise
        result["round"] = request["round"]
        document["results"].append(result)
        save()
        label = request.get("scenario", {}).get("name", request["kind"])
        print(
            f"{index}/{len(jobs)} {request.get('queue', '')} {label} round {request['round']}",
            flush=True,
        )
    document["metadata"]["system_cpu_percent_after"] = psutil.cpu_percent(interval=0.5)
    document["metadata"]["completed_utc"] = datetime.now(timezone.utc).isoformat()
    save()
    print(f"Saved {len(document['results'])} measurements to {output}", flush=True)


if __name__ == "__main__":
    main()
