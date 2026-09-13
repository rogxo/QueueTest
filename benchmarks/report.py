"""Generate a Chinese performance report, CSV tables and exportable PNG charts.

python -m benchmarks.report --input docs/performance/raw.json
"""

# Long strings preserve Markdown table rows and translated prose.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

QUEUES = ("LockFreeQueue", "SimpleQueue", "Queue")


def median(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def summarize(document):
    if "error" in document or "completed_utc" not in document["metadata"]:
        raise ValueError("Refusing to publish an incomplete benchmark run")
    groups = defaultdict(list)
    memory_groups = defaultdict(list)
    for row in document["results"]:
        if (
            row["kind"] in {"transfer", "contention", "memory"}
            and row.get("delivery_verified") is not True
        ):
            raise ValueError("Delivery validation failed")
        if row["kind"] == "transfer":
            groups[(row["scenario"]["name"], row["queue"])].append(row)
        if row["kind"] == "memory":
            memory_groups[(row["queue"], row["depth"], row["traced"])].append(row)
    transfers = []
    for (name, queue), rows in sorted(groups.items()):
        rates = [row["messages_per_second"] for row in rows]
        result = {
            "scenario": name,
            "queue": queue,
            "rounds": len(rows),
            "messages_per_round": rows[0]["messages"],
            "throughput_median": median(rates),
            "throughput_min": min(rates),
            "throughput_max": max(rates),
            "throughput_cv_percent": statistics.pstdev(rates) / statistics.mean(rates) * 100,
            "seconds_median": median([row["seconds"] for row in rows]),
            "seconds_min": min(row["seconds"] for row in rows),
            "cpu_cores_median": median([row["cpu_cores_used"] for row in rows]),
            "cpu_us_per_message": median(
                [row["cpu_seconds"] / row["messages"] * 1e6 for row in rows]
            ),
            "empty_polls_per_message": median(
                [row["empty_polls"] / row["messages"] for row in rows]
            ),
            "consumer_jain_median": median([row["consumer_jain"] for row in rows]),
            "ctx_switches_per_message": median(
                [row["context_switches"] / row["messages"] for row in rows]
            ),
        }
        for name in ("put_latency", "get_latency", "end_to_end_latency"):
            result[f"{name}_samples"] = sum(row[name]["samples"] for row in rows)
            for metric in ("p50_us", "p95_us", "p99_us", "max_us"):
                values = [row[name][metric] for row in rows if row[name][metric] is not None]
                result[f"{name}_{metric}"] = (
                    max(values, default=None) if metric == "max_us" else median(values)
                )
        transfers.append(result)
    memory = []
    for (queue, depth, traced), rows in sorted(memory_groups.items()):
        key = "python_bytes" if traced else "rss_bytes"
        memory.append(
            {
                "queue": queue,
                "depth": depth,
                "measurement": "tracemalloc" if traced else "rss",
                "filled_minus_empty_bytes": median(
                    [row["filled"][key] - row["empty"][key] for row in rows]
                ),
                "drained_minus_empty_bytes": median(
                    [row["drained"][key] - row["empty"][key] for row in rows]
                ),
                "destroyed_minus_before_bytes": median(
                    [row["destroyed"][key] - row["before"][key] for row in rows]
                ),
                "registry_bytes_after_drain": median(
                    [row["registry_bytes_after_drain"] for row in rows]
                ),
            }
        )
    return {"transfers": transfers, "memory": memory}


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def require_full_suite(document, summary):
    """Reject missing/duplicate trials before overwriting any report artifacts."""
    from .evaluate import SCENARIOS

    rounds = document["metadata"]["settings"]["rounds"]
    expected = {(scenario.name, queue) for scenario in SCENARIOS for queue in QUEUES}
    actual = {(row["scenario"], row["queue"]) for row in summary["transfers"]}
    if actual != expected:
        raise ValueError("A full scenario set is required to generate this report")
    groups = defaultdict(list)
    for row in document["results"]:
        key = (
            row["kind"],
            row.get("queue"),
            row.get("scenario", {}).get("name"),
            row.get("depth"),
            row.get("traced"),
        )
        groups[key].append(row["round"])
    for key, trials in groups.items():
        count = 1 if key[0] == "profile" else rounds
        if sorted(trials) != list(range(1, count + 1)):
            raise ValueError(f"Missing or duplicate rounds: {key}")
    kinds = {key[0] for key in groups}
    if kinds != {"transfer", "contention", "memory", "primitives", "profile"}:
        raise ValueError("Missing memory, contention, primitives, or profile measurements")
    if len(summary["memory"]) != len(QUEUES) * 2 * 2:
        raise ValueError("Missing memory measurements")
    if len([key for key in groups if key[0] == "contention"]) != 4:
        raise ValueError("Missing contention measurements")


def charts(summary, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    lookup = {(row["scenario"], row["queue"]): row for row in summary["transfers"]}
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), constrained_layout=True)
    for queue in QUEUES:
        rows = [lookup[(f"scale_{n}p{n}c", queue)] for n in (1, 2, 4, 8, 16)]
        rates = [row["throughput_median"] for row in rows]
        axes[0].errorbar(
            [2, 4, 8, 16, 32],
            rates,
            yerr=[
                [rate - row["throughput_min"] for rate, row in zip(rates, rows, strict=True)],
                [row["throughput_max"] - rate for rate, row in zip(rates, rows, strict=True)],
            ],
            marker="o",
            capsize=3,
            label=queue,
        )
        axes[1].plot(
            [2, 4, 8, 16, 32], [rate / rates[0] for rate in rates], marker="o", label=queue
        )
    axes[0].set_yscale("log")
    axes[0].set_ylabel("Messages / second (log scale)")
    axes[0].set_title("Throughput: trial median and min/max")
    axes[1].set_ylabel("Throughput / 1P1C throughput")
    axes[1].set_title("Scaling relative to one producer + one consumer")
    axes[1].axhline(1, color="grey", linestyle="--", linewidth=1)
    for axis in axes:
        axis.set_xlabel("Total threads (equal producers and consumers)")
        axis.set_xticks([2, 4, 8, 16, 32])
        axis.grid(alpha=0.2)
        axis.legend(fontsize=8)
    fig.savefig(output / "scaling.png", dpi=180)
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), constrained_layout=True)
    for index, queue in enumerate(QUEUES):
        names = ("latency_burst", "latency_sparse", "latency_slow_consumer")
        values = [lookup[(name, queue)]["end_to_end_latency_p99_us"] / 1000 for name in names]
        axes[0].plot(
            [n + index * 0.25 for n in range(3)], values, marker="o", linewidth=0, label=queue
        )
        rows = [row for row in summary["memory"] if row["queue"] == queue and row["depth"] == 10000]
        values = [
            next(row for row in rows if row["measurement"] == kind)["filled_minus_empty_bytes"]
            / 10000
            for kind in ("tracemalloc", "rss")
        ]
        axes[1].plot(
            [n + index * 0.25 for n in range(2)], values, marker="o", linewidth=0, label=queue
        )
    axes[0].set_xticks([0.25, 1.25, 2.25], ["Burst", "Sparse", "Slow consumers"])
    axes[0].set_ylabel("End-to-end p99, milliseconds (log scale)")
    axes[0].set_yscale("log")
    axes[0].set_title("Fixed message count per workload; sampled runs")
    axes[1].set_xticks([0.25, 1.25], ["Python traced", "Process RSS"])
    axes[1].set_yscale("log")
    axes[1].set_ylabel("Incremental bytes / queued reference (log scale)")
    axes[1].set_title("10,000 references; separate memory-only processes")
    for axis in axes:
        axis.legend(fontsize=8)
        axis.grid(axis="y", alpha=0.2)
    fig.savefig(output / "latency_memory.png", dpi=180)
    plt.close(fig)


def report(document, summary):
    meta = document["metadata"]
    rounds = meta["settings"]["rounds"]
    transfers = summary["transfers"]
    lookup = {(row["scenario"], row["queue"]): row for row in transfers}
    lf = lambda name: lookup[(name, "LockFreeQueue")]  # noqa: E731
    rate = lambda name: lf(name)["throughput_median"]  # noqa: E731
    measured = [
        row for row in document["results"] if row["kind"] in ("transfer", "contention", "memory")
    ]
    shortest = min(
        row["seconds_min"] for row in transfers if not row["scenario"].startswith("latency_")
    )
    short_count = sum(
        row["seconds_min"] < 0.1 for row in transfers if not row["scenario"].startswith("latency_")
    )
    lines = [
        "# 并发性能评估报告",
        "",
        f"测量时间：{meta['utc']} 至 {meta['completed_utc']}。基线提交：`{meta['base_commit']}`。",
        "",
        "## 结论",
        "",
        f"当前 Python CAS 队列在默认调度下，1P1C 吞吐中位数为 **{rate('scale_1p1c'):,.0f} 条/秒**，"
        f"4P4C 为 **{rate('scale_4p4c'):,.0f} 条/秒**，16P16C 为 **{rate('scale_16p16c'):,.0f} 条/秒**；"
        f"32 个线程相对 2 个线程的吞吐为 **{rate('scale_16p16c') / rate('scale_1p1c'):.2f} 倍**。",
        "",
        f"同为 4P4C，`SimpleQueue` 和 `Queue` 的吞吐分别为本实现的 "
        f"**{lookup[('scale_4p4c', 'SimpleQueue')]['throughput_median'] / rate('scale_4p4c'):.1f} 倍**和 "
        f"**{lookup[('scale_4p4c', 'Queue')]['throughput_median'] / rate('scale_4p4c'):.1f} 倍**。"
        "本实现适合研究、验证原子 MPMC 算法；当前证据不支持将其作为 CPython 高吞吐队列的首选。",
        "",
        "有优化空间，优先级是减少 Python/FFI 往返、每条消息的原子对象构造和不必要的空轮询。"
        "单纯增加线程不能绕过 GIL；降低内存序强度也不能消除这些主要成本。"
        "本次提交增加评估工具和报告，队列核心源码与基线提交保持一致，未把未经验证的算法改动混入测量。",
        "",
        "## 环境与方法",
        "",
        f"- CPU：{meta['cpu']}；物理核 {meta['physical_cores']}，逻辑 CPU {meta['logical_cpus']}，进程亲和性允许 {meta['affinity_cpus']} 个 CPU。",
        f"- 内存：{meta['memory_bytes'] / 2**30:.2f} GiB；系统：{meta['platform']}。",
        f"- Python：`{meta['python']}`；依赖：`{json.dumps(meta['versions'])}`。",
        f"- 全机 CPU 占用的运行前/后短采样：{meta['system_cpu_percent_before']:.1f}% / {meta['system_cpu_percent_after']:.1f}%。未独占机器、绑定核心或控制电源与睿频。",
        f"- {len(document['pilots'])} 次校准，{len(document['results'])} 次正式测量；其中 {len(measured)} 次消息传递/内存测量全部通过交付校验。",
        "",
        "每个测量点使用全新的子进程，先做 100 次顺序入队/出队预热；不同队列、负载和轮次用固定随机种子打散后串行执行。"
        "线程在计时前创建，准备好后同时释放；计时截至所有工作线程结束，包含结束标记、主线程调度及线程退出，"
        "不包含进程启动、队列构造、预建消息和最终完整性校验。吞吐单位是完成传递的消息/秒，不能将入队和出队重复计数。",
        "",
        f"吞吐场景按每种队列和配置校准消息量，目标每轮 {meta['settings']['target_seconds']} 秒，最多 50 万条；每个点正式测量 {rounds} 轮。"
        "消息量不同，因此跨队列比较的是各自有限批次的平均速率，不能解释成相同积压深度的排队延迟。"
        "预填充 drain 的入队不计时；fill 的出队校验不计时，二者单位分别是入队或出队条数/秒。",
        "",
        f"自动校准不是时长保证：非延迟场景中有 {short_count} 个配置至少一轮短于 100ms，最短 {shortest * 1000:.2f}ms。"
        "CSV/JSON 中保留最短时长、样本量、min/max 和变异系数，避免将小样本中位数当作精确常数。"
        f"{rounds} 轮只刻画本次波动，不提供统计显著性或置信区间保证。",
        "",
        "消息是预建元组，消费者仍执行引用校验与本地列表记录；这些共同的测试开销包含在计时中，"
        "尤其会限制标准库快队列的测得吞吐。逐条校验 ID 覆盖且不重复，并检查每个消费者看到的各生产者顺序；"
        "跨消费者返回日志不能重建全局线性化顺序，原有独立线性化测试负责验证该性质。",
        "",
        "## 吞吐与线程扩展性",
        "",
        f"P 为生产者数量，C 为消费者数量。表内为 {rounds} 轮吞吐中位数；括号为最小—最大值。",
        "",
        "| 配置 | LockFreeQueue（条/秒） | SimpleQueue（条/秒） | Queue（条/秒） |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name in [*(f"scale_{n}p{n}c" for n in (1, 2, 4, 8, 16)), "skew_8p1c", "skew_1p8c"]:
        cells = [
            f"{lookup[(name, queue)]['throughput_median']:,.0f} ({lookup[(name, queue)]['throughput_min']:,.0f}–{lookup[(name, queue)]['throughput_max']:,.0f})"
            for queue in QUEUES
        ]
        lines.append(f"| {name} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "![吞吐与扩展性](performance/scaling.png)",
        "",
        "下面区分排空、填充与同时读写，用于判断成本集中在生产还是消费路径。fill/drain 的数值不能直接当作一整次消息传递吞吐。",
        "",
        "| 场景 | LockFreeQueue | SimpleQueue | Queue |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name in ("fill_1p", "fill_8p", "drain_1c", "drain_8c"):
        lines.append(
            f"| {name} | "
            + " | ".join(f"{lookup[(name, queue)]['throughput_median']:,.0f}" for queue in QUEUES)
            + " |"
        )
    lines += [
        "",
        "## CPU、调度、竞争与分配公平性",
        "",
        "CPU 核占用 = 进程 CPU 时间 / 墙钟时间，1.0 表示约占满一个逻辑核，不是整机 100%。"
        "消费者 Jain 指数在 1/C 到 1 之间，越接近 1 表示这批消息分配越均衡。它受批量长度和调度影响，"
        "不能证明无饥饿或 wait-free。上下文切换计数保留在 CSV/JSON 中，属于 psutil 的平台指标，不等同于 GIL 切换次数。",
        "进程 CPU 时间包含原生库和内核执行；短切换间隔下超过一个核的 CPU 消耗，不代表 Python 字节码获得了相同倍数的并行加速。",
        "",
        "| 队列 / 场景 | CPU 核占用 | CPU μs/条 | 空轮询/条 | Jain | 吞吐 CV |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for queue in QUEUES:
        for name in ("scale_1p1c", "scale_4p4c", "scale_16p16c", "switch_0.1ms"):
            row = lookup[(name, queue)]
            lines.append(
                f"| {queue} / {name} | {row['cpu_cores_median']:.2f} | {row['cpu_us_per_message']:.2f} | {row['empty_polls_per_message']:.2f} | {row['consumer_jain_median']:.3f} | {row['throughput_cv_percent']:.1f}% |"
            )
    lines += [
        "",
        f"将 GIL 请求切换间隔从 5ms 降到 0.1ms 后，本实现 4P4C 的吞吐比例为 **{rate('switch_0.1ms') / rate('scale_4p4c'):.3f}**。"
        "该参数不是精确的调度周期；它改变 Python 解释器与原生调用之间的竞争，不能据此单独归因于 CPU 缓存或 CAS 指令。",
        "",
        "以下 CAS 计数来自独立的插桩进程。包装函数改变了调用成本及线程交错，因此仅用于诊断，"
        "不能与未插桩吞吐直接相除。计数包括结束标记和最后的空状态校验；空轮询与校验失败重试也会产生 load。",
        "",
        "| 场景 | load/消息 | CAS 尝试/消息 | CAS 失败率 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name in ("scale_1p1c", "scale_4p4c", "scale_16p16c", "switch_0.1ms"):
        rows = [
            row
            for row in document["results"]
            if row["kind"] == "contention" and row["scenario"]["name"] == name
        ]
        loads = median([row["atomic_counters"]["loads"] / row["messages"] for row in rows])
        attempts = median(
            [row["atomic_counters"]["cas_attempts"] / row["messages"] for row in rows]
        )
        failures = median(
            [
                row["atomic_counters"]["cas_failures"]
                / row["atomic_counters"]["cas_attempts"]
                * 100
                for row in rows
            ]
        )
        lines.append(f"| {name} | {loads:.2f} | {attempts:.2f} | {failures:.2f}% |")
    lines += [
        "",
        "## 调用延迟与端到端延迟",
        "",
        "端到端延迟从生产者调用 put 前开始，到消费者 get 返回后结束，包含入队执行、队列等待及出队执行，"
        "不是只测队列驻留时间。put 延迟也包含为采样消息添加时间戳的成本；get 延迟只统计采样到的成功调用。",
        "",
        "burst 为 4P4C 共 4,096 条消息，每个生产者每 4 条采样一次；sparse 为 1P1C 共 400 条，"
        "每次生产后请求 sleep(1ms)；slow_consumer 为 4P4C 共 1,024 条，每次消费后请求 sleep(1ms)。"
        "后两者逐条采样。sleep 的实际长度依赖 Windows 调度，稀疏场景没有强制达到固定到达率。"
        "三个队列在同一延迟场景使用相同消息量，避免将不同积压深度的 p99 直接比较。",
        "",
        "百分位使用 nearest-rank。表中是**各轮百分位的中位数**，不是合并原始样本后的百分位。"
        "CSV 中的 max_us 则取所有轮次观测到的最大值。"
        "这是有限批次、生产者闭环生成的延迟，存在 coordinated omission：生产者阻塞/变慢时不会补记应到达请求，"
        "因此不能当作固定外部到达率下的服务 SLA，也不能据此预测长期 p99.9。",
        "",
        "| 队列 / 场景 | put p99 μs | get p99 μs | 端到端 p50 ms | p95 ms | p99 ms | 端到端样本总数 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name in ("latency_burst", "latency_sparse", "latency_slow_consumer"):
        for queue in QUEUES:
            row = lookup[(name, queue)]
            lines.append(
                f"| {queue} / {name} | {row['put_latency_p99_us']:.2f} | {row['get_latency_p99_us']:.2f} | {row['end_to_end_latency_p50_us'] / 1000:.3f} | {row['end_to_end_latency_p95_us'] / 1000:.3f} | {row['end_to_end_latency_p99_us'] / 1000:.3f} | {row['end_to_end_latency_samples']} |"
            )
    lines += [
        "",
        "![延迟与内存](performance/latency_memory.png)",
        "",
        "## 对象大小、空轮询与可直接应用的配置优化",
        "",
        f"在 4P4C 中，消息携带共享 64KiB bytes 引用时，本实现吞吐为 {rate('payload_64KiB'):,.0f} 条/秒，"
        f"相对空 bytes 引用为 {rate('payload_64KiB') / rate('scale_4p4c'):.2f} 倍。"
        "该实验没有复制或序列化 64KiB 数据，只能评估引用传递，不能换算为网络或内存拷贝 GB/s。"
        "差异也包含独立校准、批量长度与调度波动，不表示对象越大越快或越慢。",
        "",
        f"消费者遇到 Empty 后，将 sleep(0) 改为请求 sleep(0.5ms)，本实现吞吐从 {rate('scale_4p4c'):,.0f} "
        f"变为 {rate('backoff_0.5ms'):,.0f} 条/秒（{rate('backoff_0.5ms') / rate('scale_4p4c'):.2f} 倍）；"
        f"空轮询从 {lf('scale_4p4c')['empty_polls_per_message']:.2f} 变为 {lf('backoff_0.5ms')['empty_polls_per_message']:.2f} 次/条。"
        "退避位于调用方，不修改队列算法；可能提高空闲 CPU 效率，也会增加消息唤醒等待，不能据吞吐试验宣称尾延迟改善。",
        "两种设置分别校准了批量长度，观察到的速率差异尚未排除批量大小的影响；上线前应在固定消息量和真实到达率下复测。",
        "",
        "## 内存与回收",
        "",
        "各场景只存储同一个既有对象的引用。RSS 与 tracemalloc 使用不同子进程测量，避免 tracemalloc 自身的追踪结构污染 RSS。"
        "在空队列、填充后、排空后、销毁后分别 gc.collect 并取样。tracemalloc 只覆盖可追踪 Python 分配；"
        "RSS 包含原生内存、解释器及分配器保留页，不能简单相减来精确计算原生节点大小。",
        "",
        "| 队列 | 积压条数 | 指标 | 填充增量 MiB | 排空后残留 KiB | 销毁后相对初始 KiB |",
        "| --- | ---: | --- | ---: | ---: | ---: |",
    ]
    for row in summary["memory"]:
        lines.append(
            f"| {row['queue']} | {row['depth']:,} | {row['measurement']} | {row['filled_minus_empty_bytes'] / 2**20:.3f} | {row['drained_minus_empty_bytes'] / 1024:.2f} | {row['destroyed_minus_before_bytes'] / 1024:.2f} |"
        )
    retained = next(
        row
        for row in summary["memory"]
        if row["queue"] == "LockFreeQueue"
        and row["depth"] == 10000
        and row["measurement"] == "tracemalloc"
    )
    lines += [
        "",
        f"本实现排空 10,000 条消息后，节点注册表长度为 1，但字典自身仍保留约 "
        f"**{retained['registry_bytes_after_drain'] / 1024:.1f} KiB** 的容量。"
        "节点/载荷释放与字典容量收缩、RSS 归还操作系统是不同问题；RSS 残留不能直接判定为节点泄漏。"
        "当前队列不提供背压，生产长期快于消费时内存仍随积压增长。",
        "",
        "## 原子原语与函数分析",
        "",
        f"以下为顺序微基准，每轮 5,000 次、{rounds} 轮中位数；含 Python 调用开销，noop 用于显示计时循环成本，"
        "没有从其他行中强行扣除。构造包括返回临时对象的释放；成功 CAS 使用 0→0，测的是成功路径，不模拟真实缓存行竞争。",
        "",
        "| 操作 | μs/次 |",
        "| --- | ---: |",
    ]
    primitives = defaultdict(list)
    for row in document["results"]:
        if row["kind"] == "primitives":
            for op in row["operations"]:
                primitives[op["operation"]].append(op["ns_per_call"])
    for name, values in primitives.items():
        lines.append(f"| {name} | {median(values) / 1000:.3f} |")
    profile = next(row for row in document["results"] if row["kind"] == "profile")
    lines += [
        "",
        "独立 cProfile 运行 1,000 次顺序 put/get，按累计时间排序的前 12 项如下。累计时间相互包含，"
        "不能相加为百分比；插桩时间不能视为未插桩吞吐，也不能从单线程 profile 判断硬件缓存争用。",
        "",
        "| 函数 | 调用数 | 自身 ms | 累计 ms |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in profile["top_cumulative"][:12]:
        lines.append(
            f"| `{row['function']}` | {row['calls']:,} | {row['self_seconds'] * 1000:.2f} | {row['cumulative_seconds'] * 1000:.2f} |"
        )
    lines += [
        "",
        "## 优化空间与优先级",
        "",
        "| 优先级 | 建议 | 依据与可能收益 | 代价 / 验证要求 |",
        "| --- | --- | --- | --- |",
        "| P0：选型 | 普通 CPython 业务优先评估 SimpleQueue；需要阻塞、容量限制或任务计数时选 Queue | 本次各队列实测提供直接对照；原生标准库路径减少 Python 包装成本 | 不保留当前算法级无锁目标，需按业务语义选择 |",
        "| P1：应用配置 | 限制线程数，消费者为空时采用有上限的自适应退避 | 线程扩展曲线与 backoff 对照可直接复验，不需要改核心算法 | 退避增大唤醒延迟，需补测目标流量的开放环延迟 |",
        "| P1：原生实现 | 将完整入队/出队循环和节点布局迁移到 C/C++/Rust 扩展，减少每次操作的 Python/FFI 往返 | 原语成本与 profile 表明不能只优化一条 CAS；当前成功路径至少 7 次 load、3 次 CAS及节点构造 | 高工作量；需设计 GIL 边界、PyObject 引用生命周期、异常路径和安全回收，不能直接宣称端到端 lock-free |",
        "| P2：分配 | 探索紧凑节点、原子操作元数据复用、预分配节点池 | 每条消息构造 AtomicUInt64，内存测量显示对象图开销；可先测原型 | 不依赖 atomics 私有接口；回收复用必须保留不重用 token/代数或引入可靠 ABA 防护，需重跑指定交错和线性化测试 |",
        "| P2：容量治理 | 增加明确的背压策略；在业务确认无并发访问的生命周期边界重建空队列 | 无界队列会积压，字典在排空后保留容量 | 有界队列改变接口/算法；不能在并发读写期间直接 clear 或替换注册表 |",
        "| P3：批处理或分片 | 根据业务允许的顺序范围减少每消息协调次数 | 有机会摊薄 Python 调用成本；本次没有给出实现或量化收益 | 批量原子语义、全局 FIFO 与每分片 FIFO 不同，必须先定义需求再实现 |",
        "| P3：内存序/缓存布局 | 仅在原生实现确定后评估 acquire/release、缓存行布局 | 目前缺少硬件计数器证据，Python 包装成本可见 | 弱内存序需要完整发布/读取证明和跨架构测试，不能仅凭 x64 测试通过就替换 SEQ_CST |",
        "",
        "除消费者退避和线程配置对照外，上述实现优化是候选方向，尚未完成前后对照验证，不能承诺具体加速比例。"
        "Python GIL、内存分配器及 GC 仍可能阻塞；队列算法使用原生 CAS 不等于整个 Python 调用链严格无锁。",
        "",
        "## 数据完整性与复现",
        "",
        "```powershell",
        'python -m pip install -e ".[test,benchmark]"',
        "python -m pytest -q",
        "python -m benchmarks.evaluate --output docs/performance/raw.json --rounds 3 --target-seconds 0.4",
        "python -m benchmarks.report --input docs/performance/raw.json",
        "```",
        "",
        "仅复测部分吞吐场景可使用 `--only scale_1p1c,scale_4p4c --output docs/performance/subset.json`。"
        "完整报告生成器要求完整场景集；子集文件用于单独分析，不替换完整报告数据。"
        "子进程默认硬超时 90 秒；超时或异常写入结果文件，报告生成器拒绝发布未完成的测量。",
        "",
        "- [原始测量及环境、源码 SHA-256](performance/raw.json)：包含校准、每轮样本量、CPU、GC、消费者计数、CAS 诊断、内存快照和 profile。",
        "- [汇总 JSON](performance/summary.json)、[吞吐及延迟 CSV](performance/transfers.csv)、[内存 CSV](performance/memory.csv)。",
        "- [评估脚本](../benchmarks/evaluate.py)、[报告生成脚本](../benchmarks/report.py)。",
        "- [本次测试与数据核对记录](performance/validation.txt)：84 项测试通过，并核对了被测源码哈希、场景完整性和报告链接。",
        "",
        "本报告是当前单机、单个 CPython 版本的有限负载评估，不覆盖多进程、无 GIL Python、长期稳态、"
        "真实网络 I/O、序列化成本、固定外部到达率、NUMA 或硬实时场景。需要生产容量结论时，应增加独占机器多轮测试、"
        "开放环速率扫描与过载恢复、长时间 RSS 趋势和目标业务任务处理成本。",
        "",
    ]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="docs/performance/raw.json")
    parser.add_argument("--output", default="docs/performance.md")
    args = parser.parse_args()
    raw = Path(args.input)
    document = json.loads(raw.read_text(encoding="utf-8"))
    summary = summarize(document)
    require_full_suite(document, summary)
    output = Path(args.output)
    assets = output.parent / "performance"
    assets.mkdir(parents=True, exist_ok=True)
    (assets / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_csv(assets / "transfers.csv", summary["transfers"])
    write_csv(assets / "memory.csv", summary["memory"])
    charts(summary, assets)
    output.write_text(report(document, summary), encoding="utf-8")
    print(f"Generated {output}, JSON/CSV summaries, and two PNG figures")


if __name__ == "__main__":
    main()
