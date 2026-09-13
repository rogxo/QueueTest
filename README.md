# Python 原子 MPMC 队列

使用 Python 实现 Michael–Scott 链式 FIFO 队列，通过 `atomics` / `patomic`
调用硬件级 64 位 CAS（compare-and-swap）。支持同一进程内多生产者、多消费者
并发入队和出队，无容量上限，允许保存任意 Python 对象，包括 `None`。

**无锁的范围是队列的链接、头指针和尾指针更新算法。整个 Python 调用链不具备严格的
lock-free 进度保证**：它仍使用 CPython 的 GIL、对象分配、引用计数和垃圾回收。
如果要求线程暂停时完全不影响其他线程执行 Python，或要求 CPU 多核线性扩展，
需要使用释放 GIL 的原生扩展，并为内存分配和节点回收单独设计进度保证。

## 安装与运行

要求 Python 3.10+ 的标准 CPython 构建，以及 `atomics` 支持的 64 位原子操作。
已在 Windows x64 / CPython 3.12 上验证；其他平台需要运行本项目测试。
初始化时拒绝 PyPy、free-threaded CPython 和缺少所需原子操作的平台，不退化为加锁实现。

```powershell
python -m pip install -e ".[test]"
python -m pytest -q
python examples/producer_consumer.py
```

```python
from lockfree_queue import Empty, LockFreeQueue

q = LockFreeQueue[object]()
q.put("hello")
q.put(None)

assert q.get() == "hello"
assert q.try_get() == (True, None)
assert q.try_get() == (False, None)

try:
    q.get_nowait()
except Empty:
    pass
```

## 接口与并发语义

| 接口 | 行为 |
| --- | --- |
| `put(item)` / `put_nowait(item)` | 原子入队，返回 `None`；竞争时重试 CAS |
| `get()` / `get_nowait()` | 原子出队；为空时立即抛出标准库 `queue.Empty` |
| `try_get()` | 返回 `(True, item)` 或 `(False, None)` |
| `empty()` | 返回可线性化的空状态快照；返回后状态可能立即改变 |

这些接口不等待数据到达，不提供 `block` / `timeout` 参数。`get()` 与标准库
`queue.Queue.get()` 的默认阻塞语义不同。消费者需要等待时，在调用方安排退避、调度或
结束标记；示例使用短暂休眠，避免空队列轮询持续占用 CPU。

FIFO 按入队成功链接的顺序定义。同一生产者顺序提交的消息保持顺序；不同生产者
重叠调用时，以 CAS 实际成功顺序为准。多个消费者返回后的日志顺序可能不同于
实际出队顺序，因为线程可能在完成 CAS 后暂停。单次操作可线性化，但多个操作的组合
不是事务：不要先检查 `empty()` 再假设后续 `get()` 一定成功。

不提供并发精确的 `qsize()` / `len()`，避免把单独计数器误称为与入队、出队共同原子
更新。也不提供 `peek`、批量原子操作、任务计数或跨进程共享。队列转移对象引用，
不复制对象，也不保护对象自身的并发修改。

## 实现与正确性依据

- **真正的 CAS**：链接、头和尾使用 `atomics` 的原生强 CAS，采用默认的顺序一致性
  内存序（`SEQ_CST`）。没有用 Python 的“比较后赋值”模拟 CAS，没有队列互斥锁。
- **入队线性化点**：成功将尾节点的 `next` 从 `0` 改为新节点编号的 CAS。
  随后的尾指针推进仅用于维护状态；其他生产者或消费者可以帮助完成。
- **出队线性化点**：成功把头指针从旧哨兵推进到下一节点的 CAS。失败线程重新读取
  状态，只有成功线程返回该元素。因此不会由两个消费者取走同一队列位置。
- **ABA 防护**：每个实例使用单调递增、从不重用的 64 位节点编号。编号由标准 CPython
  中单次 `next(itertools.count)` 分配，Python 整数不回绕。超过 `2**64 - 1` 时拒绝入队，
  不能将已释放的旧编号重新解释成新节点。
- **节点生命周期**：字典保存“编号 → 节点”的强引用，依赖 GIL 保证单个内置字典操作
  的安全性。原子指针读取后，节点可能已从字典删除，此时重试；成功取到节点后，局部
  强引用会保护它，直到本次访问结束。没有把整数地址强制转换成 Python 对象，也不需要
  自行释放原始指针。每次成功出队移除旧哨兵，并清空新哨兵的 payload，避免空队列
  长期保留最后一个元素。暂停中的线程仅保留其正在处理的节点，不保留整条历史链。

在原子原语及节点访问能继续执行的算法模型内，CAS 失败意味着其他线程取得了进展，
因此属于 lock-free，而不是每个线程都保证有限步完成的 wait-free。上述论证不覆盖
解释器调度、内存分配耗尽或线程被异步强制终止等情况；不能安全地通过强制终止线程
来取消一次正在执行的队列操作。节点分配失败且尚未发布时，队列保持原状态。

算法参考：[Michael & Scott, Simple, Fast, and Practical Non-Blocking and Blocking
Concurrent Queue Algorithms](https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html)。
这里通过不重用的编号和 Python 强引用替代原生指针版本的内存回收方案。

## 测试

测试包括：

- 空队列、别名接口、FIFO、`None` 和任意对象引用、独立实例。
- 随机顺序操作与标准库 `deque` 模型逐步比对。
- SPSC、MPSC、SPMC、MPMC，最多 16 个生产者与 16 个消费者同时运行；检查每条消息
  恰好一次交付、各生产者的顺序、竞争后队列可继续使用。
- 16 个消费者争抢唯一元素，以及原生 CAS 的单一成功者。
- 在链接成功但尾指针未推进、头 CAS 前后、节点查询前后主动暂停线程，验证帮助推进、
  失败重试以及节点回收时仍可安全访问。
- 对短并发历史搜索合法的 FIFO 串行排列，同时保留非重叠操作的真实先后约束，覆盖
  `put`、`get`、`try_get` 和 `empty`。另有非法历史验证检查器能发现乱序、重复和错误空状态。
- 弱引用检查历史节点和已消费 payload 的回收，编号耗尽、分配失败和运行环境限制。

线程测试设置屏障超时和有限等待，并把后台异常传回测试线程。每个压力场景默认最多
等待 300 秒，可用 `QUEUE_STRESS_TIMEOUT` 调整。测试中的屏障和事件
仅控制调度，不属于队列实现。压力测试是验证手段，不等同于对所有线程交错的形式化证明。

扩大每个生产者的消息数量（耗时随竞争程度上升）：

```powershell
$env:QUEUE_STRESS_ITEMS = "1000"
python -m pytest -q -m stress
Remove-Item Env:QUEUE_STRESS_ITEMS
```

Linux / macOS shell：

```sh
QUEUE_STRESS_ITEMS=1000 python -m pytest -q -m stress
```

## 性能测量

```powershell
python benchmarks/throughput.py --producers 4 --consumers 4 --items 1000 --rounds 3
```

脚本比较本实现、`queue.SimpleQueue` 和 `queue.Queue`，输出环境、耗时、消息吞吐量和
空轮询次数，并检查所有消息没有丢失或重复。计时包括一条消息的入队和出队，
`messages_per_second` 不是把这两次操作重复计数后的 ops/s。

本实现优先展示和验证原子 MPMC 算法；Python 到原生函数的多次调用、每节点原子对象
分配以及 CAS 竞争都有成本。**支持高并发正确性不意味着优于标准库队列的吞吐量**。
在普通 CPython 业务中，`queue.SimpleQueue` 通常更快；是否采用本实现，应以实际负载
的基准结果为依据，不能仅凭“无锁”名称判断性能。

本机实测（Windows x64 / CPython 3.12.10，4 生产者、4 消费者，每轮 4,000 条消息，
3 轮的吞吐量中位数）：

| 队列 | 消息/秒 |
| --- | ---: |
| `LockFreeQueue` | 5,321 |
| `queue.SimpleQueue` | 6,728,343 |
| `queue.Queue` | 1,010,611 |

标准库队列的本次短负载耗时很小，结果对调度和计时噪声敏感，不能外推为持续吞吐量。
所有轮次均验证消息交付。完整测试以 `QUEUE_STRESS_ITEMS=1000` 运行，结果为
**64 passed in 185.30s**；五种压力配置合计交付 34,000 条消息。压力测试还将 Python
线程切换间隔降到 0.1ms 以增加交错，因此耗时不能直接作为默认调度下的性能基准。

## 文件

```text
src/lockfree_queue/_atomic.py   64 位原生 CAS 封装
src/lockfree_queue/_queue.py    队列实现
tests/                         功能、并发、指定交错、线性化与回收测试
examples/producer_consumer.py   多生产者、多消费者示例
benchmarks/throughput.py        带交付验证的吞吐量对比
```
