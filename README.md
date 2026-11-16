# partition-rebalance-planner

ETL/Kafka 分区再平衡规划器。针对集群扩容后“一次性均匀搬迁导致新旧 worker
同时成功提交、checkpoint 顺序混乱”的问题，库内同时提供两部分能力：

- **规划器**（`etl_rebalance.planner`）：输入分区吞吐、当前 owner、worker
  容量、亲和关系、冷却时间与迁移中状态，输出搬迁次数最少且确定可复现的计划。
- **交接执行器**（`etl_rebalance.handoff`）：用 lease + epoch fencing 保证
  交接窗口内只有一个可提交 owner，新 owner 从旧 owner 的最后 checkpoint 续传，
  中断时可安全回滚。

## 安装与测试

仅依赖 Python 3.10+ 标准库，无第三方依赖。

```bash
python -m unittest
```

## 快速开始

```python
from etl_rebalance import (
    HandoffManager, LeaseRegistry, CheckpointStore,
    Partition, RebalancePlanner, Worker,
)

workers = [Worker("w1", capacity=100), Worker("w2", capacity=100)]
partitions = [
    Partition("p-hot", throughput=70, owner="w1"),
    Partition("p-a", throughput=40, owner="w1"),
    Partition("p-b", throughput=30, owner="w2"),
]

registry, store = LeaseRegistry(), CheckpointStore()
manager = HandoffManager(registry, store)
for p in partitions:
    manager.bootstrap(p.partition_id, p.owner)

# 1. 规划：w1 过载，只搬一个最大分区，迁移数最小
last_cp = {p.partition_id: store.latest_offset(p.partition_id)
           for p in partitions}
plan = RebalancePlanner(workers, partitions).plan(last_cp)

# 2. 执行交接：lease 先撤旧再授新，新 owner 从最后 checkpoint + 1 提交
for migration in plan.migrations:
    handoff = manager.begin(migration)
    manager.complete(handoff, migration.resume_from_checkpoint + 1)
```

## 规划规则

规划器是纯函数式的，不修改任何输入对象。只有以下情况才会产生搬迁，因此
迁移集合天然最小：

1. **worker 下线/排水**：`OFFLINE`、`DRAINING` worker 持有的稳定分区必须疏散。
2. **过载疏散**：在线 worker 负载超过容量时，按吞吐从大到小挑选分区搬走，
   优先搬走最大分区以最小化搬迁次数；疏散会多轮进行，直到所有在线 worker
   都不再过载。
3. **进行中交接失效**：迁移目标已下线时，按源 worker 健康程度决定回滚或
   重新选址（retarget）。

其余规则：

- **亲和关系**只影响目标 worker 的选择顺序，不会单独触发搬迁。
- **冷却时间**分两级：worker 冷却中不能接收新分区；分区冷却中不能被再次
  搬迁（防止热点分区来回抖动）。
- **容量不足**时抛出 `CapacityExceededError`，包含总容量不足与无合法目标
  两种情形。
- **确定性**：所有内部遍历按 id 排序，输出的 `Migration`/`Rollback` 带稳定
  `seq`；无论 worker、分区以什么顺序传入，重复规划得到完全相同的迁移集合。

## Lease 规则

每个分区在 `LeaseRegistry` 中只有一条当前 lease：`(partition, owner, epoch)`。

- 任意时刻**有且仅有一个 owner 持有可提交 lease**。
- 每次 lease 变更 epoch 单调递增；旧 owner 拿到的过期 epoch 会被 fencing，
   其提交直接抛 `LeaseError`，因此新旧 worker 不可能同时成功提交。
- `CheckpointStore.commit` 先校验 lease，再校验位点：新位点必须严格大于该
   分区的最后提交位点，否则抛 `CheckpointError`，从机制上杜绝 checkpoint
   回退和乱序。

## 交接（Handoff）规则

一条迁移的完整状态机（`HandoffState.phase`）：

1. `PREPARE`：`begin()` 校验源 lease 与迁移指令一致；此时旧 owner 仍可提交，
   新 owner 不能提交。
2. `LEASE_TRANSFERRED`：`transfer_lease()` **先撤销旧 owner lease，再授予新
   owner lease**，两步之间没有任何 owner 可提交，不存在双提交窗口。
3. `COMMITTED`：新 owner 用新 epoch 提交第一个 checkpoint，位点必须大于旧
   owner 的最后提交（即 `Migration.resume_from_checkpoint`），交接完成并
   清除 handoff 记录。

`complete()` 是步骤 2、3 的便捷合并；`commit_target()` 在 lease 未移交时
会被拒绝。

## 回滚规则

`interrupt()` 处理交接中断：

- 若新 owner **尚未提交**（`PREPARE` 或 `LEASE_TRANSFERRED`）：撤销新 owner
  lease，把 lease 恢复给旧 owner，并分配**更高的新 epoch** 做 fencing；旧
  owner 随后从自己的最后 checkpoint 继续，checkpoint 链不出现空洞或重复。
- 若新 owner **已经提交成功**（`COMMITTED`）：数据已前进，回滚被拒绝，
  只能基于新 owner 再发起一次迁移，避免位点倒退。
- 规划层面对“目标已下线”的进行中交接会产出 `Rollback` 指令；若源 worker
  同时处于下线/排水状态，则跳过回滚、直接 retarget 到健康 worker。

## 模块结构

| 路径 | 职责 |
| --- | --- |
| `etl_rebalance/model.py` | worker、分区、交接状态、迁移/回滚指令等领域模型 |
| `etl_rebalance/planner.py` | 纯函数式最小搬迁规划器，含引用校验与容量预检 |
| `etl_rebalance/handoff.py` | lease 注册表、checkpoint 存储、交接与回滚状态机 |
| `tests/` | 30 个 unittest，覆盖热点、上下线、容量不足、重复规划、交接中断、输入顺序变化等场景 |

## 覆盖的测试场景

- 热点分区过载时只搬迁最少数量的分区，并触发多轮连锁疏散
- worker 上线均衡、下线/排水疏散、worker 与分区两级冷却
- 全局容量不足、无合法目标、非法 owner/亲和引用
- 重复规划与输入顺序变化产生同一迁移集合、稳定 `seq`
- 进行中交接不被重复规划、中断回滚、源排水时 retarget
- 交接窗口单 committable owner、epoch fencing、checkpoint 单调续传
- 提交后拒绝回滚、未提交中断恢复旧 owner
