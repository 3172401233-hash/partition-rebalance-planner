"""再平衡规划器的领域模型。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class WorkerStatus(str, Enum):
    """worker 生命周期状态。"""

    ONLINE = "online"
    DRAINING = "draining"
    OFFLINE = "offline"


class PartitionState(str, Enum):
    """分区整体状态。"""

    STABLE = "stable"
    MIGRATING = "migrating"


class HandoffPhase(str, Enum):
    """交接窗口内部的阶段。"""

    PREPARE = "prepare"
    LEASE_TRANSFERRED = "lease_transferred"
    COMMITTED = "committed"
    INTERRUPTED = "interrupted"


class MigrationReason(str, Enum):
    """每条迁移的触发原因，用于审计与测试断言。"""

    WORKER_OFFLINE = "worker_offline"
    WORKER_DRAINING = "worker_draining"
    OVERLOAD = "overload"
    RETARGET = "retarget"


@dataclass(frozen=True)
class Worker:
    """一个可承载分区的 worker。

    ``capacity`` 是可承载吞吐上限；``cooldown_until`` 为冷却截止时间戳，
    处于冷却中的 worker 不能接收新分区，但仍可继续运行已持有分区。
    """

    worker_id: str
    capacity: float
    status: WorkerStatus = WorkerStatus.ONLINE
    cooldown_until: int = 0

    def can_receive(self, now: int) -> bool:
        return (
            self.status == WorkerStatus.ONLINE
            and self.cooldown_until <= now
        )


@dataclass(frozen=True)
class HandoffState:
    """进行中的交接状态。

    ``source_owner`` 为当前（旧）owner，``target_owner`` 为接管方；
    ``lease_committed`` 为 True 表示 lease 已移交，此时只有 target 可提交。
    """

    partition_id: str
    source_owner: str
    target_owner: str
    phase: HandoffPhase = HandoffPhase.PREPARE
    lease_committed: bool = False
    started_at: int = 0


@dataclass(frozen=True)
class Partition:
    """一个 Kafka/ETL 分区。

    ``owner`` 是当前持有 lease 的 worker；处于迁移中时由 ``handoff``
    描述交接双方。``affinity`` 为偏好 worker 列表（顺序即优先级）。
    ``cooldown_until`` 是分区级冷却：该分区在此之前不允许再次被搬迁。
    """

    partition_id: str
    throughput: float
    owner: str
    affinity: tuple[str, ...] = ()
    state: PartitionState = PartitionState.STABLE
    handoff: HandoffState | None = None
    cooldown_until: int = 0
    lease_epoch: int = 0

    def in_cooldown(self, now: int) -> bool:
        return self.cooldown_until > now


@dataclass(frozen=True)
class Migration:
    """规划器产出的一条原子搬迁指令。"""

    partition_id: str
    source_owner: str
    target_owner: str
    throughput: float
    reason: MigrationReason
    lease_epoch: int
    resume_from_checkpoint: int
    seq: int

    def key(self) -> tuple[str, str, str]:
        return (self.partition_id, self.source_owner, self.target_owner)


@dataclass(frozen=True)
class Rollback:
    """目标不可用时，对进行中交接执行的回滚指令。"""

    partition_id: str
    restore_owner: str
    lease_epoch: int
    last_checkpoint: int
    seq: int


@dataclass(frozen=True)
class Plan:
    """一次再平衡规划的完整结果。

    ``migrations`` 与 ``rollbacks`` 均按确定顺序排序，重复规划产生
    完全相同的迁移集合。
    """

    migrations: tuple[Migration, ...] = ()
    rollbacks: tuple[Rollback, ...] = ()

    def migration_keys(self) -> frozenset[tuple[str, str, str]]:
        return frozenset(m.key() for m in self.migrations)

    def is_empty(self) -> bool:
        return not self.migrations and not self.rollbacks

    def find(self, partition_id: str) -> Migration | None:
        for migration in self.migrations:
            if migration.partition_id == partition_id:
                return migration
        return None
