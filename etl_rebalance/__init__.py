"""ETL 分区再平衡规划器。

模块组成::

    model   - 领域模型：worker、分区、交接状态、迁移计划
    planner - 纯函数式规划器，输出确定性的最小搬迁计划
    handoff - lease 租约、checkpoint 存储与交接/回滚执行器
"""

from .handoff import Checkpoint, CheckpointStore, HandoffManager, Lease, LeaseRegistry
from .model import (
    HandoffPhase,
    HandoffState,
    Migration,
    MigrationReason,
    Partition,
    PartitionState,
    Plan,
    Rollback,
    Worker,
    WorkerStatus,
)
from .planner import (
    CapacityExceededError,
    InvalidClusterError,
    RebalanceError,
    RebalancePlanner,
)

__all__ = [
    "CapacityExceededError",
    "Checkpoint",
    "CheckpointStore",
    "HandoffManager",
    "HandoffPhase",
    "HandoffState",
    "InvalidClusterError",
    "Lease",
    "LeaseRegistry",
    "Migration",
    "MigrationReason",
    "Partition",
    "PartitionState",
    "Plan",
    "RebalanceError",
    "RebalancePlanner",
    "Rollback",
    "Worker",
    "WorkerStatus",
]
