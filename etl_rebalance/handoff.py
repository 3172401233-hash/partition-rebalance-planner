"""lease 租约、checkpoint 与交接执行器。

交接窗口内的不变量：

1. 任意时刻一个分区只有一个可提交 owner；lease 通过 epoch 做 fencing，
   旧 owner 在 lease 被撤销后提交会被拒绝，因此新旧 worker 不可能同时
   成功提交。
2. lease 先撤销旧 owner、再授予新 owner，两次动作之间没有任何 owner
   可提交，避免双写。
3. 新 owner 的第一个 checkpoint 必须严格大于旧 owner 的最后提交位点，
   保证 checkpoint 顺序单调并从最后提交继续。
4. 交接中断且新 owner 尚未提交时，回滚到旧 owner 并分配新 epoch；
   一旦新 owner 已提交，交接即不可逆，只能走新的迁移。
"""

from __future__ import annotations

from dataclasses import dataclass

from .model import HandoffPhase, HandoffState, Migration


class LeaseError(Exception):
    """lease 冲突或过期提交。"""


class CheckpointError(Exception):
    """checkpoint 位点非法（回退或重复）。"""


@dataclass(frozen=True)
class Lease:
    """一个分区当前的提交租约。"""

    partition_id: str
    owner: str
    epoch: int


@dataclass(frozen=True)
class Checkpoint:
    """一次成功提交的 checkpoint。"""

    partition_id: str
    offset: int
    owner: str
    epoch: int


class LeaseRegistry:
    """分区 -> 当前 lease 的注册表，负责 epoch fencing。"""

    def __init__(self) -> None:
        self._leases: dict[str, Lease] = {}

    def grant(self, partition_id: str, owner: str, epoch: int) -> Lease:
        existing = self._leases.get(partition_id)
        if existing is not None and epoch <= existing.epoch:
            raise LeaseError(
                f"分区 {partition_id} 的 epoch {epoch} 不大于当前 "
                f"{existing.epoch}，拒绝授予"
            )
        lease = Lease(partition_id=partition_id, owner=owner, epoch=epoch)
        self._leases[partition_id] = lease
        return lease

    def revoke(self, partition_id: str) -> None:
        self._leases.pop(partition_id, None)

    def current(self, partition_id: str) -> Lease | None:
        return self._leases.get(partition_id)

    def validate(self, partition_id: str, owner: str, epoch: int) -> Lease:
        lease = self._leases.get(partition_id)
        if lease is None:
            raise LeaseError(f"分区 {partition_id} 当前没有有效 lease")
        if lease.owner != owner or lease.epoch != epoch:
            raise LeaseError(
                f"分区 {partition_id} 的 lease 属于 {lease.owner}@"
                f"{lease.epoch}，拒绝 {owner}@{epoch} 的提交"
            )
        return lease


class CheckpointStore:
    """按分区保存最后提交位点，强制单调递增。"""

    def __init__(self) -> None:
        self._latest: dict[str, Checkpoint] = {}
        self._history: dict[str, list[Checkpoint]] = {}

    def commit(
        self,
        partition_id: str,
        offset: int,
        owner: str,
        epoch: int,
        registry: LeaseRegistry,
    ) -> Checkpoint:
        registry.validate(partition_id, owner, epoch)
        current = self._latest.get(partition_id)
        if current is not None and offset <= current.offset:
            raise CheckpointError(
                f"分区 {partition_id} 新位点 {offset} 必须大于最后提交 "
                f"{current.offset}"
            )
        checkpoint = Checkpoint(
            partition_id=partition_id,
            offset=offset,
            owner=owner,
            epoch=epoch,
        )
        self._latest[partition_id] = checkpoint
        self._history.setdefault(partition_id, []).append(checkpoint)
        return checkpoint

    def latest_offset(self, partition_id: str) -> int:
        checkpoint = self._latest.get(partition_id)
        return checkpoint.offset if checkpoint is not None else 0

    def latest(self, partition_id: str) -> Checkpoint | None:
        return self._latest.get(partition_id)

    def history(self, partition_id: str) -> tuple[Checkpoint, ...]:
        return tuple(self._history.get(partition_id, ()))


class HandoffManager:
    """执行迁移计划的交接状态机。

    用法::

        manager = HandoffManager(registry, store)
        handoff = manager.begin(migration)
        manager.transfer_lease(handoff)   # 旧 lease 撤销、新 lease 生效
        manager.complete(handoff, new_offset)
    """

    def __init__(
        self,
        registry: LeaseRegistry | None = None,
        store: CheckpointStore | None = None,
    ) -> None:
        self.registry = registry or LeaseRegistry()
        self.store = store or CheckpointStore()
        self._handoffs: dict[str, HandoffState] = {}

    def bootstrap(self, partition_id: str, owner: str, epoch: int = 0) -> Lease:
        """为稳定分区建立初始 lease（幂等）。"""
        existing = self.registry.current(partition_id)
        if existing is not None:
            return existing
        return self.registry.grant(partition_id, owner, epoch)

    def begin(self, migration: Migration, now: int = 0) -> HandoffState:
        partition_id = migration.partition_id
        if partition_id in self._handoffs:
            raise LeaseError(f"分区 {partition_id} 已有进行中的交接")
        lease = self.registry.current(partition_id)
        expected_epoch = max(migration.lease_epoch - 1, 0)
        if lease is None:
            self.bootstrap(partition_id, migration.source_owner, expected_epoch)
        elif lease.owner != migration.source_owner or lease.epoch != expected_epoch:
            raise LeaseError(
                f"迁移 {partition_id} 与当前 lease "
                f"{lease.owner}@{lease.epoch} 不匹配"
            )
        handoff = HandoffState(
            partition_id=partition_id,
            source_owner=migration.source_owner,
            target_owner=migration.target_owner,
            phase=HandoffPhase.PREPARE,
            lease_committed=False,
            started_at=now,
        )
        self._handoffs[partition_id] = handoff
        return handoff

    def transfer_lease(self, handoff: HandoffState) -> HandoffState:
        """原子地移交 lease：先撤销旧 owner，再授予新 owner。"""
        partition_id = self._require_active(handoff).partition_id
        current = self.registry.current(partition_id)
        if current is None or current.owner != handoff.source_owner:
            raise LeaseError(
                f"分区 {partition_id} 的源 lease 已失效，无法移交"
            )
        new_epoch = current.epoch + 1
        self.registry.revoke(partition_id)
        self.registry.grant(
            partition_id, handoff.target_owner, new_epoch
        )
        moved = self._replace(
            handoff,
            phase=HandoffPhase.LEASE_TRANSFERRED,
            lease_committed=True,
        )
        self._handoffs[partition_id] = moved
        return moved

    def commit_target(
        self, handoff: HandoffState, offset: int
    ) -> tuple[HandoffState, Checkpoint]:
        """新 owner 在拿到 lease 后提交第一个 checkpoint。"""
        active = self._require_active(handoff)
        if not active.lease_committed:
            raise LeaseError(
                f"分区 {active.partition_id} 的 lease 尚未移交，"
                "目标 owner 不可提交"
            )
        lease = self.registry.current(active.partition_id)
        assert lease is not None
        checkpoint = self.store.commit(
            active.partition_id,
            offset,
            active.target_owner,
            lease.epoch,
            self.registry,
        )
        moved = self._replace(active, phase=HandoffPhase.COMMITTED)
        self._handoffs[active.partition_id] = moved
        return moved, checkpoint

    def complete(
        self, handoff: HandoffState, new_offset: int
    ) -> tuple[Lease, Checkpoint]:
        """完成交接：移交 lease 并让新 owner 提交续传 checkpoint。"""
        active = handoff
        if not active.lease_committed:
            active = self.transfer_lease(active)
        active, checkpoint = self.commit_target(active, new_offset)
        self._handoffs.pop(active.partition_id, None)
        lease = self.registry.current(active.partition_id)
        assert lease is not None
        return lease, checkpoint

    def interrupt(self, handoff: HandoffState) -> Lease:
        """中断交接并回滚。

        新 owner 已提交时拒绝回滚（数据已前进，不可逆）；否则把 lease
        恢复给旧 owner，使用更高的新 epoch 做 fencing。
        """
        active = self._require_active(handoff)
        if active.phase == HandoffPhase.COMMITTED:
            raise LeaseError(
                f"分区 {active.partition_id} 新 owner 已提交，无法回滚"
            )
        partition_id = active.partition_id
        current = self.registry.current(partition_id)
        if current is not None and current.owner == active.target_owner:
            self.registry.revoke(partition_id)
            self.registry.grant(
                partition_id,
                active.source_owner,
                current.epoch + 1,
            )
        else:
            base_epoch = current.epoch if current is not None else 0
            self.registry.grant(
                partition_id,
                active.source_owner,
                base_epoch + 1,
            )
        self._handoffs.pop(partition_id, None)
        restored = self.registry.current(partition_id)
        assert restored is not None
        return restored

    def handoff_state(self, partition_id: str) -> HandoffState | None:
        return self._handoffs.get(partition_id)

    def is_balanced(self, partition_id: str) -> bool:
        return partition_id not in self._handoffs

    def _require_active(self, handoff: HandoffState) -> HandoffState:
        active = self._handoffs.get(handoff.partition_id)
        if active is None:
            raise LeaseError(
                f"分区 {handoff.partition_id} 没有进行中的交接"
            )
        return active

    @staticmethod
    def _replace(handoff: HandoffState, **changes) -> HandoffState:
        return HandoffState(
            partition_id=handoff.partition_id,
            source_owner=handoff.source_owner,
            target_owner=handoff.target_owner,
            phase=changes.get("phase", handoff.phase),
            lease_committed=changes.get(
                "lease_committed", handoff.lease_committed
            ),
            started_at=handoff.started_at,
        )
