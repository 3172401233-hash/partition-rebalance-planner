"""确定性的 ETL 分区再平衡规划器。

规划器是纯函数式的：不修改输入对象，同一份集群状态（无论传入顺序）
永远产出相同的迁移集合。迁移数量遵循最小化原则：只有三类分区会被
搬迁——下线/排水 worker 上的分区、过载 worker 上必须挪走的分区、
以及目标已不可用的进行中交接。亲和关系仅影响目标选择，不会引发
额外搬迁。
"""

from __future__ import annotations

from collections.abc import Mapping

from .model import (
    HandoffPhase,
    Migration,
    MigrationReason,
    Partition,
    PartitionState,
    Plan,
    Rollback,
    Worker,
    WorkerStatus,
)


class RebalanceError(Exception):
    """再平衡相关错误的基类。"""


class InvalidClusterError(RebalanceError):
    """集群描述存在引用不一致或非法字段。"""


class CapacityExceededError(RebalanceError):
    """在线 worker 总容量不足，或分区没有合法目标。"""


# 必须搬迁项的处理优先级：先疏散下线，再排水、过载，最后重定向。
_REASON_ORDER = {
    MigrationReason.WORKER_OFFLINE: 0,
    MigrationReason.WORKER_DRAINING: 1,
    MigrationReason.OVERLOAD: 2,
    MigrationReason.RETARGET: 3,
}


class RebalancePlanner:
    """根据集群快照输出最小搬迁计划。

    参数:
        workers: 全量 worker，传入顺序不影响输出。
        partitions: 全量分区，传入顺序不影响输出。
        now: 当前时间戳，用于判断 worker/分区冷却。
    """

    def __init__(
        self,
        workers: list[Worker] | tuple[Worker, ...],
        partitions: list[Partition] | tuple[Partition, ...],
        now: int = 0,
    ) -> None:
        self.now = now
        self.workers: dict[str, Worker] = {}
        for worker in workers:
            if worker.worker_id in self.workers:
                raise InvalidClusterError(f"重复的 worker id: {worker.worker_id}")
            if worker.capacity < 0:
                raise InvalidClusterError(
                    f"worker {worker.worker_id} 容量不能为负"
                )
            self.workers[worker.worker_id] = worker

        self.partitions: dict[str, Partition] = {}
        for partition in partitions:
            if partition.partition_id in self.partitions:
                raise InvalidClusterError(
                    f"重复的分区 id: {partition.partition_id}"
                )
            self.partitions[partition.partition_id] = partition

        self._validate_references()

    def _validate_references(self) -> None:
        for partition in self.partitions.values():
            if partition.owner not in self.workers:
                raise InvalidClusterError(
                    f"分区 {partition.partition_id} 的 owner "
                    f"{partition.owner} 不存在"
                )
            for worker_id in partition.affinity:
                if worker_id not in self.workers:
                    raise InvalidClusterError(
                        f"分区 {partition.partition_id} 的亲和 worker "
                        f"{worker_id} 不存在"
                    )
            handoff = partition.handoff
            if partition.state == PartitionState.MIGRATING and handoff is None:
                raise InvalidClusterError(
                    f"分区 {partition.partition_id} 处于迁移中但缺少 handoff"
                )
            if handoff is not None:
                for label, worker_id in (
                    ("source", handoff.source_owner),
                    ("target", handoff.target_owner),
                ):
                    if worker_id not in self.workers:
                        raise InvalidClusterError(
                            f"分区 {partition.partition_id} handoff 的 "
                            f"{label} worker {worker_id} 不存在"
                        )

    def plan(
        self,
        last_checkpoints: Mapping[str, int] | None = None,
    ) -> Plan:
        """生成再平衡计划。

        ``last_checkpoints`` 给出每个分区旧 owner 的最后提交位点；
        迁移指令的 ``resume_from_checkpoint`` 直接取该值，保证新 owner
        从旧 owner 的最后提交继续，而不是自行另起位点。
        """

        checkpoints = dict(last_checkpoints or {})
        load = {worker_id: 0.0 for worker_id in self.workers}
        to_move: list[tuple[Partition, MigrationReason, str]] = []
        rollbacks: list[Rollback] = []

        for partition in self._sorted_partitions():
            if partition.state == PartitionState.MIGRATING:
                self._consume_handoff(
                    partition, load, to_move, rollbacks, checkpoints
                )
            else:
                owner = self.workers[partition.owner]
                if owner.status in (WorkerStatus.OFFLINE, WorkerStatus.DRAINING):
                    reason = (
                        MigrationReason.WORKER_OFFLINE
                        if owner.status == WorkerStatus.OFFLINE
                        else MigrationReason.WORKER_DRAINING
                    )
                    to_move.append((partition, reason, partition.owner))
                else:
                    load[partition.owner] += partition.throughput

        self._shed_overload(to_move, load)
        self._check_global_capacity()
        migrations = self._assign_targets(to_move, load, checkpoints)

        migrations.sort(key=lambda item: item.partition_id)
        rollbacks.sort(key=lambda item: item.partition_id)
        return Plan(
            migrations=tuple(
                self._with_seq(migrations, Migration, "partition_id")
            ),
            rollbacks=tuple(
                self._with_seq(rollbacks, Rollback, "partition_id")
            ),
        )

    def _sorted_partitions(self) -> list[Partition]:
        return sorted(self.partitions.values(), key=lambda item: item.partition_id)

    def _consume_handoff(
        self,
        partition: Partition,
        load: dict[str, float],
        to_move: list[tuple[Partition, MigrationReason, str]],
        rollbacks: list[Rollback],
        checkpoints: Mapping[str, int],
    ) -> None:
        handoff = partition.handoff
        assert handoff is not None
        target = self.workers[handoff.target_owner]
        source = self.workers[handoff.source_owner]
        last_cp = checkpoints.get(partition.partition_id, 0)

        if target.status != WorkerStatus.OFFLINE:
            # 交接仍然有效：容量按 target 计，规划器不重复发起迁移。
            load[handoff.target_owner] += partition.throughput
            return

        # 只有源 worker 仍然健康（在线）时回滚才有意义；源下线或排水时
        # 直接重新选址，避免把 lease 恢复到一个注定要疏散的 worker 上。
        rollback_required = source.status == WorkerStatus.ONLINE

        if rollback_required:
            rollbacks.append(
                Rollback(
                    partition_id=partition.partition_id,
                    restore_owner=handoff.source_owner,
                    lease_epoch=partition.lease_epoch + 1,
                    last_checkpoint=last_cp,
                    seq=0,
                )
            )

        if source.status in (WorkerStatus.OFFLINE, WorkerStatus.DRAINING):
            reason = (
                MigrationReason.WORKER_OFFLINE
                if source.status == WorkerStatus.OFFLINE
                else MigrationReason.RETARGET
            )
            to_move.append((partition, reason, handoff.source_owner))
        elif handoff.phase == HandoffPhase.INTERRUPTED:
            # 中断后源仍健康：回滚到源，回滚完成后不产生额外搬迁。
            load[handoff.source_owner] += partition.throughput
        else:
            # 目标下线但源健康：回滚到源即恢复服务。
            load[handoff.source_owner] += partition.throughput

    def _shed_overload(
        self,
        to_move: list[tuple[Partition, MigrationReason, str]],
        load: dict[str, float],
    ) -> None:
        # 多轮疏散：一个过载 worker 挪走的分区可能压垮另一个 worker，
        # 因此持续扫描直到没有在线 worker 过载为止。
        changed = True
        while changed:
            changed = False
            for worker_id in sorted(load):
                worker = self.workers[worker_id]
                if worker.status != WorkerStatus.ONLINE:
                    continue
                while load[worker_id] > worker.capacity:
                    candidate = self._pick_shed_partition(worker_id, to_move)
                    if candidate is None:
                        raise CapacityExceededError(
                            f"worker {worker_id} 过载且没有可搬迁的分区"
                        )
                    to_move.append(
                        (candidate, MigrationReason.OVERLOAD, worker_id)
                    )
                    load[worker_id] -= candidate.throughput
                    changed = True

    def _pick_shed_partition(
        self,
        worker_id: str,
        to_move: list[tuple[Partition, MigrationReason, str]],
    ) -> Partition | None:
        moving_ids = {item[0].partition_id for item in to_move}
        candidates = [
            partition
            for partition in self.partitions.values()
            if partition.partition_id not in moving_ids
            and partition.state == PartitionState.STABLE
            and partition.owner == worker_id
            and not partition.in_cooldown(self.now)
        ]
        if not candidates:
            return None
        # 优先挪走吞吐最大的分区，使搬迁次数最少。
        candidates.sort(key=lambda item: (-item.throughput, item.partition_id))
        return candidates[0]

    def _check_global_capacity(self) -> None:
        online_capacity = sum(
            worker.capacity
            for worker in self.workers.values()
            if worker.status == WorkerStatus.ONLINE
        )
        demand = sum(
            partition.throughput for partition in self.partitions.values()
        )
        if demand > online_capacity:
            raise CapacityExceededError(
                f"在线容量 {online_capacity} 不足以承载总吞吐 {demand}"
            )

    def _assign_targets(
        self,
        to_move: list[tuple[Partition, MigrationReason, str]],
        load: dict[str, float],
        checkpoints: Mapping[str, int],
    ) -> list[Migration]:
        ordered = sorted(
            to_move,
            key=lambda item: (_REASON_ORDER[item[1]], item[0].partition_id),
        )
        migrations: list[Migration] = []
        for partition, reason, source_owner in ordered:
            if partition.in_cooldown(self.now):
                raise CapacityExceededError(
                    f"分区 {partition.partition_id} 处于冷却中，无法重新选址"
                )
            target = self._choose_target(partition, source_owner, load)
            if target is None:
                raise CapacityExceededError(
                    f"分区 {partition.partition_id} 找不到满足容量的目标 worker"
                )
            load[target] += partition.throughput
            migrations.append(
                Migration(
                    partition_id=partition.partition_id,
                    source_owner=source_owner,
                    target_owner=target,
                    throughput=partition.throughput,
                    reason=reason,
                    lease_epoch=partition.lease_epoch + 1,
                    resume_from_checkpoint=checkpoints.get(
                        partition.partition_id, 0
                    ),
                    seq=0,
                )
            )
        return migrations

    def _choose_target(
        self,
        partition: Partition,
        source_owner: str,
        load: dict[str, float],
    ) -> str | None:
        eligible = [
            worker_id
            for worker_id, worker in self.workers.items()
            if worker_id != source_owner
            and worker.can_receive(self.now)
            and load[worker_id] + partition.throughput <= worker.capacity
        ]
        if not eligible:
            return None

        def rank(worker_id: str) -> tuple[int, float, str]:
            worker = self.workers[worker_id]
            affinity_index = (
                list(partition.affinity).index(worker_id)
                if worker_id in partition.affinity
                else len(partition.affinity)
            )
            headroom = worker.capacity - load[worker_id]
            # 亲和优先；同级优先填剩余空间最小的 worker，保持整体均衡。
            return (affinity_index, -headroom, worker_id)

        eligible.sort(key=rank)
        return eligible[0]

    @staticmethod
    def _with_seq(items, item_type: type, order_attr: str) -> tuple:
        results = []
        for seq, item in enumerate(sorted(items, key=lambda x: getattr(x, order_attr))):
            results.append(
                item_type(
                    **{
                        key: value
                        for key, value in item.__dict__.items()
                        if key != "seq"
                    },
                    seq=seq,
                )
            )
        return tuple(results)
