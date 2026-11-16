"""规划器测试：热点、上下线、容量、幂等、中断、输入顺序与确定性。"""

from __future__ import annotations

import unittest

from etl_rebalance.model import (
    HandoffPhase,
    HandoffState,
    MigrationReason,
    Partition,
    PartitionState,
    Worker,
    WorkerStatus,
)
from etl_rebalance.planner import (
    CapacityExceededError,
    InvalidClusterError,
    RebalancePlanner,
)


def make_workers(spec: dict[str, tuple[float, WorkerStatus, int]]) -> list[Worker]:
    return [
        Worker(
            worker_id=worker_id,
            capacity=capacity,
            status=status,
            cooldown_until=cooldown,
        )
        for worker_id, (capacity, status, cooldown) in spec.items()
    ]


class PlannerTest(unittest.TestCase):
    def test_balanced_cluster_produces_no_migration(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [
            Partition("p1", 40, "w1"),
            Partition("p2", 40, "w1"),
            Partition("p3", 60, "w2"),
        ]
        plan = RebalancePlanner(workers, partitions).plan()
        self.assertTrue(plan.is_empty())

    def test_hotspot_moves_minimum_partitions_from_overloaded_worker(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [
            Partition("p-hot", 70, "w1"),
            Partition("p-a", 30, "w1"),
            Partition("p-b", 20, "w1"),
            Partition("p-c", 20, "w2"),
        ]
        plan = RebalancePlanner(workers, partitions).plan()
        # w1 负载 120，只需挪走一个最大分区即可回到容量内。
        self.assertEqual(len(plan.migrations), 1)
        migration = plan.find("p-hot")
        self.assertIsNotNone(migration)
        self.assertEqual(migration.source_owner, "w1")
        self.assertEqual(migration.target_owner, "w2")
        self.assertEqual(migration.reason, MigrationReason.OVERLOAD)

    def test_offline_and_draining_workers_are_evacuated(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.OFFLINE, 0),
                "w2": (100, WorkerStatus.DRAINING, 0),
                "w3": (100, WorkerStatus.ONLINE, 0),
                "w4": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [
            Partition("p1", 40, "w1"),
            Partition("p2", 40, "w2"),
            Partition("p3", 30, "w3"),
        ]
        plan = RebalancePlanner(workers, partitions).plan()
        moved = {m.partition_id: m for m in plan.migrations}
        self.assertEqual(set(moved), {"p1", "p2"})
        self.assertEqual(moved["p1"].reason, MigrationReason.WORKER_OFFLINE)
        self.assertEqual(moved["p2"].reason, MigrationReason.WORKER_DRAINING)
        self.assertIn(moved["p1"].target_owner, {"w3", "w4"})
        self.assertIn(moved["p2"].target_owner, {"w3", "w4"})

    def test_insufficient_global_capacity_raises(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (50, WorkerStatus.OFFLINE, 0),
            }
        )
        partitions = [Partition("p1", 120, "w1")]
        with self.assertRaises(CapacityExceededError):
            RebalancePlanner(workers, partitions).plan()

    def test_no_eligible_target_due_to_worker_cooldown_raises(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.OFFLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 100),
            }
        )
        partitions = [Partition("p1", 40, "w1")]
        with self.assertRaises(CapacityExceededError):
            RebalancePlanner(workers, partitions, now=50).plan()

    def test_repeated_planning_returns_identical_migration_set(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
                "w3": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [
            Partition("p1", 80, "w1"),
            Partition("p2", 70, "w1"),
            Partition("p3", 30, "w2"),
        ]
        checkpoints = {"p1": 11, "p2": 22, "p3": 5}
        planner = RebalancePlanner(workers, partitions)
        first = planner.plan(checkpoints)
        second = planner.plan(checkpoints)
        self.assertEqual(first.migration_keys(), second.migration_keys())
        self.assertEqual(first.migrations, second.migrations)
        # 规划器不修改输入，重复调用安全。
        self.assertEqual(partitions[0].owner, "w1")

    def test_input_order_does_not_change_output(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions_a = [
            Partition("p1", 80, "w1", affinity=("w2",)),
            Partition("p2", 70, "w1"),
            Partition("p3", 20, "w2"),
        ]
        partitions_b = list(reversed(partitions_a))
        worker_list = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        plan_a = RebalancePlanner(worker_list[::-1], partitions_a).plan()
        plan_b = RebalancePlanner(worker_list, partitions_b).plan()
        self.assertEqual(plan_a.migrations, plan_b.migrations)
        self.assertEqual(plan_a.migration_keys(), plan_b.migration_keys())

    def test_affinity_only_affects_target_choice(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.OFFLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
                "w3": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [Partition("p1", 40, "w1", affinity=("w3",))]
        plan = RebalancePlanner(workers, partitions).plan()
        self.assertEqual(plan.find("p1").target_owner, "w3")

    def test_affinity_never_triggers_extra_migration(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [Partition("p1", 40, "w1", affinity=("w2",))]
        plan = RebalancePlanner(workers, partitions).plan()
        self.assertTrue(plan.is_empty())

    def test_checkpoint_carried_through_migration_instruction(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.OFFLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [Partition("p1", 40, "w1", lease_epoch=3)]
        plan = RebalancePlanner(workers, partitions).plan({"p1": 137})
        migration = plan.find("p1")
        self.assertEqual(migration.resume_from_checkpoint, 137)
        self.assertEqual(migration.lease_epoch, 4)

    def test_inflight_handoff_to_healthy_target_is_not_replanned(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        handoff = HandoffState(
            partition_id="p1",
            source_owner="w1",
            target_owner="w2",
            phase=HandoffPhase.LEASE_TRANSFERRED,
            lease_committed=True,
        )
        partitions = [
            Partition(
                "p1",
                40,
                "w1",
                state=PartitionState.MIGRATING,
                handoff=handoff,
            )
        ]
        plan = RebalancePlanner(workers, partitions).plan()
        self.assertTrue(plan.is_empty())

    def test_interrupted_handoff_rolls_back_without_extra_move(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.OFFLINE, 0),
            }
        )
        handoff = HandoffState(
            partition_id="p1",
            source_owner="w1",
            target_owner="w2",
            phase=HandoffPhase.INTERRUPTED,
            lease_committed=True,
        )
        partitions = [
            Partition(
                "p1",
                40,
                "w1",
                state=PartitionState.MIGRATING,
                handoff=handoff,
                lease_epoch=2,
            )
        ]
        plan = RebalancePlanner(workers, partitions).plan({"p1": 88})
        self.assertEqual(plan.migrations, ())
        self.assertEqual(len(plan.rollbacks), 1)
        rollback = plan.rollbacks[0]
        self.assertEqual(rollback.restore_owner, "w1")
        self.assertEqual(rollback.lease_epoch, 3)
        self.assertEqual(rollback.last_checkpoint, 88)

    def test_interrupted_handoff_with_draining_source_retargets(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.DRAINING, 0),
                "w2": (100, WorkerStatus.OFFLINE, 0),
                "w3": (100, WorkerStatus.ONLINE, 0),
            }
        )
        handoff = HandoffState(
            partition_id="p1",
            source_owner="w1",
            target_owner="w2",
            phase=HandoffPhase.INTERRUPTED,
            lease_committed=True,
        )
        partitions = [
            Partition(
                "p1",
                40,
                "w1",
                state=PartitionState.MIGRATING,
                handoff=handoff,
            )
        ]
        plan = RebalancePlanner(workers, partitions).plan()
        self.assertEqual(len(plan.rollbacks), 0)
        self.assertEqual(len(plan.migrations), 1)
        migration = plan.migrations[0]
        self.assertEqual(migration.source_owner, "w1")
        self.assertEqual(migration.target_owner, "w3")
        self.assertEqual(migration.reason, MigrationReason.RETARGET)

    def test_partition_cooldown_blocks_rescheduling(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [
            Partition("p1", 90, "w1", cooldown_until=100),
            Partition("p2", 90, "w1"),
        ]
        plan = RebalancePlanner(workers, partitions, now=50).plan()
        moved_ids = {m.partition_id for m in plan.migrations}
        self.assertEqual(moved_ids, {"p2"})

    def test_overload_cascades_until_all_workers_healthy(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.ONLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
                "w3": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [
            Partition("p1", 80, "w1"),
            Partition("p2", 80, "w1"),
            Partition("p3", 70, "w2"),
        ]
        plan = RebalancePlanner(workers, partitions).plan()
        targets = [m.target_owner for m in plan.migrations]
        # w1 挪出的分区会先填满 w2，其余必须继续疏散到 w3。
        self.assertIn("w3", targets)
        self.assertTrue(
            all(
                self._load_after(workers, partitions, plan, worker_id)
                <= self._capacity(workers, worker_id)
                for worker_id in ("w1", "w2", "w3")
            )
        )

    @staticmethod
    def _capacity(workers, worker_id: str) -> float:
        return next(w.capacity for w in workers if w.worker_id == worker_id)

    def _load_after(self, workers, partitions, plan, worker_id: str) -> float:
        load = sum(p.throughput for p in partitions if p.owner == worker_id)
        for migration in plan.migrations:
            if migration.source_owner == worker_id:
                load -= migration.throughput
            if migration.target_owner == worker_id:
                load += migration.throughput
        return load

    def test_invalid_owner_reference_raises(self) -> None:
        workers = make_workers({"w1": (100, WorkerStatus.ONLINE, 0)})
        partitions = [Partition("p1", 10, "ghost")]
        with self.assertRaises(InvalidClusterError):
            RebalancePlanner(workers, partitions)

    def test_deterministic_sequence_numbers(self) -> None:
        workers = make_workers(
            {
                "w1": (100, WorkerStatus.OFFLINE, 0),
                "w2": (100, WorkerStatus.ONLINE, 0),
                "w3": (100, WorkerStatus.ONLINE, 0),
            }
        )
        partitions = [
            Partition("p-z", 30, "w1"),
            Partition("p-a", 90, "w1"),
        ]
        plan = RebalancePlanner(workers, partitions).plan()
        self.assertEqual(
            [m.partition_id for m in plan.migrations], ["p-a", "p-z"]
        )
        self.assertEqual([m.seq for m in plan.migrations], [0, 1])


if __name__ == "__main__":
    unittest.main()
