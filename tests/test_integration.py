"""规划器与交接执行器串联的端到端测试。"""

from __future__ import annotations

import unittest

from etl_rebalance import (
    CheckpointStore,
    HandoffManager,
    LeaseRegistry,
    Partition,
    RebalancePlanner,
    Worker,
    WorkerStatus,
)


class EndToEndTest(unittest.TestCase):
    def test_hotspot_handoff_keeps_single_owner_and_checkpoint_chain(self) -> None:
        workers = [
            Worker("w1", capacity=100),
            Worker("w2", capacity=100),
        ]
        partitions = [
            Partition("p-hot", 70, "w1"),
            Partition("p-a", 40, "w1"),
            Partition("p-b", 30, "w2"),
        ]
        registry = LeaseRegistry()
        store = CheckpointStore()
        manager = HandoffManager(registry, store)
        for partition in partitions:
            manager.bootstrap(partition.partition_id, partition.owner)
        store.commit("p-hot", 500, "w1", 0, registry)

        checkpoints = {
            partition.partition_id: store.latest_offset(partition.partition_id)
            for partition in partitions
        }
        plan = RebalancePlanner(workers, partitions).plan(checkpoints)
        self.assertEqual(len(plan.migrations), 1)
        migration = plan.find("p-hot")
        self.assertEqual(migration.target_owner, "w2")
        self.assertEqual(migration.resume_from_checkpoint, 500)

        handoff = manager.begin(migration)
        # 交接窗口内旧 owner 仍持 lease，新 owner 无法提前提交。
        with self.assertRaises(Exception):
            store.commit("p-hot", 501, "w2", migration.lease_epoch, registry)
        lease, checkpoint = manager.complete(
            handoff, migration.resume_from_checkpoint + 1
        )
        self.assertEqual(lease.owner, "w2")
        self.assertEqual(checkpoint.offset, 501)
        # 旧 owner 的 lease 已被 fencing，无法造成双提交。
        with self.assertRaises(Exception):
            store.commit("p-hot", 502, "w1", 0, registry)

    def test_replan_after_handoff_completion_is_stable_and_idempotent(self) -> None:
        workers = [
            Worker("w1", capacity=100),
            Worker("w2", capacity=100),
        ]
        partitions = [
            Partition("p1", 60, "w1"),
            Partition("p2", 60, "w2"),
        ]
        planner = RebalancePlanner(workers, partitions)
        first = planner.plan()
        second = planner.plan()
        self.assertEqual(first.migration_keys(), second.migration_keys())
        self.assertTrue(first.is_empty())
        self.assertEqual(
            WorkerStatus.ONLINE, workers[0].status
        )


if __name__ == "__main__":
    unittest.main()
