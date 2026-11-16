"""lease、checkpoint 交接与回滚的不变量测试。"""

from __future__ import annotations

import unittest

from etl_rebalance.handoff import (
    CheckpointError,
    CheckpointStore,
    HandoffManager,
    Lease,
    LeaseError,
    LeaseRegistry,
)
from etl_rebalance.model import (
    HandoffPhase,
    Migration,
    MigrationReason,
)


def make_migration(
    partition_id: str = "p1",
    source: str = "w1",
    target: str = "w2",
    epoch: int = 1,
    resume_from: int = 100,
) -> Migration:
    return Migration(
        partition_id=partition_id,
        source_owner=source,
        target_owner=target,
        throughput=40,
        reason=MigrationReason.OVERLOAD,
        lease_epoch=epoch,
        resume_from_checkpoint=resume_from,
        seq=0,
    )


class LeaseRegistryTest(unittest.TestCase):
    def test_epoch_fencing_rejects_stale_grant(self) -> None:
        registry = LeaseRegistry()
        registry.grant("p1", "w1", 1)
        with self.assertRaises(LeaseError):
            registry.grant("p1", "w2", 1)
        registry.grant("p1", "w2", 2)
        self.assertEqual(registry.current("p1"), Lease("p1", "w2", 2))

    def test_validate_rejects_unknown_or_stale_owner(self) -> None:
        registry = LeaseRegistry()
        registry.grant("p1", "w1", 1)
        with self.assertRaises(LeaseError):
            registry.validate("p1", "w2", 1)
        with self.assertRaises(LeaseError):
            registry.validate("p1", "w1", 0)
        self.assertEqual(
            registry.validate("p1", "w1", 1), Lease("p1", "w1", 1)
        )


class CheckpointStoreTest(unittest.TestCase):
    def test_commit_requires_active_lease_and_monotonic_offsets(self) -> None:
        registry = LeaseRegistry()
        store = CheckpointStore()
        registry.grant("p1", "w1", 1)
        store.commit("p1", 10, "w1", 1, registry)
        with self.assertRaises(LeaseError):
            store.commit("p1", 11, "w2", 1, registry)
        with self.assertRaises(CheckpointError):
            store.commit("p1", 10, "w1", 1, registry)
        store.commit("p1", 12, "w1", 1, registry)
        self.assertEqual(store.latest_offset("p1"), 12)


class HandoffManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = LeaseRegistry()
        self.store = CheckpointStore()
        self.manager = HandoffManager(self.registry, self.store)

    def test_single_committable_owner_during_handoff_window(self) -> None:
        self.manager.bootstrap("p1", "w1", 0)
        self.store.commit("p1", 100, "w1", 0, self.registry)
        migration = make_migration(epoch=1, resume_from=100)
        handoff = self.manager.begin(migration)
        self.assertEqual(handoff.phase, HandoffPhase.PREPARE)

        # 移交前旧 owner 仍可提交，新 owner 不行。
        self.store.commit("p1", 105, "w1", 0, self.registry)
        with self.assertRaises(LeaseError):
            self.store.commit("p1", 106, "w2", 1, self.registry)

        handoff = self.manager.transfer_lease(handoff)
        self.assertTrue(handoff.lease_committed)
        # 移交后旧 owner 的 epoch 立刻失效，无法再成功提交。
        with self.assertRaises(LeaseError):
            self.store.commit("p1", 107, "w1", 0, self.registry)
        self.manager.commit_target(handoff, 110)

    def test_complete_resumes_from_last_checkpoint(self) -> None:
        self.manager.bootstrap("p1", "w1", 0)
        self.store.commit("p1", 100, "w1", 0, self.registry)
        migration = make_migration(epoch=1, resume_from=100)
        handoff = self.manager.begin(migration)
        lease, checkpoint = self.manager.complete(handoff, 101)
        self.assertEqual(lease.owner, "w2")
        self.assertEqual(lease.epoch, 1)
        self.assertEqual(checkpoint.offset, 101)
        self.assertEqual(self.store.latest_offset("p1"), 101)
        history = self.store.history("p1")
        self.assertEqual([cp.owner for cp in history], ["w1", "w2"])
        self.assertEqual([cp.offset for cp in history], [100, 101])

    def test_target_cannot_commit_before_lease_transfer(self) -> None:
        self.manager.bootstrap("p1", "w1", 0)
        handoff = self.manager.begin(make_migration())
        with self.assertRaises(LeaseError):
            self.manager.commit_target(handoff, 101)

    def test_interrupt_before_commit_rolls_back_lease_to_source(self) -> None:
        self.manager.bootstrap("p1", "w1", 0)
        handoff = self.manager.begin(make_migration())
        handoff = self.manager.transfer_lease(handoff)
        restored = self.manager.interrupt(handoff)
        self.assertEqual(restored.owner, "w1")
        self.assertEqual(restored.epoch, 2)
        # 新 owner 的旧 epoch 已被 fencing，旧 owner 用新 epoch 恢复提交。
        with self.assertRaises(LeaseError):
            self.store.commit("p1", 101, "w2", 1, self.registry)
        self.store.commit("p1", 101, "w1", 2, self.registry)
        self.assertTrue(self.manager.is_balanced("p1"))

    def test_interrupt_after_commit_is_rejected(self) -> None:
        self.manager.bootstrap("p1", "w1", 0)
        self.store.commit("p1", 100, "w1", 0, self.registry)
        handoff = self.manager.begin(make_migration(resume_from=100))
        self.manager.complete(handoff, 101)
        handoff = self.manager.handoff_state("p1")
        self.assertIsNone(handoff)
        # 交接已完成并清除，再次中断必须报错而不是静默回滚。
        from etl_rebalance.model import HandoffState

        stale = HandoffState(
            partition_id="p1", source_owner="w1", target_owner="w2"
        )
        with self.assertRaises(LeaseError):
            self.manager.interrupt(stale)

    def test_begin_rejects_mismatched_source_lease(self) -> None:
        self.manager.bootstrap("p1", "w9", 0)
        with self.assertRaises(LeaseError):
            self.manager.begin(make_migration(source="w1"))

    def test_double_begin_rejected_until_handoff_finishes(self) -> None:
        self.manager.bootstrap("p1", "w1", 0)
        self.manager.begin(make_migration())
        with self.assertRaises(LeaseError):
            self.manager.begin(make_migration())

    def test_interrupt_in_prepare_phase_keeps_source_lease_with_new_epoch(
        self,
    ) -> None:
        self.manager.bootstrap("p1", "w1", 0)
        handoff = self.manager.begin(make_migration())
        restored = self.manager.interrupt(handoff)
        self.assertEqual(restored.owner, "w1")
        self.assertEqual(restored.epoch, 1)


if __name__ == "__main__":
    unittest.main()
