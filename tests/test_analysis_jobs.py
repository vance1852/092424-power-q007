"""分析任务租约生命周期的状态流转验证。

覆盖任务领取、心跳续租、超时回收、失败重试、放弃终态、
进程恢复与重复完成请求，并核对数据库状态与审计链。
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from plant_science.clock import FrozenClock, isoformat
from plant_science.errors import InvalidState
from plant_science.jsonio import load_json
from plant_science.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


def prepare_batch(service: TrialService) -> None:
    """登记用户、协议与批次，封存后留下一个 queued 状态的分析任务。"""

    for user_id, role in (
        ("operator", "operator"),
        ("stat", "statistician"),
        ("stat-2", "statistician"),
        ("approver", "approver"),
    ):
        service.create_user(user_id, user_id, role)
    protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
    rows = [
        json.loads(line)
        for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    service.register_robot("operator", "robot-a", "A 型", "厂商")
    service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
    service.publish_protocol("stat", protocol)
    service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
    service.start_batch("operator", "batch-a", 1)
    service.import_observations("operator", "batch-a", "key-1", rows)
    service.seal_batch("stat", "batch-a", 2)


class JobLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        prepare_batch(self.service)

    def tearDown(self) -> None:
        self.connection.close()

    def _job_row(self) -> sqlite3.Row:
        return self.connection.execute("SELECT * FROM analysis_jobs").fetchone()

    def _job_events(self) -> list[tuple[str, str, dict]]:
        rows = self.connection.execute(
            "SELECT event_type,actor_id,payload_json FROM audit_events "
            "WHERE entity_type='analysis_job' ORDER BY event_id"
        ).fetchall()
        return [(row["event_type"], row["actor_id"], json.loads(row["payload_json"])) for row in rows]

    def test_competing_workers_cannot_preempt_valid_lease(self) -> None:
        first = self.service.claim_job("worker-a", 30)
        self.assertIsNotNone(first)
        # 持有者租约仍然有效：第二个统计员不能抢占。
        self.assertIsNone(self.service.claim_job("worker-b", 30))
        # 心跳续租后，原租约到期时刻也不再放行。
        renewed = self.service.heartbeat_job("worker-a", first["job_id"], 60)
        self.clock.advance(seconds=31)
        self.assertIsNone(self.service.claim_job("worker-b", 30))
        # 超过心跳后的租约才允许回收。
        self.clock.advance(seconds=60)
        second = self.service.claim_job("worker-b", 30)
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(second["lease_owner"], "worker-b")
        self.assertEqual(second["attempts"], 2)
        self.assertEqual(second["failures"], 1)
        self.assertIn("worker-a", second["last_error"])
        self.assertEqual(renewed["lease_expires_at"] > first["lease_expires_at"], True)
        events = self._job_events()
        self.assertEqual(
            [event[0] for event in events],
            ["analysis.claimed", "analysis.heartbeat", "analysis.lease_expired", "analysis.claimed"],
        )
        self.assertEqual(events[2][2]["previous_owner"], "worker-a")
        self.assertEqual(events[2][2]["reclaimed_by"], "worker-b")
        self.assertEqual(events[3][2]["reclaimed_from"], "worker-a")

    def test_heartbeat_requires_live_ownership(self) -> None:
        job = self.service.claim_job("worker-a", 10)
        with self.assertRaises(InvalidState):
            self.service.heartbeat_job("worker-b", job["job_id"], 10)
        self.clock.advance(seconds=11)
        with self.assertRaises(InvalidState):
            self.service.heartbeat_job("worker-a", job["job_id"], 10)
        row = self._job_row()
        self.assertEqual(row["state"], "leased")
        self.assertEqual(row["lease_owner"], "worker-a")

    def test_lease_expiry_reclaim_preserves_failure_record(self) -> None:
        first = self.service.claim_job("worker-a", 10)
        self.clock.advance(seconds=11)
        second = self.service.claim_job("worker-b", 10)
        row = self._job_row()
        self.assertEqual(row["failures"], 1)
        self.assertEqual(row["last_error"], "租约到期未心跳，原持有者 worker-a 被回收")
        # 原持有者已失去租约，不能完成也不能上报失败。
        with self.assertRaises(InvalidState):
            self.service.complete_job("worker-a", first["job_id"], "stat")
        with self.assertRaises(InvalidState):
            self.service.fail_job("worker-a", first["job_id"], "迟到的心跳")
        # 新持有者可以正常完成。
        analysis = self.service.complete_job("worker-b", second["job_id"], "stat")
        self.assertEqual(len(analysis["input_sha256"]), 64)

    def test_fail_job_records_error_and_honours_retry_delay(self) -> None:
        job = self.service.claim_job("worker-a", 10)
        failed = self.service.fail_job("worker-a", job["job_id"], "临时计算失败", retry_seconds=5)
        self.assertEqual(failed["state"], "queued")
        self.assertEqual(failed["failures"], 1)
        self.assertIsNone(self.service.claim_job("worker-b", 10))
        self.clock.advance(seconds=5)
        retried = self.service.claim_job("worker-b", 10)
        self.assertEqual(retried["job_id"], job["job_id"])
        # 失败次数与最后错误在重新领取后仍然保留。
        self.assertEqual(retried["failures"], 1)
        self.assertEqual(retried["last_error"], "临时计算失败")
        events = self._job_events()
        self.assertEqual([event[0] for event in events], ["analysis.claimed", "analysis.failed", "analysis.claimed"])
        self.assertEqual(events[1][2]["error"], "临时计算失败")
        self.assertEqual(events[1][2]["failures"], 1)

    def test_abandon_is_terminal_and_blocks_same_input(self) -> None:
        job = self.service.claim_job("worker-a", 10)
        abandoned = self.service.abandon_job("worker-a", job["job_id"], "测点数据不可用")
        self.assertEqual(abandoned["state"], "failed")
        # 终态不再回队列。
        self.assertIsNone(self.service.claim_job("worker-b", 10))
        with self.assertRaises(InvalidState):
            self.service.complete_job("worker-a", job["job_id"], "stat")
        with self.assertRaises(InvalidState):
            self.service.fail_job("worker-a", job["job_id"], "重复上报")
        with self.assertRaises(InvalidState):
            self.service.abandon_job("worker-a", job["job_id"], "重复放弃")
        row = self._job_row()
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["failures"], 1)
        self.assertEqual(row["last_error"], "测点数据不可用")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM analyses").fetchone()[0], 0)
        events = self._job_events()
        self.assertEqual([event[0] for event in events], ["analysis.claimed", "analysis.abandoned"])

    def test_duplicate_completion_is_rejected_without_side_effects(self) -> None:
        job = self.service.claim_job("worker-a", 30)
        completed = self.service.complete_job("worker-a", job["job_id"], "stat")
        with self.assertRaises(InvalidState):
            self.service.complete_job("worker-a", job["job_id"], "stat")
        with self.assertRaises(InvalidState):
            self.service.complete_job("worker-b", job["job_id"], "stat-2")
        # 同一输入只执行一次：分析结果与完成事件都只有一条。
        analyses = self.connection.execute("SELECT analysis_id,input_sha256 FROM analyses").fetchall()
        self.assertEqual(len(analyses), 1)
        self.assertEqual(analyses[0]["analysis_id"], completed["analysis_id"])
        completed_events = self.connection.execute(
            "SELECT count(*) FROM audit_events WHERE event_type='analysis.completed'"
        ).fetchone()[0]
        self.assertEqual(completed_events, 1)
        batch = self.service.get_batch("batch-a")
        self.assertEqual(batch["state"], "analyzed")
        # 准入决定可以基于唯一的分析结果正常形成。
        decided = self.service.decide("approver", "batch-a", completed["analysis_id"], "approved", "数据齐全")
        self.assertEqual(decided["decision"], "approved")


class JobRecoveryTests(unittest.TestCase):
    """进程重启后，文件数据库中的租约状态仍然约束新的统计员。"""

    def test_process_restart_recovers_queue_state(self) -> None:
        clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "jobs.sqlite3"
            first_connection = sqlite3.connect(str(database), isolation_level=None)
            first_connection.row_factory = sqlite3.Row
            first_service = TrialService(first_connection, clock)
            prepare_batch(first_service)
            stuck = first_service.claim_job("worker-a", 30)
            self.assertIsNotNone(stuck)
            # 模拟进程崩溃：连接直接关闭，任务停在已领取状态。
            first_connection.close()

            recovered_connection = sqlite3.connect(str(database), isolation_level=None)
            recovered_connection.row_factory = sqlite3.Row
            try:
                recovered = TrialService(recovered_connection, clock)
                # 租约尚未按注入时钟到期，新的统计员不能接手。
                self.assertIsNone(recovered.claim_job("worker-b", 30))
                clock.advance(seconds=31)
                claimed = recovered.claim_job("worker-b", 30)
                self.assertEqual(claimed["job_id"], stuck["job_id"])
                self.assertEqual(claimed["lease_owner"], "worker-b")
                # 崩溃前留下的租约被记为一次失败，原因可查。
                self.assertEqual(claimed["failures"], 1)
                self.assertIn("worker-a", claimed["last_error"])
                analysis = recovered.complete_job("worker-b", claimed["job_id"], "stat")
                self.assertEqual(len(analysis["input_sha256"]), 64)
                events = recovered_connection.execute(
                    "SELECT event_type,actor_id FROM audit_events "
                    "WHERE entity_type='analysis_job' ORDER BY event_id"
                ).fetchall()
                self.assertEqual(
                    [(row["event_type"], row["actor_id"]) for row in events],
                    [
                        ("analysis.claimed", "worker-a"),
                        ("analysis.lease_expired", "worker-b"),
                        ("analysis.claimed", "worker-b"),
                    ],
                )
                batch = recovered.get_batch("batch-a")
                self.assertEqual(batch["state"], "analyzed")
            finally:
                recovered_connection.close()


class ClockFormatTests(unittest.TestCase):
    def test_isoformat_is_fixed_width_for_ordering(self) -> None:
        whole = isoformat(datetime(2026, 9, 24, 8, 0, 10, tzinfo=timezone.utc))
        fraction = isoformat(datetime(2026, 9, 24, 8, 0, 10, 500000, tzinfo=timezone.utc))
        self.assertEqual(whole, "2026-09-24T08:00:10.000000Z")
        self.assertEqual(fraction, "2026-09-24T08:00:10.500000Z")
        self.assertLess(whole, fraction)


if __name__ == "__main__":
    unittest.main()
