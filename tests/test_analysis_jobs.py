from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from plant_science.api import JsonApplication
from plant_science.clock import FrozenClock
from plant_science.errors import InvalidState, NotFound, ValidationFailed
from plant_science.jsonio import load_json
from plant_science.service import TrialService
from plant_science.storage import connect


ROOT = Path(__file__).resolve().parents[1]


class AnalysisJobTests(unittest.TestCase):
    """在共享文件数据库上验证任务领取、心跳、超时回收、失败重试与幂等完成。"""

    def setUp(self) -> None:
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self._temporary = tempfile.TemporaryDirectory(prefix="analysis-jobs-")
        self.database = Path(self._temporary.name) / "jobs.sqlite3"
        self.service = self._open_service()
        self._seed_sealed_batch(self.service)

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def _open_service(self) -> TrialService:
        return TrialService(connect(self.database), self.clock)

    def _seed_sealed_batch(self, service: TrialService) -> None:
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("stat-b", "statistician"),
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

    def _job_row(self, service: TrialService | None = None) -> sqlite3.Row:
        service = service or self.service
        return service.connection.execute("SELECT * FROM analysis_jobs").fetchone()

    def _job_events(self, job_id: int, service: TrialService | None = None) -> list[tuple[str, str, dict]]:
        service = service or self.service
        rows = service.connection.execute(
            "SELECT event_type,actor_id,payload_json FROM audit_events "
            "WHERE entity_type='analysis_job' AND entity_id=? ORDER BY event_id",
            (str(job_id),),
        ).fetchall()
        return [(row["event_type"], row["actor_id"], json.loads(row["payload_json"])) for row in rows]

    def test_competing_workers_produce_single_winner(self) -> None:
        barrier = threading.Barrier(2)
        results: dict[str, object] = {}

        def claim(worker: str) -> None:
            # 每个线程独立连接，模拟两个统计员进程同时领取
            service = self._open_service()
            try:
                barrier.wait()
                results[worker] = service.claim_job(worker, 60)
            except Exception as exc:  # noqa: BLE001 - 汇总到主线程断言
                results[worker] = exc
            finally:
                service.connection.close()

        threads = [
            threading.Thread(target=claim, args=("worker-a",)),
            threading.Thread(target=claim, args=("worker-b",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        for value in results.values():
            if isinstance(value, Exception):
                raise value
        winners = {worker: job for worker, job in results.items() if job is not None}
        self.assertEqual(len(winners), 1)
        winner, job = next(iter(winners.items()))
        loser = "worker-b" if winner == "worker-a" else "worker-a"

        row = self._job_row()
        self.assertEqual(row["state"], "leased")
        self.assertEqual(row["lease_owner"], winner)
        self.assertEqual(row["attempts"], 1)
        self.assertEqual(row["failures"], 0)
        self.assertEqual(self.service.get_batch("batch-a")["state"], "analyzing")

        # 持有者租约仍有效，另一个统计员不能抢占
        other = self._open_service()
        self.assertIsNone(other.claim_job(loser, 60))
        row = self._job_row()
        self.assertEqual(row["lease_owner"], winner)
        self.assertEqual(row["attempts"], 1)

        events = self._job_events(job["job_id"])
        self.assertEqual([event[0] for event in events], ["analysis_job.claimed"])
        self.assertEqual(events[0][1], winner)
        self.assertEqual(events[0][2]["attempt"], 1)

    def test_lease_expiry_and_process_recovery(self) -> None:
        job = self.service.claim_job("worker-a", 10)
        job_id = job["job_id"]

        # 进程重启：新连接、新服务实例，同一数据库与同一注入时钟
        recovered = self._open_service()

        # 持有者仍有效，新进程不能抢占
        self.assertIsNone(recovered.claim_job("worker-b", 10))
        self.assertEqual(self._job_row(recovered)["lease_owner"], "worker-a")

        # 租约按注入时钟到期后才允许回队列
        self.clock.advance(seconds=11)
        taken = recovered.claim_job("worker-b", 30)
        self.assertIsNotNone(taken)
        self.assertEqual(taken["job_id"], job_id)
        self.assertEqual(taken["lease_owner"], "worker-b")
        self.assertEqual(taken["attempts"], 2)
        self.assertEqual(taken["failures"], 1)
        self.assertIn("worker-a", taken["last_error"])
        self.assertIn("回收", taken["last_error"])

        # 原持有者失去一切权利：不能完成、不能上报失败、不能心跳
        with self.assertRaises(InvalidState):
            recovered.complete_job("worker-a", job_id, "stat")
        with self.assertRaises(InvalidState):
            recovered.fail_job("worker-a", job_id, "失联进程复活")
        with self.assertRaises(InvalidState):
            recovered.heartbeat_job("worker-a", job_id, 30)

        events = self._job_events(job_id, recovered)
        self.assertEqual(
            [event[0] for event in events],
            ["analysis_job.claimed", "analysis_job.lease_expired", "analysis_job.claimed"],
        )
        self.assertEqual(events[1][2]["previous_owner"], "worker-a")
        self.assertEqual(events[2][1], "worker-b")

        # 新持有者可以正常完成，准入决定不再缺数据
        done = recovered.complete_job("worker-b", job_id, "stat")
        self.assertFalse(done["replayed"])
        self.assertEqual(recovered.get_batch("batch-a")["state"], "analyzed")

    def test_heartbeat_extends_lease_and_rejects_stale_holder(self) -> None:
        job = self.service.claim_job("worker-a", 30)
        job_id = job["job_id"]
        original_expiry = job["lease_expires_at"]

        self.clock.advance(seconds=20)
        renewed = self.service.heartbeat_job("worker-a", job_id, 60)
        self.assertGreater(renewed["lease_expires_at"], original_expiry)
        self.assertEqual(renewed["state"], "leased")

        # 非持有者不能心跳
        with self.assertRaises(InvalidState):
            self.service.heartbeat_job("worker-b", job_id, 60)

        # 已超过原始租约但仍在续期租约内，任务不会被回收
        self.clock.advance(seconds=25)
        self.assertIsNone(self.service.claim_job("worker-b", 10))

        result = self.service.complete_job("worker-a", job_id, "stat")
        self.assertFalse(result["replayed"])

        # 终态与非法输入的心跳
        with self.assertRaises(InvalidState):
            self.service.heartbeat_job("worker-a", job_id, 60)
        with self.assertRaises(NotFound):
            self.service.heartbeat_job("worker-a", 9999, 60)
        with self.assertRaises(ValidationFailed):
            self.service.heartbeat_job("worker-a", job_id, 0)

    def test_failure_retry_preserves_error_and_terminal_abandon(self) -> None:
        job = self.service.claim_job("worker-a", 10)
        job_id = job["job_id"]

        failed = self.service.fail_job("worker-a", job_id, "临时计算失败", retry_seconds=5)
        self.assertEqual(failed["state"], "queued")
        row = self._job_row()
        self.assertEqual(row["failures"], 1)
        self.assertEqual(row["last_error"], "临时计算失败")
        self.assertIsNone(row["lease_owner"])

        # 重试延迟未到，其他统计员不能领取
        self.assertIsNone(self.service.claim_job("worker-b", 10))
        self.clock.advance(seconds=5)
        retried = self.service.claim_job("worker-b", 10)
        self.assertEqual(retried["attempts"], 2)
        self.assertEqual(retried["failures"], 1)
        self.assertEqual(retried["last_error"], "临时计算失败")

        # 放弃：进入终态，失败次数与最后错误保留
        final = self.service.fail_job("worker-b", job_id, "数据不可恢复", retriable=False)
        self.assertEqual(final["state"], "failed")
        row = self._job_row()
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["failures"], 2)
        self.assertEqual(row["last_error"], "数据不可恢复")

        # 放弃后同一输入不能再次执行：不可领取、不可完成、不可再上报
        self.assertIsNone(self.service.claim_job("worker-c", 10))
        with self.assertRaises(InvalidState):
            self.service.complete_job("worker-b", job_id, "stat")
        with self.assertRaises(InvalidState):
            self.service.fail_job("worker-b", job_id, "重复上报")

        events = self._job_events(job_id)
        self.assertEqual(
            [event[0] for event in events],
            ["analysis_job.claimed", "analysis_job.failed", "analysis_job.claimed", "analysis_job.abandoned"],
        )
        self.assertEqual(events[1][2]["error"], "临时计算失败")
        self.assertEqual(events[3][2]["failures"], 2)

        # 报告让当天准入决定看到卡住的原因
        report = self.service.report("stat", "batch-a")
        self.assertEqual(report["batch"]["state"], "analyzing")
        self.assertEqual(report["job"]["state"], "failed")
        self.assertEqual(report["job"]["failures"], 2)
        self.assertEqual(report["job"]["last_error"], "数据不可恢复")

    def test_fail_job_validates_input_and_lease(self) -> None:
        job = self.service.claim_job("worker-a", 10)
        job_id = job["job_id"]
        with self.assertRaises(ValidationFailed):
            self.service.fail_job("worker-a", job_id, "  ")
        with self.assertRaises(ValidationFailed):
            self.service.fail_job("worker-a", job_id, "错误", retry_seconds=-1)
        with self.assertRaises(NotFound):
            self.service.fail_job("worker-a", 9999, "错误")
        # 租约过期后不再允许上报失败
        self.clock.advance(seconds=11)
        with self.assertRaises(InvalidState):
            self.service.fail_job("worker-a", job_id, "太迟了")

    def test_duplicate_completion_is_idempotent(self) -> None:
        job = self.service.claim_job("worker-a", 30)
        job_id = job["job_id"]
        first = self.service.complete_job("worker-a", job_id, "stat")
        self.assertFalse(first["replayed"])

        # 同一工作进程重复完成：返回同一结果，不再次执行
        second = self.service.complete_job("worker-a", job_id, "stat")
        self.assertTrue(second["replayed"])
        self.assertEqual(second["analysis_id"], first["analysis_id"])
        self.assertEqual(second["input_sha256"], first["input_sha256"])
        self.assertEqual(second["result"], first["result"])

        # 其他统计员重复同一请求也只读到已存结果
        third = self.service.complete_job("worker-c", job_id, "stat-b")
        self.assertTrue(third["replayed"])
        self.assertEqual(third["analysis_id"], first["analysis_id"])

        # 数据库状态：一条分析、一条完成审计、任务终态、批次可审批
        analyses = self.service.connection.execute("SELECT count(*) FROM analyses").fetchone()[0]
        self.assertEqual(analyses, 1)
        completed = self.service.connection.execute(
            "SELECT count(*) FROM audit_events WHERE event_type='analysis.completed'"
        ).fetchone()[0]
        self.assertEqual(completed, 1)
        row = self._job_row()
        self.assertEqual(row["state"], "succeeded")
        self.assertIsNone(row["lease_owner"])
        self.assertIsNone(row["lease_expires_at"])
        self.assertEqual(self.service.get_batch("batch-a")["state"], "analyzed")

        # 完成后同一输入不能再次执行：没有可领取的任务
        self.assertIsNone(self.service.claim_job("worker-d", 10))

        # 准入决定链路不受影响
        decision = self.service.decide("approver", "batch-a", first["analysis_id"], "approved", "满足规则")
        self.assertEqual(decision["decision"], "approved")

    def test_api_exposes_heartbeat_and_abandon(self) -> None:
        app = JsonApplication(self.service)
        claimed = app.handle("POST", "/jobs/claim", body=json.dumps({"worker_id": "worker-a", "lease_seconds": 30}).encode())
        self.assertEqual(claimed.status, 200)
        job_id = claimed.body["job"]["job_id"]

        renewed = app.handle(
            "POST",
            f"/jobs/{job_id}/heartbeat",
            body=json.dumps({"worker_id": "worker-a", "lease_seconds": 90}).encode(),
        )
        self.assertEqual(renewed.status, 200)
        self.assertGreater(renewed.body["lease_expires_at"], claimed.body["job"]["lease_expires_at"])

        stale = app.handle(
            "POST",
            f"/jobs/{job_id}/heartbeat",
            body=json.dumps({"worker_id": "worker-b"}).encode(),
        )
        self.assertEqual(stale.status, 409)

        abandoned = app.handle(
            "POST",
            f"/jobs/{job_id}/fail",
            body=json.dumps({"worker_id": "worker-a", "error": "不可恢复", "retriable": False}).encode(),
        )
        self.assertEqual(abandoned.status, 200)
        self.assertEqual(abandoned.body["state"], "failed")


if __name__ == "__main__":
    unittest.main()
