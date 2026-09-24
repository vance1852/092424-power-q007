from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path

from plant_science.api import JsonApplication
from plant_science.jsonio import load_json
from plant_science.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(TrialService(self.connection))

    def tearDown(self) -> None:
        self.connection.close()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_json_error_shape(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_user_route(self) -> None:
        payload = json.dumps({"user_id": "u1", "display_name": "操作员", "role": "operator"}).encode()
        response = self.app.handle("POST", "/users", body=payload)
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["role"], "operator")

    def test_job_heartbeat_and_abandon_routes(self) -> None:
        for user_id, role in (("op", "operator"), ("stat", "statistician")):
            payload = json.dumps({"user_id": user_id, "display_name": user_id, "role": role}).encode()
            self.assertEqual(self.app.handle("POST", "/users", body=payload).status, 201)
        headers = {"X-Actor-Id": "op"}
        self.app.handle("POST", "/robots", headers, json.dumps(
            {"robot_id": "robot-a", "model_name": "M", "vendor": "V"}
        ).encode())
        self.app.handle("POST", "/builds", headers, json.dumps(
            {"build_id": "build-a", "robot_id": "robot-a", "version": "1.0", "content_sha256": "c" * 64}
        ).encode())
        protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        response = self.app.handle("POST", "/protocols", {"X-Actor-Id": "stat"}, json.dumps(protocol).encode())
        self.assertEqual(response.status, 201, response.body)
        self.app.handle("POST", "/batches", headers, json.dumps(
            {"batch_id": "batch-1", "protocol_id": "demo-delivery-v1", "protocol_version": 1, "build_id": "build-a"}
        ).encode())
        self.app.handle("POST", "/batches/batch-1/start", headers, json.dumps({"expected_revision": 1}).encode())
        rows = [
            json.loads(line)
            for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        imported = self.app.handle(
            "POST",
            "/batches/batch-1/observations",
            {**headers, "Idempotency-Key": "k1"},
            json.dumps({"observations": rows}).encode(),
        )
        self.assertEqual(imported.status, 200, imported.body)
        self.app.handle("POST", "/batches/batch-1/seal", {"X-Actor-Id": "stat"}, json.dumps({"expected_revision": 2}).encode())
        claimed = self.app.handle("POST", "/jobs/claim", body=json.dumps({"worker_id": "w1", "lease_seconds": 30}).encode())
        self.assertEqual(claimed.status, 200)
        job_id = claimed.body["job"]["job_id"]
        renewed = self.app.handle(
            "POST", f"/jobs/{job_id}/heartbeat", body=json.dumps({"worker_id": "w1", "lease_seconds": 60}).encode()
        )
        self.assertEqual(renewed.status, 200)
        self.assertEqual(renewed.body["state"], "leased")
        foreign = self.app.handle(
            "POST", f"/jobs/{job_id}/heartbeat", body=json.dumps({"worker_id": "w2"}).encode()
        )
        self.assertEqual(foreign.status, 409)
        abandoned = self.app.handle(
            "POST", f"/jobs/{job_id}/abandon", body=json.dumps({"worker_id": "w1", "error": "放弃"}).encode()
        )
        self.assertEqual(abandoned.status, 200)
        self.assertEqual(abandoned.body["state"], "failed")
        self.assertEqual(
            self.app.handle("POST", "/jobs/claim", body=json.dumps({"worker_id": "w2"}).encode()).body["job"],
            None,
        )


if __name__ == "__main__":
    unittest.main()
