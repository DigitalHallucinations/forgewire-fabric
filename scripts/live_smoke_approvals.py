"""Live smoke for M2.5.1 approval inbox against the OptiPlex hub.

Hits 10.120.81.95:8765 with the local hub.token and exercises:
  1. POST /tasks  (protected branch)            -> 428 + approval_id
  2. GET  /approvals?status=pending             -> includes that id
  3. POST /approvals/{id}/approve               -> status approved
  4. POST /tasks  (same intent, w/ approval_id) -> 200 (consumed)
  5. POST /tasks  (same intent again)           -> 428 (single-use)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx


HUB_URL = "http://10.120.81.95:8765"
TOKEN = Path(r"C:\Users\jerem\.forgewire\hub.token").read_text(encoding="utf-8").strip()
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

BASE = {
    "title": "live-approval-smoke",
    "prompt": "noop probe",
    "base_commit": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    "scope_globs": ["docs/_audit/approval-smoke.md"],
    "branch": "main",
    "todo_id": "approval-smoke-1",
}


def main() -> int:
    with httpx.Client(base_url=HUB_URL, headers=HEADERS, timeout=10.0) as c:
        # 1. expect 428
        r = c.post("/tasks", json=BASE)
        assert r.status_code == 428, f"expected 428, got {r.status_code}: {r.text}"
        detail = r.json()["detail"]
        approval_id = detail["approval_id"]
        print(f"[1] 428 OK approval_id={approval_id} envelope={detail['envelope_hash'][:12]}")

        # 2. list pending
        rows = c.get("/approvals", params={"status": "pending"}).json()["approvals"]
        assert any(row["approval_id"] == approval_id for row in rows), rows
        print(f"[2] pending list contains approval_id ({len(rows)} pending total)")

        # 3. approve
        r = c.post(
            f"/approvals/{approval_id}/approve",
            json={"approver": "live-smoke", "reason": "M2.5.1 verification"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "approved"
        print("[3] approved")

        # 4. re-dispatch with approval_id -> should pass the gate (200)
        body2 = dict(BASE)
        body2["approval_id"] = approval_id
        r = c.post("/tasks", json=body2)
        assert r.status_code == 200, f"expected 200 after approve, got {r.status_code}: {r.text}"
        task = r.json()
        print(f"[4] re-dispatch 200 task_id={task.get('task_id') or task.get('id')}")

        # 5. single-use: same approval_id second time should bounce
        r = c.post("/tasks", json=body2)
        assert r.status_code == 428, f"expected 428 (single-use), got {r.status_code}: {r.text}"
        new_id = r.json()["detail"]["approval_id"]
        assert new_id != approval_id, "consumed approval must not be re-used"
        print(f"[5] single-use OK; new pending approval_id={new_id}")

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
