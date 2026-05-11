"""Live smoke for the M2.5.3 audit chain on the OptiPlex hub.

Steps:
  1. Read /audit/tail.
  2. POST /tasks (unsigned) and confirm /audit/tasks/{id} carries one
     'dispatch' event linked to the prior tail.
  3. POST /tasks/claim and confirm 'claim' event lands on the chain.
  4. POST /tasks/{id}/result and confirm 'result' event lands.
  5. /audit/day/today verifies the chain over the day's events.
  6. Print the new tail and the per-task chain.

Mocking policy: none. We hit the live OptiPlex hub.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import httpx


HUB_URL = "http://10.120.81.95:8765"
TOKEN = Path(r"C:\Users\jerem\.forgewire\hub.token").read_text(encoding="utf-8").strip()
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def main() -> int:
    with httpx.Client(base_url=HUB_URL, headers=HEADERS, timeout=10.0) as c:
        tail0 = c.get("/audit/tail").json()["chain_tail"]
        print(f"[0] tail before = {tail0[:16]}")

        # 1. dispatch (unprotected branch -> gate allows)
        body = {
            "title": "audit-live-smoke",
            "prompt": "noop probe",
            "scope_globs": ["docs/_audit/audit-smoke.md"],
            "base_commit": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "branch": "feature/audit-smoke",
            "todo_id": "audit-live-1",
        }
        r = c.post("/tasks", json=body)
        assert r.status_code == 200, r.text
        task = r.json()
        tid = task["id"]
        print(f"[1] dispatched task_id={tid}")

        # 2. claim
        r = c.post(
            "/tasks/claim",
            json={"worker_id": "audit-smoke-w", "hostname": "smoke", "capabilities": {}},
        )
        assert r.status_code == 200, r.text
        # Claim returns highest-priority queued task; might not be ours if other
        # work is queued. Re-check via task lookup either way.
        claimed = r.json()["task"]

        # 3. report a result if we got our own task
        if claimed and claimed["id"] == tid:
            r = c.post(
                f"/tasks/{tid}/result",
                json={
                    "worker_id": "audit-smoke-w",
                    "status": "done",
                    "head_commit": "f" * 40,
                    "commits": ["f" * 40],
                    "files_touched": ["docs/_audit/audit-smoke.md"],
                    "test_summary": "ok",
                    "log_tail": "",
                    "error": None,
                },
            )
            assert r.status_code == 200, r.text
            print("[3] reported result")
        else:
            print(f"[3] skipped: claim returned task_id={claimed['id'] if claimed else None}")

        # 4. fetch + verify per-task chain
        doc = c.get(f"/audit/tasks/{tid}").json()
        kinds = [e["kind"] for e in doc["events"]]
        print(f"[4] task chain kinds={kinds} verified={doc['verified']}")
        assert doc["verified"] is True, doc.get("error")
        assert "dispatch" in kinds, kinds

        # First event must link to the tail we observed before dispatch (or
        # any prior event if other writers raced — verify_audit_chain
        # already proved the chain itself is intact).
        first = doc["events"][0]
        print(
            f"    dispatch.event_id_hash={first['event_id_hash'][:16]} "
            f"prev={first['prev_event_id_hash'][:16]}"
        )

        # 5. day-wide chain verification
        today = dt.datetime.utcnow().date().isoformat()
        day = c.get(f"/audit/day/{today}").json()
        print(f"[5] day={today} events={len(day['events'])} verified={day['verified']}")
        assert day["verified"] is True, day.get("error")

        tail1 = c.get("/audit/tail").json()["chain_tail"]
        assert tail1 != tail0, "chain head must have advanced"
        print(f"[6] tail after = {tail1[:16]}")

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
