"""Smoke test for the blackboard service. Drives the full task lifecycle.

Assumes the server is already running and BLACKBOARD_URL / BLACKBOARD_TOKEN are set.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("BLACKBOARD_URL", "http://127.0.0.1:8799")
TOKEN = os.environ["BLACKBOARD_TOKEN"]


def call(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {TOKEN}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode()
            try:
                return resp.status, json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


fails = 0


def expect(label: str, ok: bool, detail: object = "") -> None:
    global fails
    mark = "PASS" if ok else "FAIL"
    suffix = f"  -- {detail}" if not ok else ""
    print(f"[{mark}] {label}{suffix}")
    if not ok:
        fails += 1


# 1. unauth check
req = urllib.request.Request(f"{BASE}/tasks", method="GET")
try:
    urllib.request.urlopen(req, timeout=5)
    expect("rejects requests without bearer token", False, "no error raised")
except urllib.error.HTTPError as e:
    expect("rejects requests without bearer token", e.code in (401, 403), f"got {e.code}")

# 2. dispatch
status, task = call(
    "POST",
    "/tasks",
    {
        "title": "smoke 1",
        "todo_id": "smoke-1",
        "branch": "agent/smoke/test-1",
        "base_commit": "deadbeefcafe",
        "scope_globs": ["docs/smoke/**"],
        "prompt": "Add a single line.",
        "priority": 10,
        "metadata": {"created_by": "smoke_test"},
    },
)
expect("dispatch_task returns 200", status == 200, f"{status}: {task}")
assert isinstance(task, dict)
task_id = task["id"]
expect("task starts queued", task["status"] == "queued", task["status"])
print(f"  task_id = {task_id}")

# 3. list queued
status, listed = call("GET", "/tasks?status=queued")
expect("list_tasks returns 200", status == 200)
assert isinstance(listed, dict)
expect("task appears in queued list", any(t["id"] == task_id for t in listed["tasks"]))

# 4. claim
status, claimed = call("POST", "/tasks/claim", {"worker_id": "smoke-worker-1", "hostname": "smoke-host"})
expect("claim returns 200", status == 200, claimed)
assert isinstance(claimed, dict)
expect("claim returns the dispatched task", claimed.get("task", {}).get("id") == task_id, claimed)
expect("claimed task is in 'claimed' state", claimed["task"]["status"] == "claimed")

# 5. mark running
status, started = call("POST", f"/tasks/{task_id}/start")
expect("mark_running returns 200", status == 200, started)
assert isinstance(started, dict)
expect("task is running", started["status"] == "running", started["status"])

# 6. progress beats (server auto-assigns seq)
for msg in ["preparing worktree", "editing file", "committing"]:
    status, prog = call(
        "POST",
        f"/tasks/{task_id}/progress",
        {
            "worker_id": "smoke-worker-1",
            "message": msg,
            "files_touched": ["docs/smoke/note.md"],
        },
    )
    expect(f"progress '{msg}' accepted", status == 200, prog)

# 7. notes round-trip
status, _ = call("POST", f"/tasks/{task_id}/notes", {"author": "dispatcher", "body": "looks good"})
expect("post_note (dispatcher) accepted", status == 200)
status, _ = call("POST", f"/tasks/{task_id}/notes", {"author": "smoke-worker-1", "body": "ack"})
expect("post_note (worker) accepted", status == 200)
status, notes = call("GET", f"/tasks/{task_id}/notes")
ok = status == 200 and isinstance(notes, dict) and len(notes["notes"]) == 2
expect("read_notes returns both", ok, notes)

# 8. wrong-worker submit rejected
status, body = call(
    "POST",
    f"/tasks/{task_id}/result",
    {"worker_id": "wrong-worker", "status": "done", "head_commit": "abc1234"},
)
expect("result with wrong worker_id is rejected", status in (400, 403, 409), f"status={status} body={body}")

# 9. correct submit
status, final = call(
    "POST",
    f"/tasks/{task_id}/result",
    {
        "worker_id": "smoke-worker-1",
        "status": "done",
        "head_commit": "abc1234",
        "commits": ["abc1234"],
        "files_touched": ["docs/smoke/note.md"],
        "test_summary": "skipped",
        "log_tail": "all good",
    },
)
expect("submit_result returns 200", status == 200, final)

# 10. final state
time.sleep(0.2)
status, finalTask = call("GET", f"/tasks/{task_id}")
expect("final task fetch 200", status == 200)
assert isinstance(finalTask, dict)
expect("final task is 'done'", finalTask["status"] == "done", finalTask["status"])

# 11. cancel flow
status, t2 = call(
    "POST",
    "/tasks",
    {
        "title": "smoke cancel",
        "todo_id": "smoke-cancel",
        "branch": "agent/smoke/cancel",
        "base_commit": "deadbeefcafe",
        "scope_globs": ["docs/smoke/**"],
        "prompt": "nothing",
    },
)
expect("dispatch second task", status == 200)
assert isinstance(t2, dict)
status, _ = call("POST", f"/tasks/{t2['id']}/cancel")
expect("cancel returns 200", status == 200)
status, t2c = call("GET", f"/tasks/{t2['id']}")
assert isinstance(t2c, dict)
ok = (
    t2c.get("cancel_requested") in (True, 1)
    or t2c["status"] in ("cancelled", "cancel_requested")
)
expect("cancel reflected on task", ok, t2c)

# 12. empty claim
status, empty = call("POST", "/tasks/claim", {"worker_id": "smoke-worker-1"})
expect("claim when empty returns 200 or 204", status in (200, 204), f"{status}: {empty}")
expect("empty claim body is task:null", isinstance(empty, dict) and empty.get("task") is None, empty)

if fails == 0:
    print("\nALL SMOKE CHECKS PASSED")
    sys.exit(0)
else:
    print(f"\n{fails} CHECK(S) FAILED")
    sys.exit(1)
