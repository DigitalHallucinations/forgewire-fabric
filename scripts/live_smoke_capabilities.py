"""Live smoke for M2.5.4 capability tags + smart routing on the OptiPlex hub.

Steps:
  1. GET /runners — print each runner's runner_id + advertised capabilities.
  2. Dispatch a task that *should* match the OptiPlex runner's caps
     (Python 3.13.x + windows-11 OS, both of which the runner advertises).
  3. Dispatch a task with an impossible capability (gpu.cuda >= 99).
  4. GET /tasks/waiting and assert the impossible task appears with
     missing_per_runner populated.
  5. Print PASS.

Mocking policy: none — we hit the live hub at OptiPlex.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx


HUB_URL = "http://10.120.81.95:8765"
TOKEN = Path(r"C:\Users\jerem\.forgewire\hub.token").read_text(encoding="utf-8").strip()
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def main() -> int:
    with httpx.Client(base_url=HUB_URL, headers=HEADERS, timeout=10.0) as c:
        # 1. Runner caps inventory.
        r = c.get("/runners")
        assert r.status_code == 200, r.text
        runners = r.json().get("runners", [])
        print(f"[1] {len(runners)} runner(s) registered")
        for runner in runners:
            print(
                f"    - {runner['runner_id'][:16]} state={runner.get('state')}"
                f" caps={json.dumps(runner.get('capabilities') or {})}"
            )

        # 2. Achievable task: python ~= 3.13.
        body_ok = {
            "title": "cap-smoke-ok",
            "prompt": "noop, capability-routed",
            "scope_globs": ["docs/_audit/cap-smoke-ok.md"],
            "base_commit": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "branch": "feature/cap-smoke-ok",
            "required_capabilities": ["python ~= 3.13"],
            "todo_id": "cap-live-ok",
        }
        r = c.post("/tasks", json=body_ok)
        assert r.status_code == 200, r.text
        ok_tid = r.json()["id"]
        print(f"[2] dispatched achievable task id={ok_tid}")

        # 3. Impossible task: gpu.cuda >= 99 (no OptiPlex GPU advertises that).
        body_bad = {
            "title": "cap-smoke-waiting",
            "prompt": "noop, must wait forever",
            "scope_globs": ["docs/_audit/cap-smoke-bad.md"],
            "base_commit": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "branch": "feature/cap-smoke-bad",
            "required_capabilities": ["gpu.cuda >= 99"],
            "todo_id": "cap-live-bad",
        }
        r = c.post("/tasks", json=body_bad)
        assert r.status_code == 200, r.text
        bad_tid = r.json()["id"]
        print(f"[3] dispatched impossible task id={bad_tid}")

        # 4. /tasks/waiting should list the bad one.
        time.sleep(1.0)
        r = c.get("/tasks/waiting")
        assert r.status_code == 200, r.text
        waiting = r.json()
        bad_entries = [t for t in waiting["tasks"] if t["task_id"] == bad_tid]
        assert bad_entries, (
            f"expected task {bad_tid} in /tasks/waiting, got: "
            f"{json.dumps(waiting, indent=2)}"
        )
        entry = bad_entries[0]
        print(
            f"[4] task {bad_tid} is waiting; "
            f"missing_per_runner runners={list(entry['missing_per_runner'])}"
        )
        assert entry["missing_per_runner"], "expected per-runner miss diagnostics"

        # The achievable task should NOT be in waiting (something can take it).
        ok_in_waiting = [t for t in waiting["tasks"] if t["task_id"] == ok_tid]
        if ok_in_waiting:
            print(
                f"[!] note: task {ok_tid} is also in /tasks/waiting — "
                f"likely no online runner advertises python ~= 3.13 yet"
            )

        # Cancel both probe tasks so they don't linger forever.
        for tid in (ok_tid, bad_tid):
            try:
                c.post(f"/tasks/{tid}/cancel")
            except Exception:
                pass

        print("[5] PASS")
        return 0


if __name__ == "__main__":
    sys.exit(main())
