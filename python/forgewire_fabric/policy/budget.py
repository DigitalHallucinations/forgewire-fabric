"""Cost ledger and hard-budget enforcement (todo 114 Phase 2.5 M2.5.2).

Companion to :mod:`core.services.cluster.fabric_policy`. The fabric
policy engine in M2.5.1 gates *what* a task may do; this module gates
*how much* a task is allowed to spend. Two stops:

* per-task ``max_cost_usd`` / ``max_tokens`` / ``max_wall_seconds``,
* per-day ``daily_budget_usd`` (cluster-wide hard cap).

Decisions are returned as
:class:`core.services.cluster.fabric_policy.PolicyDecision` records so
the hub can serialise and surface them through the same machinery as
M2.5.1 refusals. Violations are
:class:`BudgetViolation` (a ``PolicyViolation`` with a ``budget`` rule
prefix), preserving the structured-refusal contract.

The ledger keeps an in-memory record of every dispatch the hub has
charged so far. Day buckets use UTC (``YYYY-MM-DD``). Persistence is
out of scope for this slice — the operator CLI in M1.7 will wire a
SQLite-backed ledger when it lands.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any

from .engine import (
    DecisionKind,
    PolicyDecision,
    PolicyViolation,
)


__all__ = [
    "BudgetPolicy",
    "BudgetViolation",
    "CostLedger",
    "CostRecord",
    "TaskBudget",
    "BudgetEnforcer",
]


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CostRecord:
    """One charged dispatch."""

    task_id: str
    dispatch_id: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0
    wall_seconds: float = 0.0
    recorded_at: float = field(default_factory=time.time)

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    @property
    def day(self) -> str:
        return _to_day(self.recorded_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "dispatch_id": self.dispatch_id,
            "model": self.model,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "cost_usd": self.cost_usd,
            "wall_seconds": self.wall_seconds,
            "recorded_at": self.recorded_at,
            "day": self.day,
        }


@dataclass(frozen=True, slots=True)
class TaskBudget:
    """Per-task spend cap. ``None`` fields are unbounded."""

    max_cost_usd: float | None = None
    max_tokens: int | None = None
    max_wall_seconds: float | None = None

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "TaskBudget":
        if data is None:
            return cls()
        return cls(
            max_cost_usd=_opt_float(data.get("max_cost_usd")),
            max_tokens=_opt_int(data.get("max_tokens")),
            max_wall_seconds=_opt_float(data.get("max_wall_seconds")),
        )


@dataclass(frozen=True, slots=True)
class BudgetPolicy:
    """Hub-wide budget policy."""

    daily_budget_usd: float | None = None
    default_task_budget: TaskBudget = field(default_factory=TaskBudget)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "BudgetPolicy":
        if data is None:
            return cls()
        return cls(
            daily_budget_usd=_opt_float(data.get("daily_budget_usd")),
            default_task_budget=TaskBudget.from_mapping(
                data.get("default_task_budget")
            ),
        )


class BudgetViolation(PolicyViolation):
    """A :class:`PolicyViolation` with a ``budget.*`` rule name.

    Subclassed only so callers can branch on ``isinstance`` without
    re-checking the rule prefix string.
    """

    __slots__ = ()


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------


class CostLedger:
    """In-memory append-only ledger of :class:`CostRecord`."""

    def __init__(self) -> None:
        self._records: list[CostRecord] = []
        self._task_totals: dict[str, _Aggregate] = defaultdict(_Aggregate)
        self._day_totals: dict[str, _Aggregate] = defaultdict(_Aggregate)
        self._lock = threading.Lock()

    def record(self, entry: CostRecord) -> None:
        with self._lock:
            self._records.append(entry)
            self._task_totals[entry.task_id].add(entry)
            self._day_totals[entry.day].add(entry)

    def records(self) -> tuple[CostRecord, ...]:
        with self._lock:
            return tuple(self._records)

    def task_total_cost(self, task_id: str) -> float:
        return self._task_totals[task_id].cost_usd

    def task_total_tokens(self, task_id: str) -> int:
        return self._task_totals[task_id].tokens

    def task_total_wall(self, task_id: str) -> float:
        return self._task_totals[task_id].wall_seconds

    def daily_total_cost(self, day: str | None = None) -> float:
        key = day or _today()
        return self._day_totals[key].cost_usd

    def clear(self) -> None:
        with self._lock:
            self._records.clear()
            self._task_totals.clear()
            self._day_totals.clear()


@dataclass
class _Aggregate:
    cost_usd: float = 0.0
    tokens: int = 0
    wall_seconds: float = 0.0

    def add(self, record: CostRecord) -> None:
        self.cost_usd += record.cost_usd
        self.tokens += record.total_tokens
        self.wall_seconds += record.wall_seconds


# ---------------------------------------------------------------------------
# Enforcer
# ---------------------------------------------------------------------------


class BudgetEnforcer:
    """Pre/post-flight budget gate."""

    def __init__(
        self,
        *,
        ledger: CostLedger,
        policy: BudgetPolicy,
    ):
        self.ledger = ledger
        self.policy = policy

    # ---- pre-flight --------------------------------------------------

    def evaluate_dispatch(
        self,
        *,
        task_id: str,
        estimated_cost_usd: float = 0.0,
        estimated_tokens: int = 0,
        estimated_wall_seconds: float = 0.0,
        task_budget: TaskBudget | None = None,
        day: str | None = None,
    ) -> PolicyDecision:
        """Hard-deny if dispatch would breach a per-task or daily cap.

        ``estimated_*`` are upstream prompts at sealing time. Pass 0 to
        skip pre-flight projection and only use post-completion gates.
        """

        budget = task_budget or self.policy.default_task_budget
        violations: list[PolicyViolation] = []

        projected_cost = self.ledger.task_total_cost(task_id) + estimated_cost_usd
        projected_tokens = self.ledger.task_total_tokens(task_id) + estimated_tokens
        projected_wall = self.ledger.task_total_wall(task_id) + estimated_wall_seconds

        if budget.max_cost_usd is not None and projected_cost > budget.max_cost_usd:
            violations.append(
                BudgetViolation(
                    rule="budget.task.max_cost_usd",
                    value=budget.max_cost_usd,
                    observed=projected_cost,
                    message=(
                        f"task {task_id!r} would exceed max_cost_usd"
                        f" (projected={projected_cost:.4f} > cap={budget.max_cost_usd:.4f})"
                    ),
                )
            )
        if budget.max_tokens is not None and projected_tokens > budget.max_tokens:
            violations.append(
                BudgetViolation(
                    rule="budget.task.max_tokens",
                    value=budget.max_tokens,
                    observed=projected_tokens,
                    message=(
                        f"task {task_id!r} would exceed max_tokens"
                        f" (projected={projected_tokens} > cap={budget.max_tokens})"
                    ),
                )
            )
        if budget.max_wall_seconds is not None and projected_wall > budget.max_wall_seconds:
            violations.append(
                BudgetViolation(
                    rule="budget.task.max_wall_seconds",
                    value=budget.max_wall_seconds,
                    observed=projected_wall,
                    message=(
                        f"task {task_id!r} would exceed max_wall_seconds"
                        f" (projected={projected_wall:.2f} > cap={budget.max_wall_seconds:.2f})"
                    ),
                )
            )

        if self.policy.daily_budget_usd is not None:
            day_key = day or _today()
            projected_day = (
                self.ledger.daily_total_cost(day_key) + estimated_cost_usd
            )
            if projected_day > self.policy.daily_budget_usd:
                violations.append(
                    BudgetViolation(
                        rule="budget.daily_budget_usd",
                        value=self.policy.daily_budget_usd,
                        observed=projected_day,
                        message=(
                            f"day {day_key!r} would exceed daily_budget_usd"
                            f" (projected={projected_day:.4f} > cap={self.policy.daily_budget_usd:.4f})"
                        ),
                    )
                )

        if violations:
            return PolicyDecision(
                decision=DecisionKind.DENY,
                violations=tuple(violations),
                rule_name="budget",
                reason="dispatch would exceed budget",
            )
        return PolicyDecision(decision=DecisionKind.ALLOW)

    # ---- post-flight -------------------------------------------------

    def evaluate_completion(
        self,
        *,
        task_id: str,
        task_budget: TaskBudget | None = None,
        day: str | None = None,
    ) -> PolicyDecision:
        """Deny on observed totals already-recorded in the ledger."""

        budget = task_budget or self.policy.default_task_budget
        violations: list[PolicyViolation] = []

        actual_cost = self.ledger.task_total_cost(task_id)
        actual_tokens = self.ledger.task_total_tokens(task_id)
        actual_wall = self.ledger.task_total_wall(task_id)

        if budget.max_cost_usd is not None and actual_cost > budget.max_cost_usd:
            violations.append(
                BudgetViolation(
                    rule="budget.task.max_cost_usd",
                    value=budget.max_cost_usd,
                    observed=actual_cost,
                    message=(
                        f"task {task_id!r} exceeded max_cost_usd"
                        f" (actual={actual_cost:.4f} > cap={budget.max_cost_usd:.4f})"
                    ),
                )
            )
        if budget.max_tokens is not None and actual_tokens > budget.max_tokens:
            violations.append(
                BudgetViolation(
                    rule="budget.task.max_tokens",
                    value=budget.max_tokens,
                    observed=actual_tokens,
                    message=(
                        f"task {task_id!r} exceeded max_tokens"
                        f" (actual={actual_tokens} > cap={budget.max_tokens})"
                    ),
                )
            )
        if budget.max_wall_seconds is not None and actual_wall > budget.max_wall_seconds:
            violations.append(
                BudgetViolation(
                    rule="budget.task.max_wall_seconds",
                    value=budget.max_wall_seconds,
                    observed=actual_wall,
                    message=(
                        f"task {task_id!r} exceeded max_wall_seconds"
                        f" (actual={actual_wall:.2f} > cap={budget.max_wall_seconds:.2f})"
                    ),
                )
            )

        if self.policy.daily_budget_usd is not None:
            day_key = day or _today()
            actual_day = self.ledger.daily_total_cost(day_key)
            if actual_day > self.policy.daily_budget_usd:
                violations.append(
                    BudgetViolation(
                        rule="budget.daily_budget_usd",
                        value=self.policy.daily_budget_usd,
                        observed=actual_day,
                        message=(
                            f"day {day_key!r} exceeded daily_budget_usd"
                            f" (actual={actual_day:.4f} > cap={self.policy.daily_budget_usd:.4f})"
                        ),
                    )
                )

        if violations:
            return PolicyDecision(
                decision=DecisionKind.DENY,
                violations=tuple(violations),
                rule_name="budget",
                reason="task exceeded budget",
            )
        return PolicyDecision(decision=DecisionKind.ALLOW)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _today() -> str:
    return _to_day(time.time())


def _to_day(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")


def _opt_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _opt_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _records_iter(records: Iterable[CostRecord]) -> tuple[CostRecord, ...]:  # pragma: no cover
    return tuple(records)
