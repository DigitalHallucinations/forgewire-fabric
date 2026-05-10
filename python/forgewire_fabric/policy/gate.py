"""Composed hub dispatch gate (todo 114 M2.5.1 + M2.5.2).

This module is the single entry point the hub state machine calls when it
needs to decide whether a sealed-brief dispatch is allowed. It composes the
two structured evaluators that already exist:

* :class:`FabricPolicyEngine` — `policy.yaml` enforcement.
* :class:`BudgetEnforcer`     — per-task and daily budget caps.

The composition order is **policy first, budget second**: a brief that
violates `forbidden_paths` is denied even if it would fit the budget. If
policy returns ``REQUIRE_APPROVAL`` and budget says ``ALLOW``, the result
is ``REQUIRE_APPROVAL``; if either says ``DENY``, the result is ``DENY``.
Completion uses the same merging rules.

The gate is intentionally pure: callers pass the `DispatchRequest` /
`CompletionRequest` they already hold, and receive a single combined
:class:`PolicyDecision`. No state is mutated. Recording cost into the
ledger happens upstream when the runner reports actuals; the gate only
reads.
"""

from __future__ import annotations

from dataclasses import dataclass

from forgewire_fabric.policy.budget import BudgetEnforcer, TaskBudget
from forgewire_fabric.policy.engine import (
    CompletionRequest,
    DecisionKind,
    DispatchRequest,
    FabricPolicyEngine,
    PolicyDecision,
    PolicyViolation,
    TaskIntent,
)


__all__ = ["HubDispatchGate"]


def _merge(*decisions: PolicyDecision) -> PolicyDecision:
    """Combine multiple decisions into the strictest single decision.

    Ordering: ``DENY`` > ``REQUIRE_APPROVAL`` > ``ALLOW``. All violations
    from contributing decisions are concatenated so the caller has full
    structured context for the refusal.
    """

    if not decisions:
        return PolicyDecision(decision=DecisionKind.ALLOW)

    violations: list[PolicyViolation] = []
    rules: list[str] = []
    reasons: list[str] = []
    worst = DecisionKind.ALLOW
    for d in decisions:
        violations.extend(d.violations)
        if d.rule_name:
            rules.append(d.rule_name)
        if d.reason:
            reasons.append(d.reason)
        if d.decision is DecisionKind.DENY:
            worst = DecisionKind.DENY
        elif d.decision is DecisionKind.REQUIRE_APPROVAL and worst is not DecisionKind.DENY:
            worst = DecisionKind.REQUIRE_APPROVAL

    if worst is DecisionKind.ALLOW:
        return PolicyDecision(decision=DecisionKind.ALLOW)

    return PolicyDecision(
        decision=worst,
        violations=tuple(violations),
        rule_name="+".join(sorted(set(rules))) if rules else None,
        reason="; ".join(reasons) if reasons else "",
    )


@dataclass(frozen=True, slots=True)
class HubDispatchGate:
    """Combined policy + budget gate for the hub state machine."""

    policy_engine: FabricPolicyEngine
    budget_enforcer: BudgetEnforcer

    # ---- dispatch ----------------------------------------------------

    def evaluate_dispatch(
        self,
        request: DispatchRequest,
        *,
        estimated_cost_usd: float = 0.0,
        estimated_tokens: int = 0,
        estimated_wall_seconds: float = 0.0,
        task_budget: TaskBudget | None = None,
        day: str | None = None,
    ) -> PolicyDecision:
        policy_decision = self.policy_engine.evaluate_dispatch(request)
        budget_decision = self.budget_enforcer.evaluate_dispatch(
            task_id=request.task_id,
            estimated_cost_usd=estimated_cost_usd,
            estimated_tokens=estimated_tokens,
            estimated_wall_seconds=estimated_wall_seconds,
            task_budget=task_budget,
            day=day,
        )
        return _merge(policy_decision, budget_decision)

    # ---- intent ------------------------------------------------------

    def evaluate_intent(self, intent: TaskIntent) -> PolicyDecision:
        # Budget is not consulted on intents — intent enforcement is
        # purely policy-driven. Budgets are checked at dispatch/completion.
        return self.policy_engine.evaluate_intent(intent)

    # ---- completion --------------------------------------------------

    def evaluate_completion(
        self,
        request: CompletionRequest,
        *,
        task_budget: TaskBudget | None = None,
        day: str | None = None,
    ) -> PolicyDecision:
        policy_decision = self.policy_engine.evaluate_completion(request)
        budget_decision = self.budget_enforcer.evaluate_completion(
            task_id=request.task_id,
            task_budget=task_budget,
            day=day,
        )
        return _merge(policy_decision, budget_decision)
