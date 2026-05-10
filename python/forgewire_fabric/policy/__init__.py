"""Hub-side policy + budget gating (todo 114 M2.5.1 + M2.5.2).

Public surface re-exports the engine, ledger, and composed gate so the hub
state machine has a single import line.

Imported by :mod:`forgewire_fabric.hub.server` at request-handling time. The
consumer repo (forgewire) re-exports these symbols from
``core.services.cluster`` for backward compatibility.
"""

from __future__ import annotations

from .budget import (
    BudgetEnforcer,
    BudgetPolicy,
    BudgetViolation,
    CostLedger,
    CostRecord,
    TaskBudget,
)
from .engine import (
    CompletionRequest,
    DecisionKind,
    DispatchRequest,
    FabricPolicy,
    FabricPolicyEngine,
    IntentKind,
    PolicyDecision,
    PolicyViolation,
    TaskIntent,
    load_policy_from_mapping,
    load_policy_yaml,
)
from .gate import HubDispatchGate


__all__ = [
    "BudgetEnforcer",
    "BudgetPolicy",
    "BudgetViolation",
    "CompletionRequest",
    "CostLedger",
    "CostRecord",
    "DecisionKind",
    "DispatchRequest",
    "FabricPolicy",
    "FabricPolicyEngine",
    "HubDispatchGate",
    "IntentKind",
    "PolicyDecision",
    "PolicyViolation",
    "TaskBudget",
    "TaskIntent",
    "load_policy_from_mapping",
    "load_policy_yaml",
]
