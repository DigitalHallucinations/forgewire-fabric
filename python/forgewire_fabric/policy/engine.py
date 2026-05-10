"""Fabric-side policy engine for hub task gating (todo 114 Phase 2.5 M2.5.1).

Provides the structured policy spine the hub consults to gate dispatched
tasks against `policy.yaml`. Three enforcement points are modelled:

* ``evaluate_dispatch`` — pre-flight gate at ``dispatch_task``. Rejects briefs
  whose ``scope_globs`` overlap forbidden paths, whose ``target_branch`` is
  protected without an approval, or whose intent matches a
  ``require_approval`` rule.
* ``evaluate_intent`` — runtime gate for streamed runner intents
  (``fs_write``, ``network_egress``, ``shell_exec``, ``destructive_fs``,
  ``merge``, ``push``).
* ``evaluate_completion`` — post-flight gate at ``complete_task``. Rejects
  diffs that touch forbidden paths or exceed ``max_diff_lines``.

Policy decisions are *structured*: each refusal/approval-gate carries a list
of :class:`PolicyViolation` records with the rule name, configured value, and
observed value, mirroring the requirement in the M2.5.1 spec that refusal is
machine-readable rather than a string.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Intents
# ---------------------------------------------------------------------------


class IntentKind(str, Enum):
    FS_WRITE = "fs_write"
    NETWORK_EGRESS = "network_egress"
    SHELL_EXEC = "shell_exec"
    DESTRUCTIVE_FS = "destructive_fs"
    MERGE = "merge"
    PUSH = "push"


@dataclass(frozen=True, slots=True)
class TaskIntent:
    """An intent-to-do event the runner streams to the hub."""

    kind: IntentKind
    paths: tuple[str, ...] = ()
    hosts: tuple[str, ...] = ()
    command: str | None = None
    workspace_root: str | None = None
    branch: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DispatchRequest:
    """Pre-flight context evaluated at ``dispatch_task``."""

    task_id: str
    scope_globs: Sequence[str]
    target_branch: str | None = None
    intents: Sequence[IntentKind] = ()
    dispatcher_id: str | None = None


@dataclass(frozen=True, slots=True)
class CompletionRequest:
    """Post-flight context evaluated at ``complete_task``."""

    task_id: str
    changed_paths: Sequence[str]
    diff_lines: int
    target_branch: str | None = None


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------


class DecisionKind(str, Enum):
    ALLOW = "allow"
    REQUIRE_APPROVAL = "require_approval"
    DENY = "deny"


@dataclass(frozen=True, slots=True)
class PolicyViolation:
    """Structured refusal record. Always machine-readable."""

    rule: str
    value: Any
    observed: Any
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "value": _jsonable(self.value),
            "observed": _jsonable(self.observed),
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    """Result of evaluating a request/intent against a policy."""

    decision: DecisionKind
    violations: tuple[PolicyViolation, ...] = ()
    rule_name: str | None = None
    reason: str = ""

    @property
    def allowed(self) -> bool:
        return self.decision is DecisionKind.ALLOW

    @property
    def needs_approval(self) -> bool:
        return self.decision is DecisionKind.REQUIRE_APPROVAL

    @property
    def denied(self) -> bool:
        return self.decision is DecisionKind.DENY

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision.value,
            "rule_name": self.rule_name,
            "reason": self.reason,
            "violations": [v.to_dict() for v in self.violations],
        }


# ---------------------------------------------------------------------------
# Policy document
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FabricPolicy:
    """In-memory representation of ``policy.yaml``."""

    forbidden_paths: tuple[str, ...] = ()
    protected_branches: tuple[str, ...] = ()
    require_approval: frozenset[IntentKind] = frozenset()
    max_diff_lines: int | None = None
    egress_allowlist: tuple[str, ...] | None = None
    workspace_required_for_shell: bool = True
    approvers: tuple[str, ...] = ()
    reviewers_required: int = 0

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "FabricPolicy":
        require_approval_raw = data.get("require_approval") or ()
        require_approval = frozenset(
            IntentKind(value) if not isinstance(value, IntentKind) else value
            for value in require_approval_raw
        )
        egress = data.get("egress_allowlist")
        return cls(
            forbidden_paths=tuple(str(p) for p in data.get("forbidden_paths") or ()),
            protected_branches=tuple(str(b) for b in data.get("protected_branches") or ()),
            require_approval=require_approval,
            max_diff_lines=int(data["max_diff_lines"]) if data.get("max_diff_lines") is not None else None,
            egress_allowlist=tuple(str(h) for h in egress) if egress is not None else None,
            workspace_required_for_shell=bool(data.get("workspace_required_for_shell", True)),
            approvers=tuple(str(a) for a in data.get("approvers") or ()),
            reviewers_required=int(data.get("reviewers_required") or 0),
        )


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class FabricPolicyEngine:
    """Evaluate dispatch / intent / completion requests against a policy."""

    def __init__(self, policy: FabricPolicy):
        self.policy = policy

    # ---- dispatch -----------------------------------------------------

    def evaluate_dispatch(self, request: DispatchRequest) -> PolicyDecision:
        violations: list[PolicyViolation] = []
        # forbidden paths overlap
        for glob in request.scope_globs:
            for forbidden in self.policy.forbidden_paths:
                if _scope_overlaps_forbidden(glob, forbidden):
                    violations.append(
                        PolicyViolation(
                            rule="forbidden_paths",
                            value=forbidden,
                            observed=glob,
                            message=f"scope glob {glob!r} overlaps forbidden path {forbidden!r}",
                        )
                    )
        if violations:
            return PolicyDecision(
                decision=DecisionKind.DENY,
                violations=tuple(violations),
                rule_name="forbidden_paths",
                reason="dispatch scope overlaps forbidden paths",
            )

        approval_violations: list[PolicyViolation] = []
        # protected branch
        if request.target_branch is not None and self._branch_is_protected(request.target_branch):
            approval_violations.append(
                PolicyViolation(
                    rule="protected_branches",
                    value=list(self.policy.protected_branches),
                    observed=request.target_branch,
                    message=f"branch {request.target_branch!r} is protected",
                )
            )
        # intents requiring approval
        for intent in request.intents:
            if intent in self.policy.require_approval:
                approval_violations.append(
                    PolicyViolation(
                        rule="require_approval",
                        value=intent.value,
                        observed=intent.value,
                        message=f"intent {intent.value!r} requires approval",
                    )
                )
        if approval_violations:
            return PolicyDecision(
                decision=DecisionKind.REQUIRE_APPROVAL,
                violations=tuple(approval_violations),
                rule_name="require_approval",
                reason="dispatch requires approval",
            )
        return PolicyDecision(decision=DecisionKind.ALLOW)

    # ---- intent ------------------------------------------------------

    def evaluate_intent(self, intent: TaskIntent) -> PolicyDecision:
        if intent.kind is IntentKind.FS_WRITE or intent.kind is IntentKind.DESTRUCTIVE_FS:
            forbidden_hits: list[PolicyViolation] = []
            for path in intent.paths:
                for forbidden in self.policy.forbidden_paths:
                    if _path_matches_glob(path, forbidden):
                        forbidden_hits.append(
                            PolicyViolation(
                                rule="forbidden_paths",
                                value=forbidden,
                                observed=path,
                                message=f"write to {path!r} matches forbidden path {forbidden!r}",
                            )
                        )
            if forbidden_hits:
                return PolicyDecision(
                    decision=DecisionKind.DENY,
                    violations=tuple(forbidden_hits),
                    rule_name="forbidden_paths",
                    reason="intent writes to forbidden path",
                )

        if intent.kind is IntentKind.NETWORK_EGRESS and self.policy.egress_allowlist is not None:
            disallowed = [
                host for host in intent.hosts if not _host_allowed(host, self.policy.egress_allowlist)
            ]
            if disallowed:
                violations = tuple(
                    PolicyViolation(
                        rule="egress_allowlist",
                        value=list(self.policy.egress_allowlist),
                        observed=host,
                        message=f"host {host!r} is not in egress allowlist",
                    )
                    for host in disallowed
                )
                if intent.kind in self.policy.require_approval:
                    return PolicyDecision(
                        decision=DecisionKind.REQUIRE_APPROVAL,
                        violations=violations,
                        rule_name="egress_allowlist",
                        reason="egress to non-allowlisted host requires approval",
                    )
                return PolicyDecision(
                    decision=DecisionKind.DENY,
                    violations=violations,
                    rule_name="egress_allowlist",
                    reason="egress to non-allowlisted host",
                )

        if (
            intent.kind is IntentKind.SHELL_EXEC
            and self.policy.workspace_required_for_shell
            and intent.workspace_root is not None
            and intent.command is not None
        ):
            # Heuristic: if any path argument escapes workspace_root, gate it.
            if any(_path_outside(p, intent.workspace_root) for p in intent.paths):
                if intent.kind in self.policy.require_approval:
                    return PolicyDecision(
                        decision=DecisionKind.REQUIRE_APPROVAL,
                        violations=(
                            PolicyViolation(
                                rule="shell_exec_outside_workspace",
                                value=intent.workspace_root,
                                observed=list(intent.paths),
                                message="shell touches paths outside workspace",
                            ),
                        ),
                        rule_name="shell_exec_outside_workspace",
                        reason="shell exec outside workspace requires approval",
                    )
                return PolicyDecision(
                    decision=DecisionKind.DENY,
                    violations=(
                        PolicyViolation(
                            rule="shell_exec_outside_workspace",
                            value=intent.workspace_root,
                            observed=list(intent.paths),
                            message="shell touches paths outside workspace",
                        ),
                    ),
                    rule_name="shell_exec_outside_workspace",
                    reason="shell exec outside workspace",
                )

        if intent.kind in (IntentKind.MERGE, IntentKind.PUSH) and intent.branch is not None:
            if self._branch_is_protected(intent.branch):
                return PolicyDecision(
                    decision=DecisionKind.REQUIRE_APPROVAL,
                    violations=(
                        PolicyViolation(
                            rule="protected_branches",
                            value=list(self.policy.protected_branches),
                            observed=intent.branch,
                            message=f"{intent.kind.value} to protected branch {intent.branch!r}",
                        ),
                    ),
                    rule_name="protected_branches",
                    reason=f"{intent.kind.value} to protected branch requires approval",
                )

        if intent.kind in self.policy.require_approval:
            return PolicyDecision(
                decision=DecisionKind.REQUIRE_APPROVAL,
                violations=(
                    PolicyViolation(
                        rule="require_approval",
                        value=intent.kind.value,
                        observed=intent.kind.value,
                        message=f"intent {intent.kind.value!r} requires approval",
                    ),
                ),
                rule_name="require_approval",
                reason=f"intent {intent.kind.value!r} requires approval",
            )

        return PolicyDecision(decision=DecisionKind.ALLOW)

    # ---- completion --------------------------------------------------

    def evaluate_completion(self, request: CompletionRequest) -> PolicyDecision:
        violations: list[PolicyViolation] = []
        for path in request.changed_paths:
            for forbidden in self.policy.forbidden_paths:
                if _path_matches_glob(path, forbidden):
                    violations.append(
                        PolicyViolation(
                            rule="forbidden_paths",
                            value=forbidden,
                            observed=path,
                            message=f"diff touches forbidden path {path!r}",
                        )
                    )
        if (
            self.policy.max_diff_lines is not None
            and request.diff_lines > self.policy.max_diff_lines
        ):
            violations.append(
                PolicyViolation(
                    rule="max_diff_lines",
                    value=self.policy.max_diff_lines,
                    observed=request.diff_lines,
                    message=f"diff has {request.diff_lines} lines, exceeds cap {self.policy.max_diff_lines}",
                )
            )
        if violations:
            return PolicyDecision(
                decision=DecisionKind.DENY,
                violations=tuple(violations),
                rule_name="completion",
                reason="completion violates policy",
            )
        if request.target_branch is not None and self._branch_is_protected(request.target_branch):
            return PolicyDecision(
                decision=DecisionKind.REQUIRE_APPROVAL,
                violations=(
                    PolicyViolation(
                        rule="protected_branches",
                        value=list(self.policy.protected_branches),
                        observed=request.target_branch,
                        message=f"completion targets protected branch {request.target_branch!r}",
                    ),
                ),
                rule_name="protected_branches",
                reason="merge to protected branch requires approval",
            )
        return PolicyDecision(decision=DecisionKind.ALLOW)

    # ---- internals ---------------------------------------------------

    def _branch_is_protected(self, branch: str) -> bool:
        return any(fnmatch.fnmatchcase(branch, pattern) for pattern in self.policy.protected_branches)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_policy_from_mapping(data: Mapping[str, Any]) -> FabricPolicy:
    return FabricPolicy.from_mapping(data)


def load_policy_yaml(path: str) -> FabricPolicy:
    """Load a ``policy.yaml`` from disk. Requires ``pyyaml`` to be installed."""

    import yaml  # local import: avoid hard dep at import time

    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, Mapping):
        raise ValueError(f"policy file {path!r} must be a YAML mapping at the top level")
    return FabricPolicy.from_mapping(raw)


# ---------------------------------------------------------------------------
# Glob helpers
# ---------------------------------------------------------------------------


def _path_matches_glob(path: str, glob: str) -> bool:
    """Match ``path`` against an fnmatch-style glob, handling ``**`` recursively."""

    if glob.endswith("/**"):
        prefix = glob[:-3].rstrip("/")
        return path == prefix or path.startswith(prefix + "/")
    if "**" in glob:
        # Translate '**' to fnmatch-friendly '*' segment-wise.
        translated = glob.replace("**", "*")
        return fnmatch.fnmatchcase(path, translated) or _path_matches_glob_recursive(path, glob)
    return fnmatch.fnmatchcase(path, glob)


def _path_matches_glob_recursive(path: str, glob: str) -> bool:
    glob_parts = glob.split("/")
    path_parts = path.split("/")
    return _match_parts(path_parts, glob_parts)


def _match_parts(path_parts: list[str], glob_parts: list[str]) -> bool:
    if not glob_parts:
        return not path_parts
    head, *rest = glob_parts
    if head == "**":
        if not rest:
            return True
        for index in range(len(path_parts) + 1):
            if _match_parts(path_parts[index:], rest):
                return True
        return False
    if not path_parts:
        return False
    if fnmatch.fnmatchcase(path_parts[0], head):
        return _match_parts(path_parts[1:], rest)
    return False


def _scope_overlaps_forbidden(scope_glob: str, forbidden_glob: str) -> bool:
    """Conservative overlap check: do two glob patterns share matchable paths?

    This is a structural check used to reject dispatches whose declared scope
    *could* touch forbidden paths. We treat a scope as overlapping if it
    matches the forbidden pattern as a literal path, or if either glob is a
    prefix of the other after normalising ``**`` segments.
    """

    if scope_glob == forbidden_glob:
        return True
    scope_norm = scope_glob.replace("**", "*").rstrip("/").rstrip("*").rstrip("/")
    forb_norm = forbidden_glob.replace("**", "*").rstrip("/").rstrip("*").rstrip("/")
    if not scope_norm or not forb_norm:
        return True
    if scope_norm.startswith(forb_norm) or forb_norm.startswith(scope_norm):
        return True
    return _path_matches_glob(forb_norm, scope_glob) or _path_matches_glob(scope_norm, forbidden_glob)


def _host_allowed(host: str, allowlist: Iterable[str]) -> bool:
    host_lower = host.lower()
    for pattern in allowlist:
        pattern_lower = pattern.lower()
        if pattern_lower == host_lower:
            return True
        if pattern_lower.startswith("*.") and host_lower.endswith(pattern_lower[1:]):
            return True
    return False


def _path_outside(path: str, root: str) -> bool:
    norm_path = path.replace("\\", "/")
    norm_root = root.replace("\\", "/").rstrip("/")
    if not norm_root:
        return False
    return not (norm_path == norm_root or norm_path.startswith(norm_root + "/"))


def _jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value
