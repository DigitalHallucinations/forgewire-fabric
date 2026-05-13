"""Capability expression parser + evaluator (M2.5.4).

A brief can declare ``required_capabilities``: a list of small predicate
strings. The hub evaluates them against a runner's structured capability
blob. A runner is eligible iff every predicate evaluates True.

Supported predicate shapes
--------------------------

    "<dotted.path>"                     -- presence: path resolves to a
                                          truthy scalar OR a non-empty
                                          list/dict/string.

    "<dotted.path> <op> <literal>"      -- comparison.

Operators::

    ==   !=   >=   >   <=   <   ~=   in

Where ``~=`` is PEP 440 "compatible release": ``X ~= 3.12`` means the
major matches AND the minor is ``>= 3.12``. ``in`` only makes sense
for list-valued capabilities (e.g. ``"cuda" in gpu``).

Literals can be bare ints/floats, bare strings (``windows-11``),
quoted strings (``"windows-11"`` or ``'windows-11'``), or dotted
version strings (``3.12.4``).

Dotted paths walk dicts (``cpu.cores``) and treat list-valued nodes as
sets for ``in`` / presence (``toolchains.rust`` is shorthand for
``"rust" in toolchains``).

Design notes
------------

* Pure stdlib. The hub already ships fastapi/pydantic but nothing else.
* Errors are returned as ``(False, "<reason>")`` rather than raised so
  one bad predicate doesn't take down the matcher loop. A predicate
  that fails to parse is treated as a non-match with a structured
  reason that surfaces on ``/tasks/waiting``.
* No regex backtracking on attacker input: predicate length is capped
  by callers (the brief schema rejects overlong values).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from collections.abc import Iterable, Mapping


_OPS = ("==", "!=", ">=", "<=", ">", "<", "~=", " in ")


# ---------------------------------------------------------------- parsing


@dataclass(frozen=True)
class Predicate:
    """Parsed form of one ``required_capabilities`` entry."""

    raw: str
    path: str
    op: str | None        # None means presence-only
    literal: Any | None   # None for presence-only; parsed value otherwise


def parse(expr: str) -> Predicate:
    text = (expr or "").strip()
    if not text:
        raise ValueError("empty capability predicate")
    # Find the first matching operator. Order matters so ``>=`` wins
    # over ``>`` and ``~=`` over ``=``.
    for op in _OPS:
        idx = text.find(op)
        if idx == -1:
            continue
        path = text[:idx].strip()
        rhs = text[idx + len(op) :].strip()
        if op == " in ":
            # In ``"<value>" in <path>`` the LHS is the literal, the
            # RHS is the dotted path. Normalise to (path=rhs,
            # literal=lhs, op="in").
            return Predicate(raw=text, path=rhs, op="in", literal=_parse_literal(path))
        return Predicate(raw=text, path=path, op=op.strip(), literal=_parse_literal(rhs))
    return Predicate(raw=text, path=text, op=None, literal=None)


def _parse_literal(token: str) -> Any:
    t = token.strip()
    if (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'")):
        return t[1:-1]
    # int
    try:
        if "." not in t and "-" not in t[1:]:
            return int(t)
    except ValueError:
        pass
    # float
    try:
        if t.count(".") == 1 and all(p.isdigit() for p in t.split(".")):
            return float(t)
    except ValueError:
        pass
    # bare string / version: leave as-is
    return t


# --------------------------------------------------------------- evaluation


def resolve(caps: Mapping[str, Any], path: str) -> Any:
    """Walk ``caps`` along a dotted ``path``. Returns ``None`` on miss.

    Two convenience semantics on top of plain dict-walking:

    * If a node is a list and the next path segment is a string, we
      treat the list as a set: ``toolchains.rust`` returns ``True``
      iff ``"rust" in toolchains``.
    * If a node is a string and the next path segment matches a
      well-known suffix (``cuda``), we substring-match the string:
      ``gpu.cuda`` over ``gpu="nvidia:cuda:12.4"`` returns the
      version-looking suffix ``12.4`` so ``>=`` predicates work.
    """
    cur: Any = caps
    parts = [p for p in path.split(".") if p]
    for part in parts:
        if isinstance(cur, Mapping):
            cur = cur.get(part)
        elif isinstance(cur, list):
            # set-membership shorthand for the FIRST list-segment
            return part in cur
        elif isinstance(cur, str):
            # cuda subkey on a flat label: hunt for "<part>:<ver>" or
            # "<part>-<ver>" and return the version suffix so version
            # comparisons work.
            label = cur.lower()
            target = part.lower()
            if target in label:
                tail = label.split(target, 1)[1].lstrip(":-")
                ver = tail.split()[0].rstrip(",;)") if tail else ""
                return ver or True
            return None
        else:
            return None
        if cur is None:
            return None
    return cur


def evaluate(predicate: Predicate, caps: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Return ``(ok, reason_if_not_ok)``."""
    value = resolve(caps, predicate.path)
    if predicate.op is None:
        # presence-only
        if value is None or value is False:
            return False, f"missing {predicate.path}"
        if isinstance(value, (list, dict, str)) and len(value) == 0:
            return False, f"empty {predicate.path}"
        return True, None
    if value is None:
        return False, f"missing {predicate.path}"
    op = predicate.op
    lit = predicate.literal
    try:
        if op == "in":
            if not isinstance(value, (list, tuple, set, str)):
                return False, f"{predicate.path} not iterable"
            return (lit in value, None if lit in value else f"{lit!r} not in {predicate.path}")
        if op == "==":
            return (str(value) == str(lit), None if str(value) == str(lit) else f"{predicate.path}={value!r} != {lit!r}")
        if op == "!=":
            return (str(value) != str(lit), None if str(value) != str(lit) else f"{predicate.path}={value!r} == {lit!r}")
        if op == "~=":
            return _compatible_release(value, lit, predicate.path)
        if op in (">=", ">", "<=", "<"):
            return _ordered(value, lit, op, predicate.path)
    except Exception as exc:  # noqa: BLE001 - any parse error -> structured no-match
        return False, f"eval {predicate.raw!r}: {exc}"
    return False, f"unknown op {op}"


def _version_tuple(value: Any) -> tuple[int, ...] | None:
    s = str(value).strip()
    if not s:
        return None
    parts = s.split(".")
    out: list[int] = []
    for p in parts:
        # Strip non-numeric trailing junk like "1.84-stable" -> 1, 84.
        digits = "".join(c for c in p if c.isdigit())
        if not digits:
            return None
        out.append(int(digits))
    return tuple(out) if out else None


def _ordered(value: Any, lit: Any, op: str, path: str) -> tuple[bool, str | None]:
    # Numeric fast path.
    if isinstance(value, (int, float)) and isinstance(lit, (int, float)):
        ok = _cmp(value, lit, op)
        return ok, None if ok else f"{path}={value} {op} {lit} failed"
    # Otherwise version-compare.
    a = _version_tuple(value)
    b = _version_tuple(lit)
    if a is None or b is None:
        return False, f"{path}={value!r} not orderable against {lit!r}"
    # Right-pad the shorter tuple so 3.12 < 3.12.4.
    width = max(len(a), len(b))
    a2 = a + (0,) * (width - len(a))
    b2 = b + (0,) * (width - len(b))
    ok = _cmp(a2, b2, op)
    return ok, None if ok else f"{path}={value!r} {op} {lit!r} failed"


def _cmp(a: Any, b: Any, op: str) -> bool:
    if op == ">=":
        return a >= b
    if op == ">":
        return a > b
    if op == "<=":
        return a <= b
    if op == "<":
        return a < b
    return False


def _compatible_release(value: Any, lit: Any, path: str) -> tuple[bool, str | None]:
    a = _version_tuple(value)
    b = _version_tuple(lit)
    if a is None or b is None or len(b) < 2:
        return False, f"{path}={value!r} ~= {lit!r} requires X.Y literal"
    # Same major, minor >= literal.minor (and full >= literal).
    if a[0] != b[0]:
        return False, f"{path} major {a[0]} != {b[0]}"
    width = max(len(a), len(b))
    a2 = a + (0,) * (width - len(a))
    b2 = b + (0,) * (width - len(b))
    ok = a2 >= b2
    return ok, None if ok else f"{path}={value!r} < {lit!r}"


# ---------------------------------------------------------------- public API


def match(
    required: Iterable[str] | None,
    caps: Mapping[str, Any] | None,
) -> tuple[bool, list[str]]:
    """Evaluate a list of required-capability strings against a caps blob.

    Returns ``(ok, missing)``. ``missing`` is the list of structured
    reasons (one per failed predicate) suitable for surfacing on
    ``/tasks/waiting``.
    """
    reqs = list(required or [])
    if not reqs:
        return True, []
    caps_view: Mapping[str, Any] = caps or {}
    missing: list[str] = []
    for raw in reqs:
        try:
            pred = parse(raw)
        except ValueError as exc:
            missing.append(f"{raw!r}: {exc}")
            continue
        ok, reason = evaluate(pred, caps_view)
        if not ok:
            missing.append(reason or f"{raw!r}: not satisfied")
    return (len(missing) == 0), missing
