"""Scan server.py for SELECT/fetch calls inside BEGIN IMMEDIATE/COMMIT blocks.

Walks the AST so that each method body is analyzed independently and the
"inside transaction" state is reset at function boundaries. Reports any
read that happens between an `execute("BEGIN IMMEDIATE")` and the next
`execute("COMMIT")` (or `execute("ROLLBACK")`) within the same function.
"""
from __future__ import annotations

import ast
import pathlib


def _is_call_to(node: ast.AST, attr: str) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == attr
    )


def _first_str_arg(call: ast.Call) -> str | None:
    if not call.args:
        return None
    a0 = call.args[0]
    if isinstance(a0, ast.Constant) and isinstance(a0.value, str):
        return a0.value
    return None


def main() -> int:
    src_path = pathlib.Path("python/forgewire_fabric/hub/server.py")
    tree = ast.parse(src_path.read_text(encoding="utf-8"))

    hits: list[tuple[int, str]] = []

    def visit_function(fn: ast.AST) -> None:
        state = "outside"
        # ast.walk doesn't preserve source order across siblings; iterate
        # statements in lexical order via ast.iter_child_nodes recursion.
        ordered: list[ast.AST] = []

        def collect(n: ast.AST) -> None:
            for child in ast.iter_child_nodes(n):
                ordered.append(child)
                collect(child)

        collect(fn)
        ordered.sort(key=lambda n: getattr(n, "lineno", 0))

        for node in ordered:
            if _is_call_to(node, "execute"):
                sql = _first_str_arg(node) or ""
                head = sql.strip().upper()
                if head.startswith("BEGIN"):
                    state = "inside"
                    continue
                if head.startswith("COMMIT") or head.startswith("ROLLBACK"):
                    state = "outside"
                    continue
                if state == "inside" and "SELECT" in head:
                    hits.append(
                        (node.lineno, sql.replace("\n", " ").strip()[:100])
                    )
            elif _is_call_to(node, "fetchone") or _is_call_to(node, "fetchall"):
                if state == "inside":
                    hits.append((node.lineno, "<fetch* inside tx>"))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            visit_function(node)

    if not hits:
        print("OK: no SELECT/fetch inside BEGIN/COMMIT blocks.")
        return 0
    print(f"Found {len(hits)} read(s) inside same-function transactions:")
    for ln, s in hits:
        print(f"  L{ln}: {s}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
