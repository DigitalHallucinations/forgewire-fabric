"""One-off: convert FastAPI route handlers in hub/server.py from async def to def
unless they contain an `await`. Excludes require_auth and the inner stream() generator."""
import re
from pathlib import Path

KEEP_ASYNC = {"require_auth", "stream", "_install_loop_watchdog", "state_import"}

p = Path(r"c:\Projects\forgewire-fabric\python\forgewire_fabric\hub\server.py")
src = p.read_text(encoding="utf-8")
lines = src.splitlines(keepends=True)

# Find all "async def NAME(" lines and the function body extent.
def find_function_body(start: int) -> int:
    """Return end index (exclusive) of the function body starting at start."""
    # Determine indent of `async def` line
    line = lines[start]
    indent = len(line) - len(line.lstrip())
    body_indent = indent + 4
    i = start + 1
    while i < len(lines):
        ln = lines[i]
        if ln.strip() == "":
            i += 1
            continue
        cur_indent = len(ln) - len(ln.lstrip())
        if cur_indent <= indent and ln.strip():
            return i
        i += 1
    return i

changes = 0
i = 0
out_lines = list(lines)
while i < len(out_lines):
    m = re.match(r"^(\s*)async def (\w+)\b", out_lines[i])
    if not m:
        i += 1
        continue
    indent_s, name = m.group(1), m.group(2)
    if name in KEEP_ASYNC:
        i += 1
        continue
    body_end = find_function_body(i)
    body_text = "".join(out_lines[i:body_end])
    # If the body contains any `await ` outside of nested `async def` blocks we
    # cannot convert. We do a conservative check: ignore lines inside a nested
    # async def (deeper indent).
    skip = False
    nested_async_indent = None
    for j in range(i + 1, body_end):
        ln = out_lines[j]
        stripped = ln.lstrip()
        if not stripped:
            continue
        cur_indent = len(ln) - len(stripped)
        if nested_async_indent is not None:
            if cur_indent <= nested_async_indent:
                nested_async_indent = None
            else:
                continue
        if re.match(r"async def \w+", stripped):
            nested_async_indent = cur_indent
            continue
        # Match awaits at this scope
        if re.search(r"\bawait\b", ln):
            skip = True
            break
    if skip:
        i = body_end
        continue
    # Replace `async def` with `def` on this line
    out_lines[i] = re.sub(r"^(\s*)async def ", r"\1def ", out_lines[i], count=1)
    changes += 1
    i = body_end

p.write_text("".join(out_lines), encoding="utf-8")
print(f"converted {changes} handlers")
