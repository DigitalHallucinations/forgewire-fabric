"""Shared helpers for the dispatcher and runner MCP servers.

Centralises:
* server construction with the standard MCP SDK style,
* tool registration helpers,
* a small JSON-arg unpack so each tool stays under 30 lines.
"""

from __future__ import annotations

import json
from typing import Any
from collections.abc import Callable, Awaitable

from mcp.server import Server
from mcp.types import TextContent, Tool


def make_text_result(payload: Any) -> list[TextContent]:
    """Serialize a tool result to a single TextContent block."""
    if isinstance(payload, (dict, list)):
        body = json.dumps(payload, indent=2, default=str)
    else:
        body = str(payload)
    return [TextContent(type="text", text=body)]


ToolHandler = Callable[[dict[str, Any]], Awaitable[Any]]


class ToolRegistry:
    """Collects (Tool, handler) pairs and binds them onto an MCP Server."""

    def __init__(self) -> None:
        self._tools: list[tuple[Tool, ToolHandler]] = []

    def register(
        self,
        *,
        name: str,
        description: str,
        input_schema: dict[str, Any],
        handler: ToolHandler,
    ) -> None:
        tool = Tool(name=name, description=description, inputSchema=input_schema)
        self._tools.append((tool, handler))

    def bind(self, server: Server) -> None:
        tools = [t for t, _ in self._tools]
        handlers: dict[str, ToolHandler] = {t.name: h for t, h in self._tools}

        @server.list_tools()
        async def _list_tools() -> list[Tool]:
            return list(tools)

        @server.call_tool()
        async def _call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
            handler = handlers.get(name)
            if handler is None:
                raise ValueError(f"unknown tool: {name}")
            result = await handler(arguments or {})
            return make_text_result(result)
