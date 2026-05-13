"""Compatibility shim for the ForgeWire CLI package."""

from __future__ import annotations

from forgewire_fabric.cli import cli, main

__all__ = ["cli", "main"]


if __name__ == "__main__":
    main()
