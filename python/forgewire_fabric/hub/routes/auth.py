"""Reserved authentication route module.

Authentication is currently a shared dependency rather than a public route surface.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()
