"""
Human Handoff Package
──────────────────────
Exposes a single combined `router` for inclusion in main.py.

The router merges:
  - REST endpoints from router.py  (GET/POST routes)
  - WebSocket endpoints from websocket.py  (WS routes)

Usage in main.py:
    from app.api.routes.human import router as human_router
    app.include_router(human_router)
"""

from fastapi import APIRouter

from .connection_manager import manager
from .router import router as _rest_router
from .websocket import router as _ws_router

# Merge both sub-routers into a single router that main.py includes
router = APIRouter()
router.include_router(_rest_router)
router.include_router(_ws_router)
