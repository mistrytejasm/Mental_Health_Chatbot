"""
Connection Registry
────────────────────
In-process reference-counted registry of counselor IDs that have at least one
active WebSocket connection on THIS server process.

A reference count is used instead of a plain set so that a counselor who has
BOTH a dashboard connection AND a patient-room connection is not evicted from
the registry when only one of those connections closes.

human.py calls mark_counselor_connected() on every join (dashboard + patient room)
and mark_counselor_disconnected() on every disconnect.
routing_service.py calls is_counselor_connected() before confirming availability.
"""

_connected: dict[str, int] = {}


def mark_counselor_connected(counselor_id: str) -> None:
    _connected[counselor_id] = _connected.get(counselor_id, 0) + 1


def mark_counselor_disconnected(counselor_id: str) -> None:
    count = _connected.get(counselor_id, 0) - 1
    if count <= 0:
        _connected.pop(counselor_id, None)
    else:
        _connected[counselor_id] = count


def is_counselor_connected(counselor_id: str) -> bool:
    return _connected.get(counselor_id, 0) > 0


def force_counselor_connected(counselor_id: str) -> None:
    """Ensures a counselor is marked as connected without infinitely incrementing the counter."""
    if _connected.get(counselor_id, 0) == 0:
        _connected[counselor_id] = 1
