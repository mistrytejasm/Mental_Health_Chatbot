import pytest
from unittest.mock import MagicMock, AsyncMock
from app.api.routes.human.connection_manager import ConnectionManager

@pytest.fixture
def manager():
    return ConnectionManager()

def test_manager_initial_state(manager):
    assert len(manager.rooms) == 0
    assert len(manager.dashboard_clients) == 0

@pytest.mark.asyncio
async def test_connect_accepts_socket(manager):
    mock_ws = MagicMock()
    mock_ws.accept = AsyncMock()
    
    await manager.connect("sess1", mock_ws)
    
    mock_ws.accept.assert_called_once()
    assert len(manager.rooms["sess1"]) == 1
    assert manager.rooms["sess1"][0] == mock_ws

def test_disconnect_removes_socket(manager):
    mock_ws = MagicMock()
    manager.rooms["sess1"] = [mock_ws]
    manager.has_human["sess1"] = True
    
    manager.disconnect("sess1", mock_ws)
    
    assert "sess1" not in manager.rooms
    assert "sess1" not in manager.has_human

def test_mark_session_ended(manager):
    assert not manager.is_session_ended("sess1")
    manager.mark_session_ended("sess1")
    assert manager.is_session_ended("sess1")

def test_role_tracking(manager):
    mock_ws = MagicMock()
    manager.register_ws_role(mock_ws, "user")
    
    # Needs a room context to check if role is in room
    manager.rooms["sess1"] = [mock_ws]
    
    assert manager.is_role_in_room("sess1", "user") is True
    assert manager.is_role_in_room("sess1", "human_counselor") is False
    
    manager.unregister_ws_role(mock_ws)
    assert manager.is_role_in_room("sess1", "user") is False
