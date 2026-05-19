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

def test_disconnect_removes_socket(manager, mocker):
    mock_ws = MagicMock()
    manager.rooms["sess1"] = [mock_ws]
    
    # Mock human presence to avoid DB/Redis mock issues
    mocker.patch.object(manager, "human_has_joined", return_value=AsyncMock(return_value=True))
    
    async def run_disconnect():
        await manager.disconnect("sess1", mock_ws)
        
    import asyncio
    asyncio.run(run_disconnect())
    
    assert "sess1" not in manager.rooms

def test_mark_session_ended(manager):
    assert not manager.is_session_ended("sess1")
    manager.mark_session_ended("sess1")
    assert manager.is_session_ended("sess1")

@pytest.mark.asyncio
async def test_role_tracking(manager, mocker):
    mock_ws = MagicMock()
    
    # Mock Redis manager
    mock_redis = AsyncMock()
    mock_redis.hincrby = AsyncMock(return_value=1)
    
    async def mock_hget(key, field):
        if field == "user":
            return "1"
        return "0"
        
    mock_redis.hget = mock_hget
    mocker.patch("app.api.routes.human.connection_manager.get_redis", return_value=mock_redis)
    
    await manager.register_ws_role(mock_ws, "sess1", "user")
    
    # Needs a room context to check if role is in room
    manager.rooms["sess1"] = [mock_ws]
    
    assert await manager.is_role_in_room("sess1", "user") is True
    assert await manager.is_role_in_room("sess1", "human_counselor") is False
    
    await manager.unregister_ws_role(mock_ws, "sess1")
