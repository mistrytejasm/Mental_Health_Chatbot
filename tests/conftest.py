import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient
from app.main import app

@pytest.fixture
def mock_db(mocker):
    db_mock = MagicMock()
    db_mock.sessions = AsyncMock()
    db_mock.messages = AsyncMock()
    db_mock.admins = AsyncMock()
    db_mock.users = AsyncMock()
    db_mock.doctor_user_assignments = AsyncMock()
    mocker.patch("app.core.database.get_database", return_value=db_mock)
    mocker.patch("app.api.routes.human.websocket.get_database", return_value=db_mock)
    mocker.patch("app.api.routes.chat.get_database", return_value=db_mock)
    mocker.patch("app.services.session_service.get_database", return_value=db_mock)
    mocker.patch("app.services.db_service.get_database", return_value=db_mock)
    return db_mock

@pytest.fixture
def mock_redis(mocker):
    redis_mock = AsyncMock()
    # For pubsub
    pubsub_mock = AsyncMock()
    pubsub_mock.subscribe = AsyncMock()
    pubsub_mock.psubscribe = AsyncMock()
    pubsub_mock.unsubscribe = AsyncMock()
    pubsub_mock.punsubscribe = AsyncMock()
    pubsub_mock.close = AsyncMock()
    
    async def dummy_listen():
        # Yield a single message or just sleep to represent an empty listener
        await asyncio.sleep(100)
        yield {"type": "message", "data": '{"action": "broadcast", "payload": {}}'}
    
    pubsub_mock.listen = dummy_listen
    redis_mock.pubsub = MagicMock(return_value=pubsub_mock)
    
    # Mock hincrby to return an integer so type checks (like count <= 0) don't fail
    redis_mock.hincrby = AsyncMock(return_value=1)
    redis_mock.hget = AsyncMock(return_value=1)
    
    mocker.patch("app.core.redis.get_redis", return_value=redis_mock)
    mocker.patch("app.api.routes.human.connection_manager.get_redis", return_value=redis_mock)
    return redis_mock

@pytest.fixture
def test_client(mock_redis):
    return TestClient(app)
