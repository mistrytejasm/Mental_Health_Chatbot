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
    pubsub_mock.listen = MagicMock(return_value=asyncio.sleep(100)) # dummy listen
    
    redis_mock.pubsub = MagicMock(return_value=pubsub_mock)
    
    mocker.patch("app.core.redis.get_redis", return_value=redis_mock)
    mocker.patch("app.api.routes.human.connection_manager.get_redis", return_value=redis_mock)
    return redis_mock

@pytest.fixture
def test_client(mock_redis):
    return TestClient(app)
