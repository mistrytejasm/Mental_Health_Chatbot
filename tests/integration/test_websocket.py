import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

# Create a mock token data class
class MockTokenData:
    def __init__(self, user_id):
        self.user_id = user_id

@pytest.fixture
def override_verify_token(mocker):
    async def mock_verify(token, exc):
        if token == "valid_token":
            return MockTokenData(user_id="user123")
        raise exc
    mocker.patch("app.api.routes.human.websocket.verify_token", side_effect=mock_verify)

def test_chat_ws_rejects_without_token(test_client):
    try:
        with test_client.websocket_connect("/api/human/chat/sess1") as websocket:
            websocket.receive_text()
            assert False, "Should have disconnected"
    except WebSocketDisconnect:
        pass 

def test_chat_ws_rejects_invalid_token(test_client, override_verify_token):
    try:
        with test_client.websocket_connect("/api/human/chat/sess1?token=invalid") as websocket:
            websocket.receive_text()
            assert False, "Should have disconnected"
    except WebSocketDisconnect:
        pass 

def test_chat_ws_accepts_valid_user(test_client, override_verify_token, mock_db):
    # Setup mock DB to return a session document matching the user
    mock_db.sessions.find_one.return_value = {
        "session_id": "sess1",
        "user_id": "user123",
        "is_escalated": True
    }
    
    with test_client.websocket_connect("/api/human/chat/sess1?token=valid_token&role=user") as websocket:
        websocket.send_json({"type": "ping"})
        response = websocket.receive_json()
        assert response == {"type": "pong"}

def test_chat_ws_rejects_unassigned_counselor(test_client, override_verify_token, mock_db):
    # Setup mock DB: session assigned to someone else
    mock_db.sessions.find_one.return_value = {
        "session_id": "sess1",
        "user_id": "patient1",
        "is_escalated": True,
        "assigned_counselor_id": "other_counselor"
    }
    
    try:
        with test_client.websocket_connect("/api/human/chat/sess1?token=valid_token&role=human_counselor") as websocket:
            websocket.receive_text()
            assert False, "Should have disconnected"
    except WebSocketDisconnect:
        pass
