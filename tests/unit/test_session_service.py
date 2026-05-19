import pytest
from unittest.mock import AsyncMock
from app.services.session_service import session_service

@pytest.mark.asyncio
async def test_process_manual_escalation_no_session(mocker):
    mocker.patch("app.services.session_service.get_existing_session", return_value=None)
    result = await session_service.process_manual_escalation("user123")
    assert result == {"status": "failed", "message": "No active chat session found to escalate."}

@pytest.mark.asyncio
async def test_process_manual_escalation_no_counselor(mocker, mock_db):
    mocker.patch("app.services.session_service.get_existing_session", return_value={"session_id": "sess123"})
    mocker.patch("app.services.session_service.get_available_counselor_count", return_value=0)
    result = await session_service.process_manual_escalation("user123")
    assert result["status"] == "failed"
    assert "No counselor is available" in result["message"]
    mock_db.messages.insert_one.assert_called_once()

@pytest.mark.asyncio
async def test_process_manual_escalation_success(mocker, mock_db):
    mocker.patch("app.services.session_service.get_existing_session", return_value={"session_id": "sess123"})
    mocker.patch("app.services.session_service.get_available_counselor_count", return_value=1)
    
    # Mock find_one_and_update to simulate lock acquisition success
    mock_db.sessions.find_one_and_update.return_value = {"session_id": "sess123"}
    # Mock find_one to return final successful assignment
    mock_db.sessions.find_one.return_value = {"session_id": "sess123", "assigned_counselor_id": "counselor123"}
    
    mock_route = mocker.patch("app.services.session_service.route_crisis_session", new_callable=AsyncMock)
    
    result = await session_service.process_manual_escalation("user123")
    assert result == {"status": "success"}
    mock_route.assert_called_once()

def test_build_recent_history_string():
    history = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "hi there"},
        {"role": "user", "content": "I am sad"},
        {"role": "assistant", "content": "I hear you"}
    ]
    result = session_service.build_recent_history_string(history, n_turns=1)
    # n_turns=1 means last 2 messages
    assert "User: I am sad" in result
    assert "MindBuddy: I hear you" in result
    assert "hello" not in result

def test_safe_fallback_consensus():
    result = session_service.safe_fallback_consensus()
    assert result["llm_sentiment"] == "neutral"
    assert result["is_crisis"] is False
