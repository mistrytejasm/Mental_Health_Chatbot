import asyncio
import json
from datetime import datetime, timezone
from app.core.config import get_settings
from app.core.database import get_database
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

# Schemas
from app.api.schemas.request import StreamChatRequest
from app.api.schemas.response import (
    ChatHistoryResponse,
    ChatMessageResponse,
    SessionListResponse,
    SessionResponse,
    ManualEscalateResponse,
)

# Core
from app.core.auth.oauth2 import get_current_user
from app.core.logger import get_logger

# Services
from app.services import emotion as emotion_svc
from app.services import llm as llm_svc
from app.services.safety import synthesize_consensus
from app.services.db_service import (
    get_user_profile,
    get_formatted_history,
    save_message,
    generate_embedding,
    retrieve_long_term_memory,
    get_user_messages,
    get_all_sessions,
    escalate_session,
    is_user_escalated,
    get_existing_session,
    upsert_session,
)
from app.services.routing_service import get_available_counselor_count

logger = get_logger(__name__)
router = APIRouter(prefix="/api/chat", tags=["chat"])




from app.services.session_service import session_service


# ── Counselor Status API ────────────────────────────────────────────────────────

@router.get("/counselor-status")
async def get_counselor_status(current_user = Depends(get_current_user)):
    """
    Checks if there is at least one counselor online.
    Used by the frontend to determine if chat can be started.
    """
    count = await get_available_counselor_count()
    return {
        "status": "success",
        "is_counselor_online": count > 0,
        "online_counselors_count": count
    }


# ── Manual Escalation ─────────────────────────────────────────────────────────

@router.post("/manual-escalate", response_model=ManualEscalateResponse, response_model_exclude_none=True)
async def manual_escalate(
    current_user = Depends(get_current_user)
):
    """
    Triggered by the Android app when the user clicks 'Connect to Counselor'.
    Requires NO request body. Fetches the active session and routes to a human.
    """
    user_id = str(current_user.get("user_id") or current_user.get("_id"))
    
    result = await session_service.process_manual_escalation(user_id)
    return result


# ── SSE Stream ─────────────────────────────────────────────────────────────────

from fastapi import Request

@router.post("/stream")
async def stream_message(req: StreamChatRequest, request: Request, current_user = Depends(get_current_user)):
    """
    Main chat endpoint.
    FC7: user_id is extracted from the JWT token, not the request body.
    Fix 1: crisis detection returns a single-item SSE stream immediately,
           preventing AI chunks from being sent alongside the handoff message.
    Fix 19: escalation guard checks the specific session_id, not any session
            for the user, eliminating false blocks from older escalated sessions.
    """
    if current_user.get("role") == "admin":
        raise HTTPException(status_code=403, detail="Counselors cannot use the AI chat endpoint.")

    if not req.message.strip():
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    # FC7: identity from JWT only — client no longer sends user_id in body
    user_id = str(current_user.get("user_id") or current_user.get("_id"))

    # 1. Load profile from DB
    profile = await get_user_profile(user_id)

    actual_session_id = req.session_id

    # Ensure session document exists so escalate_session has a document to update
    await upsert_session(user_id, actual_session_id)

    # 1b. Fix 19: guard checks the SPECIFIC session_id, not any session for this user
    db_ref = get_database()
    current_session = None
    if db_ref is not None:
        current_session = await db_ref.sessions.find_one(
            {"session_id": actual_session_id, "user_id": user_id}
        )
    if current_session and current_session.get("is_escalated"):
        scheme = "wss" if request.url.scheme == "https" else "ws"
        ws_url = f"{scheme}://{request.url.netloc}/api/human/chat/{actual_session_id}"

        logger.info(f"[GUARD] Session {actual_session_id} for user {user_id} is escalated. Redirecting.")

        redirect_payload = {
            "done": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "escalation_active",
            "handoff_message": "You are currently connected to a human counselor. Please continue in the live chat.",
            "websocket_url": ws_url,
            "is_crisis": False,
        }

        async def _redirect_stream():
            yield f"data: {json.dumps(redirect_payload)}\n\n"

        return StreamingResponse(
            _redirect_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # 2. Load full conversation history
    history = await get_formatted_history(actual_session_id, limit=100)
    turn_count = len(history) // 2
    recent_history_str = session_service.build_recent_history_string(history, n_turns=4)

    logger.info("\n" + "═" * 70)
    logger.info(f"[STREAM] User: {user_id} | Session: {actual_session_id} | Turn: {turn_count}")
    logger.info(f"[USER]:  {req.message}")
    logger.info("═" * 70)

    # 3. Generate embedding and retrieve long-term memory (RAG)
    query_vector = await generate_embedding(req.message)
    long_term_memory = await retrieve_long_term_memory(
        user_id=user_id,
        query_vector=query_vector,
        exclude_session_id=actual_session_id,
    )

    # 4. Save user message
    await save_message({
        "session_id": actual_session_id,
        "user_id": user_id,
        "turn_number": turn_count + 1,
        "role": "user",
        "content": req.message,
    })

    # 5. RoBERTa emotion analysis
    emotion_result = None
    try:
        logger.info("[STEP 1] RoBERTa emotion analysis...")
        emotion_result = await emotion_svc.analyse(
            req.message,
            context_window=recent_history_str or None,
        )
        if emotion_result:
            logger.info(
                f"[STEP 1 OK] {emotion_result.dominant} | "
                f"top: {dict(list(emotion_result.scores.items())[:3])}"
            )
    except Exception as e:
        logger.error(f"[STEP 1 ERROR] {e}")

    sadness_now = emotion_result.scores.get("sadness", 0.0) if emotion_result else 0.0

    # 6. Consensus synthesis
    logger.info("[STEP 2] LLM Consensus Synthesizer...")
    try:
        consensus = await synthesize_consensus(
            text=req.message,
            roberta_emotion=emotion_result.dominant if emotion_result else "neutral",
            roberta_score=sadness_now,
            history=recent_history_str or None,
        )
        logger.info(
            f"[STEP 2 OK] crisis: {consensus.get('is_crisis')} | "
            f"wants_counselor: {consensus.get('wants_counselor')} | "
            f"category: {consensus.get('category')}"
        )
    except Exception as e:
        logger.error(f"[STEP 2 ERROR] {e}")
        consensus = session_service.safe_fallback_consensus()

    # ── Nuance handling now managed by context-aware LLM in synthesize_consensus ──
    is_message_crisis = bool(consensus.get("is_crisis", False))

    if consensus.get("wants_counselor") is True and not consensus.get("is_crisis"):
        logger.info(f"[COUNSELOR_REQUEST] User {user_id} requested a human counselor.")
        _cfg = get_settings()
        _base_url = f"http://{_cfg.SERVER_PUBLIC_HOST}:{_cfg.SERVER_PORT}"
        counselor_info_payload = {
            "done": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "counselor_request",
            "message": "Of course — you can connect with a human counselor anytime using the button in the top right corner 👤.",
            "button_icon_url": f"{_base_url}/static/images/connect_counselor_btn.png",
            "is_crisis": is_message_crisis,
        }

        async def _counselor_request_stream():
            yield f"data: {json.dumps(counselor_info_payload)}\n\n"

        return StreamingResponse(
            _counselor_request_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # ── Fix 1: Crisis fork — EARLY RETURN, AI generator never runs ───────────
    if consensus.get("is_crisis") is True:
        logger.warning(f"[ESCALATION] Crisis detected for user {user_id} in session {actual_session_id}.")

        escalated_ok = await escalate_session(actual_session_id)
        if not escalated_ok:
            logger.error(
                f"[ESCALATION] escalate_session() failed for session {actual_session_id} — "
                "routing aborted; falling back to normal AI response."
            )
            # Clear the crisis flag so execution continues to the normal AI stream below
            consensus["is_crisis"] = False

    if consensus.get("is_crisis") is True:
        # Before routing, check if anyone is available to avoid sending false hope
        from app.services.routing_service import get_available_counselor_count
        count = await get_available_counselor_count()
        if count == 0:
            hotline_text = "No counselor is available at the moment. Please call the helpline at 911."
            await save_message({
                "session_id": actual_session_id,
                "user_id": user_id,
                "turn_number": turn_count + 2,
                "role": "system",
                "content": hotline_text,
            })
            hotline_payload = {
                "done": True,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "text": hotline_text,
                "is_crisis": True,
            }
            async def _hotline_stream():
                yield f"data: {json.dumps(hotline_payload)}\n\n"
            return StreamingResponse(
                _hotline_stream(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        # Routing service exclusively owns all dashboard notifications — do NOT
        from app.services.routing_service import route_crisis_session
        asyncio.create_task(
            route_crisis_session(
                user_id=user_id,
                session_id=actual_session_id,
                consensus=consensus,
            )
        )

        # Return a single done-event — no AI chunks, no dual response
        _settings = get_settings()
        _ws_url = f"ws://{_settings.SERVER_PUBLIC_HOST}:{_settings.SERVER_PORT}/api/human/chat/{actual_session_id}"
        crisis_payload = {
            "done": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "crisis_escalation",
            "handoff_message": "A counselor is joining shortly... you're not alone.",
            "websocket_url": _ws_url,
            "emotion": {
                "dominant_emotion": emotion_result.dominant if emotion_result else "neutral",
                "response_mode": consensus.get("category", "general"),
                "intensity": consensus.get("intensity", "high"),
                "is_crisis_signal": True,
            },
            "is_crisis": True,
        }

        async def _crisis_stream():
            yield f"data: {json.dumps(crisis_payload)}\n\n"

        return StreamingResponse(
            _crisis_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # ── Normal AI stream ───────────────────────────────────────────────────────
    async def generate():
        full_reply = []
        try:
            async for chunk in llm_svc.chat_stream(
                user_message=req.message,
                profile=profile,
                history=history,
                consensus=consensus,
                long_term_memory=long_term_memory,
            ):
                full_reply.append(chunk)
                yield f"data: {json.dumps({'chunk': chunk, 'is_crisis': is_message_crisis})}\n\n"

            emotion_dict = {
                "dominant_emotion":  emotion_result.dominant if emotion_result else "neutral",
                "response_mode":     consensus.get("category", "general"),
                "intensity":         consensus.get("intensity", "moderate"),
                "is_crisis_signal":  False,
            }
            done_payload = {
                "done": True,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "emotion": emotion_dict,
                "is_crisis": is_message_crisis,
            }
            yield f"data: {json.dumps(done_payload)}\n\n"

            final = "".join(full_reply)
            logger.info(f"[STEP 3 OK] {len(final)} chars streamed")

            roberta_doc = None
            if emotion_result:
                roberta_doc = {
                    "dominant_emotion": emotion_result.dominant,
                    "scores": emotion_result.scores,
                }

            await save_message({
                "session_id": actual_session_id,
                "user_id": user_id,
                "turn_number": turn_count + 1,
                "role": "assistant",
                "content": final,
                "roberta_analysis": roberta_doc,
                "llm_consensus": consensus,
            })

            logger.info(f"[AI RESPONSE]:\n{final}\n" + "═" * 70)

        except Exception as e:
            logger.error(f"[STREAM ERROR] {e}")
            yield f"data: {json.dumps({'error': 'Stream interrupted'})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Chat History API ──────────────────────────────────────────────────────────

@router.get("/history", response_model=ChatHistoryResponse)
async def get_chat_history(current_user = Depends(get_current_user)):
    """Returns ALL messages for the authenticated user, sorted chronologically."""
    user_id = str(current_user.get("user_id") or current_user.get("_id"))
    docs = await get_user_messages(user_id)

    formatted_messages = [
        ChatMessageResponse(
            user_id=doc.get("user_id", user_id),
            role=doc.get("role", "unknown"),
            content=doc.get("content", ""),
            timestamp=doc.get("timestamp"),
        )
        for doc in docs
        if doc.get("content")
    ]

    return ChatHistoryResponse(
        status="success",
        user_id=user_id,
        total_messages=len(formatted_messages),
        messages=formatted_messages,
    )


# ── Sessions List API ─────────────────────────────────────────────────────────

@router.get("/sessions", response_model=SessionListResponse)
async def get_user_sessions(current_user = Depends(get_current_user)):
    """Returns ALL sessions for the authenticated user, newest first."""
    user_id = str(current_user.get("user_id") or current_user.get("_id"))
    sessions = await get_all_sessions(user_id)

    formatted_sessions = [
        SessionResponse(**{**s, "user_id": s.get("user_id") or user_id})
        for s in sessions
    ]

    return SessionListResponse(
        status="success",
        user_id=user_id,
        total_sessions=len(formatted_sessions),
        sessions=formatted_sessions,
    )
