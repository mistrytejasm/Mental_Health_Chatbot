"""
Audio Routes
─────────────
POST /api/audio/transcribe — accepts audio file, returns transcript via Groq Whisper
"""

import io
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from groq import AsyncGroq

from app.core.auth.oauth2 import get_current_user
from app.core.config import get_settings
from app.api.schemas.response import TranscriptionResponse
from app.core.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()
router = APIRouter(prefix="/api/audio", tags=["audio"])

ALLOWED_MIME = {
    "audio/webm", "audio/ogg", "audio/mp4", "audio/mpeg",
    "audio/wav", "audio/x-wav", "audio/flac",
}


@router.post("/transcribe", response_model=TranscriptionResponse)
async def transcribe_audio(file: UploadFile = File(...), user = Depends(get_current_user)):
    """
    Accepts an audio blob from the browser (MediaRecorder output, typically webm/ogg).
    Sends it to Groq's Whisper large-v3 model and returns the transcript.
    """
    logger.info(f"Receiving audio file for transcription: {file.filename}")
    if file.content_type and file.content_type not in ALLOWED_MIME:
        logger.warning(f"Unsupported audio type: {file.content_type}")

    audio_bytes = await file.read()
    if len(audio_bytes) < 100:
        raise HTTPException(status_code=400, detail="Audio file is too small or empty.")

    try:
        client = AsyncGroq(api_key=settings.GROQ_API_KEY)

        # Groq Whisper expects a (filename, bytes, content_type) tuple
        filename = file.filename or "recording.webm"
        transcription = await client.audio.transcriptions.create(
            file=(filename, audio_bytes, file.content_type or "audio/webm"),
            model=settings.GROQ_WHISPER_MODEL,
            response_format="verbose_json",
        )

        text = transcription.text.strip()
        language = getattr(transcription, "language", None)
        duration = getattr(transcription, "duration", None)

        # Only allow English input. If another language is detected, wipe the text!
        if language and language.lower() not in ["english", "en"]:
            logger.warning(f"Rejected non-English audio. Detected: {language}")
            text = ""

        logger.debug(f"Whisper transcript: '{text[:60]}...' lang={language}")
        logger.info("Transcription successful")

        return TranscriptionResponse(text=text, language=language, duration=duration)

    except Exception as e:
        logger.error(f"Whisper transcription error: {e}")
        raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)}")