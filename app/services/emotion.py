"""
Emotion Analysis Service
─────────────────────────
Uses SamLowe/roberta-base-go_emotions (28 labels) loaded locally via
HuggingFace Transformers. The pipeline is lazy-loaded once on first use
and cached — no cold-start penalty after the first call.

Why this model over j-hartmann (7 labels)?
  • Includes: hopelessness, grief, remorse, nervousness — critical for mental health
  • 28 fine-grained labels allow much richer response-mode selection
  • Still fast: ~80–100 ms inference on CPU after warmup
"""

from __future__ import annotations
import asyncio
from functools import lru_cache
from typing import Optional

from transformers import pipeline as hf_pipeline
from app.core.config import get_settings
from app.core.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

# ── Lazy pipeline holder ──────────────────────────────────────────────────────
_pipeline = None


def _load_pipeline():
    """Load the HF pipeline once and cache it."""
    global _pipeline
    if _pipeline is not None:
        return _pipeline
    try:
        logger.info(f"Loading emotion model: {settings.HF_EMOTION_MODEL} (local inference — HF_API_TOKEN not used)")
        _pipeline = hf_pipeline(
            task="text-classification",
            model=settings.HF_EMOTION_MODEL,
            top_k=5,               # return top-5 labels with scores
            truncation=True,
            max_length=512,
        )
        logger.info("Emotion model loaded successfully.")
    except Exception as e:
        logger.error(f"Failed to load emotion model: {e}")
        _pipeline = None
    return _pipeline


# ── Public API ────────────────────────────────────────────────────────────────

class EmotionResult:
    def __init__(
        self,
        dominant: str,
        scores: dict[str, float],
    ):
        self.dominant = dominant
        self.scores = scores

    def to_dict(self) -> dict:
        return {
            "dominant_emotion": self.dominant,
            "top_scores": self.scores,
        }


async def analyse(text: str, context_window: Optional[str] = None) -> EmotionResult:
    """
    Analyse `text` and return an EmotionResult.

    `context_window` — optional: pass the last 1–2 turns of conversation
    concatenated as a string. This prevents misclassification of short
    ambiguous fragments like "just useless" (which RoBERTa alone reads as
    'annoyance' rather than 'hopelessness' without context).

    Falls back to neutral if the model is unavailable.
    """
    logger.info(f"Starting emotion analysis for text: '{text[:50]}...'")
    # For very short messages, prepend context so the model has enough signal
    inference_text = text
    if context_window and len(text.split()) <= 6:
        # Trim context to avoid exceeding 512 tokens
        ctx = context_window[-300:]
        inference_text = f"{ctx} {text}".strip()

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _run_inference, inference_text, text)
    return result


def _run_inference(text: str, original_text: Optional[str] = None) -> EmotionResult:
    # ── Model inference ───────────────────────────────────────────────────────
    pipe = _load_pipeline()
    if pipe is None:
        return _fallback()

    try:
        raw = pipe(text)
        # raw is a list of lists when top_k > 1
        items = raw[0] if isinstance(raw[0], list) else raw
        scores = {item["label"].lower(): round(item["score"], 4) for item in items}
        dominant = max(scores, key=scores.get)
    except Exception as e:
        logger.warning(f"Emotion inference error: {e}")
        return _fallback()

    logger.info(f"Emotion Analysis — Dominant: {dominant} ({scores.get(dominant):.2f})")

    return EmotionResult(
        dominant=dominant,
        scores=scores,
    )


def _fallback() -> EmotionResult:
    logger.info("Emotion analysis fallback triggered")
    return EmotionResult(
        dominant="neutral",
        scores={"neutral": 1.0},
    )


# Pre-warm the model at import time (runs in background on startup)
def warmup():
    try:
        _load_pipeline()
        if _pipeline:
            _pipeline("I feel okay today")
            logger.info("Emotion model warm-up complete.")
    except Exception as e:
        logger.warning(f"Model warm-up skipped: {e}")