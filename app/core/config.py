"""
Application Configuration
──────────────────────────
Loaded once via lru_cache. All settings pulled from .env file.

v2 changes:
  • MAX_HISTORY_TURNS reduced from 20 → 15
    (20 turns = ~6000 tokens of history alone, pushing context near limit)
  • MAX_TOKENS changed from 2000 → 300 (soft default only)
    NOTE: llm.py now overrides max_tokens dynamically per message class.
    This value is only used as a safety ceiling for any call that bypasses
    the message classifier (e.g. direct calls in tests).
  • Added SYNTHESIZER_MODEL as a separate config key so it can be
    swapped independently from the main generator model.
"""

from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic import Field, AliasChoices
from functools import lru_cache
from app.core.logger import get_logger

from dotenv import load_dotenv
# override=True ensures .env values always win over stale OS/shell environment
# variables, preventing short expiry values set elsewhere from shadowing .env.
load_dotenv(override=True)

logger = get_logger(__name__)


class Settings(BaseSettings):
    APP_NAME:    str = "MindBuddy"
    APP_VERSION: str = "1.0.0"
    DEBUG:       bool = False
    
    # ── Database & Cache ──────────────────────────────────────────────────────
    MONGODB_URL: str = "mongodb://localhost:27017"
    DATABASE_NAME: str = "mindbuddy_db"
    REDIS_URL: str = "redis://localhost:6379/0"

    # ── OpenAI — Main Generator ───────────────────────────────────────────────
    OPENAI_API_KEY: str  = ""
    MAIN_MODEL: str = "gpt-4o"

    # ── OpenAI — Synthesizer (Fast JSON metadata) ────────────────────────────
    SYNTHESIZER_MODEL: str = "gpt-4o-mini"

    # ── Groq Whisper (STT) ───────────────────────────────────────────────────
    GROQ_API_KEY: str = ""
    GROQ_WHISPER_MODEL: str = "whisper-large-v3"

    # ── HuggingFace Emotion Model ─────────────────────────────────────────────
    # 28-label GoEmotions — runs locally via transformers pipeline
    HF_EMOTION_MODEL: str = "SamLowe/roberta-base-go_emotions"
    HF_API_TOKEN:     str = ""   # only needed for HF Inference API fallback

    # ── Session / History ─────────────────────────────────────────────────────
    # 50 turns = ~100 messages. GPT-4o has 128K context — use it.
    # This gives the AI full conversation memory, like ChatGPT.
    MAX_HISTORY_TURNS: int = 50

    # ── Token ceiling ─────────────────────────────────────────────────────────
    # llm.py overrides this dynamically per message class.
    # This value is a safety fallback ONLY — never used for normal chat flow.
    MAX_TOKENS: int = 300

    # ── Server address ────────────────────────────────────────────────────────
    # SERVER_HOST is the binding address for uvicorn (0.0.0.0 = all interfaces).
    # SERVER_PUBLIC_HOST is the address clients use to connect (real IP or domain).
    # These MUST be different when running on a remote server.
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8000
    SERVER_PUBLIC_HOST: str = "localhost"  # override in .env with actual IP or domain

    # ── CORS ─────────────────────────────────────────────────────────────────
    # Comma-separated list of allowed origins. Leave empty to allow all (dev only).
    ALLOWED_ORIGINS: str = ""
    # ALLOWED_ORIGINS: str = "http://192.168.29.22:5173"
    ALLOWED_ORIGINS: str = "http://192.168.3.117:5173"

    # ── JWT Authentication ────────────────────────────────────────────────────
    SECRET_KEY: str = Field(
        ...,
        validation_alias=AliasChoices("JWT_SECRET_KEY", "SECRET_KEY"),
        description="Set via JWT_SECRET_KEY or SECRET_KEY environment variable.",
    )
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 10080  # 7 days
    REFRESH_TOKEN_EXPIRE_DAYS: int = 15
    OTP_EXPIRY_MINUTES: int = 10

    class Config:
        env_file          = ".env"
        env_file_encoding = "utf-8"
        extra             = "ignore"

@lru_cache()
def get_settings() -> Settings:
    settings = Settings()
    if not settings.SECRET_KEY:
        raise RuntimeError("SECRET_KEY must be set")
    logger.info(
        f"Settings loaded — "
        f"ACCESS_TOKEN_EXPIRE_MINUTES={settings.ACCESS_TOKEN_EXPIRE_MINUTES} "
        f"({settings.ACCESS_TOKEN_EXPIRE_MINUTES / 1440:.1f} days), "
        f"REFRESH_TOKEN_EXPIRE_DAYS={settings.REFRESH_TOKEN_EXPIRE_DAYS}"
    )
    return settings