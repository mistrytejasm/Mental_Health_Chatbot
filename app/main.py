"""
MindBuddy — FastAPI Application Entry Point
──────────────────────────────────────────────
Initialises the application, registers middleware, mounts routers,
and manages the lifespan (startup/shutdown) of shared resources.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes import assessment, audio, chat, user
from app.api.routes.human import router as human_router
from app.core.config import get_settings
from app.core.database import close_mongo_connection, connect_to_mongo, get_database
from app.core.redis import redis_manager
from app.core.logger import get_logger
from app.services.emotion import warmup
import os

logger = get_logger("main")
settings = get_settings()
origins = os.getenv("ALLOWED_ORIGINS", "").split(",")



# ── Application Lifespan ──────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages startup and shutdown of shared application resources.

    Startup sequence:
      1. Connect to MongoDB and verify indexes.
      2. Pre-warm the HuggingFace emotion model (CPU — run in thread pool).
      3. Reset all counselor online flags (in-memory registry is lost on restart).
      4. Start the global 35-minute session inactivity watchdog.
    """
    logger.info("MindBuddy starting up...")

    # 1. Database & Cache
    await connect_to_mongo()
    await redis_manager.connect()

    # 2. Emotion model warm-up (CPU-bound — offloaded to thread pool)
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, warmup)
    except Exception as exc:
        logger.warning(f"Emotion model warmup skipped: {exc}")

    # 3. Reset counselor presence flags that were lost when the process restarted
    try:
        db = get_database()
        if db is not None:
            result = await db.admins.update_many(
                {},
                {"$set": {"is_online": False, "current_active_sessions": 0}},
            )
            logger.info(f"Startup: reset {result.modified_count} counselor(s) to offline.")
    except Exception as exc:
        logger.warning(f"Startup: could not reset counselor online flags: {exc}")

    # 4. Global inactivity watchdog
    from app.api.routes.human.background_tasks import inactivity_watchdog
    try:
        loop.create_task(inactivity_watchdog())
    except Exception as exc:
        logger.error(f"Failed to start inactivity watchdog: {exc}")

    logger.info("MindBuddy ready.")
    yield

    logger.info("MindBuddy shutting down.")
    await redis_manager.disconnect()
    await close_mongo_connection()


# ── Application Factory ───────────────────────────────────────────────────────

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# app.add_middleware(
#     CORSMiddleware,
#     allow_origin_regex=r"https?://.*",  # Permissive for local network development
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# ── Exception Handlers ────────────────────────────────────────────────────────

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    """
    Converts Pydantic's 422 Unprocessable Entity into a mobile-friendly 400 Bad Request.

    Field path cleaning:
      - Strips the leading 'body' segment Pydantic injects for request bodies.
      - Returns only the terminal (most specific) field name so mobile clients
        receive 'email' rather than 'body -> common_fields -> email'.
      - Returns only the first error to guide the user one field at a time.
    """
    all_errors = exc.errors()
    first_error = all_errors[0] if all_errors else {}
    location_parts = [str(p) for p in first_error.get("loc", []) if str(p) != "body"]
    field_name = location_parts[-1] if location_parts else "unknown"
    error_message = first_error.get("msg", "Invalid value")

    return JSONResponse(
        status_code=400,
        content={
            "error_code": "VALIDATION_ERROR",
            "message": error_message,
            "details": [
                {
                    "field": field_name,
                    "message": error_message,
                    "type": first_error.get("type", "value_error"),
                }
            ],
        },
    )


# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(chat.router)
app.include_router(audio.router)
app.include_router(assessment.router)
app.include_router(human_router)
app.include_router(user.router)


# ── Static Files & Root ───────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", include_in_schema=False)
async def serve_ui():
    """Serves the counselor dashboard single-page application."""
    return FileResponse("app/static/index.html")


@app.get("/health", tags=["health"])
async def health_check():
    """Returns application health status. Used by load balancers and uptime monitors."""
    return {"status": "ok", "app": settings.APP_NAME, "version": settings.APP_VERSION}