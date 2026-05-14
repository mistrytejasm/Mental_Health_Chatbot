import asyncio
import bcrypt

# Fix passlib incompatibility with bcrypt 4.0+
if not hasattr(bcrypt, "__about__"):
    class BcryptAbout:
        __version__ = getattr(bcrypt, "__version__", "4.0.0")
    bcrypt.__about__ = BcryptAbout()
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError

from app.services.emotion import warmup
from app.core.database import connect_to_mongo, close_mongo_connection, get_database
from app.core.config import get_settings
from app.core.logger import get_logger
from app.api.routes import chat, audio, assessment, human, user

logger = get_logger("main")
settings = get_settings()


# ── Lifespan: pre-warm the emotion model on startup ──────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("MindBridge starting up...")
    
    # 1. Connect to MongoDB
    await connect_to_mongo()
    
    # 2. Warm up the HuggingFace emotion model in a background thread
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, warmup)
    except Exception as e:
        logger.warning(f"Model warmup skipped: {e}")
        
    # 3. Reset all counselor online flags — in-memory registry is gone after restart
    try:
        db = get_database()
        if db is not None:
            result = await db.admins.update_many(
                {},
                {"$set": {"is_online": False, "current_active_sessions": 0}},
            )
            logger.info(f"Startup: reset {result.modified_count} counselor(s) to offline.")
    except Exception as e:
        logger.warning(f"Startup: could not reset counselor online flags: {e}")

    # 4. Start Global 35-minute Inactivity Watchdog
    try:
        loop.create_task(human.inactivity_watchdog())
    except Exception as e:
        logger.error(f"Failed to start watchdog: {e}")

    logger.info("MindBridge ready.")
    yield
    logger.info("MindBridge shutting down.")
    await close_mongo_connection()


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

_allowed_origins = (
    [o.strip() for o in settings.ALLOWED_ORIGINS.split(",") if o.strip()]
    if settings.ALLOWED_ORIGINS
    else ["*"]
)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https?://.*",  # Permissive for development/local WiFi
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(RequestValidationError)
async def custom_validation_exception_handler(request, exc):
    """
    Intercepts Pydantic's default 422 Unprocessable Entity and returns
    a mobile-friendly 400 Bad Request with structured error details.

    Field path cleaning:
    - Strips the leading 'body' segment Pydantic injects for request bodies.
    - Uses only the terminal (most specific) field name so mobile clients
      receive plain names like 'email' instead of 'body -> common_fields -> email'.
    """
    all_errors = exc.errors()
    # Return only the first error so the mobile app can guide the user
    # one field at a time (email → phone → gender → …) rather than
    # overwhelming them with every problem at once.
    first = all_errors[0] if all_errors else {}
    loc = first.get("loc", [])
    loc_parts = [str(p) for p in loc if str(p) != "body"]
    field = loc_parts[-1] if loc_parts else "unknown"
    error_message = first.get("msg", "Invalid value")

    return JSONResponse(
        status_code=400,
        content={
            "error_code": "VALIDATION_ERROR",
            "message": error_message,
            "details": [
                {
                    "field": field,
                    "message": error_message,
                    "type": first.get("type", "value_error"),
                }
            ],
        },
    )

# ── API Routers ───────────────────────────────────────────────────────────────
app.include_router(chat.router)
app.include_router(audio.router)
app.include_router(assessment.router)
app.include_router(human.router)
app.include_router(user.router)

# ── Static files (UI) ─────────────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/", include_in_schema=False)
async def serve_ui():
    logger.info("Serving UI index.html")
    return FileResponse("app/static/index.html")

@app.get("/health")
async def health():
    logger.info("Health check endpoint hit")
    return {"status": "ok", "app": settings.APP_NAME}