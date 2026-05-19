"""
Safety & Consensus Service
──────────────────────────
This acts as the Hybrid Consensus Synthesizer (GPT-4o-mini).
It reads the raw user text AND the RoBERTa statistical emotion,
cross-validates them, and generates a structured clinical JSON
containing the logical category, true sentiment, and an active crisis flag.

Pre-screen layer (added):
  Before calling the LLM, a deterministic keyword check runs against the
  lowercased message.  Any unambiguous crisis phrase (e.g. "kill myself",
  "want to die", "end my life") immediately forces is_crisis=True without
  waiting for the LLM.  This prevents a probabilistic model from silently
  swallowing short or blunt crisis signals like "die" or "end it all".
  Similarly, plain "talk to human / speak to a person" phrases force
  wants_counselor=True before the LLM prompt is evaluated.
"""
from __future__ import annotations
import json
from openai import AsyncOpenAI
from app.core.config import get_settings
from app.core.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

def _get_client() -> AsyncOpenAI:
    return AsyncOpenAI(api_key=settings.OPENAI_API_KEY)


# ── Deterministic pre-screens ─────────────────────────────────────────────────
# These run BEFORE the LLM.  They are intentionally conservative: only include
# phrases that are unambiguously crisis signals in isolation.  Ambiguous phrases
# that need context (e.g. "I want to disappear") stay LLM-only.

_CRISIS_KEYWORDS: tuple[str, ...] = (
    # Direct self-harm / suicidal intent — specific enough to avoid false positives.
    # Phrases that start with "i " or contain self-referential structure cannot
    # appear inside a negation like "I don't want to die" without the negation
    # itself also being present, which the anti-crisis guard in chat.py strips.
    "kill myself",          "killing myself",
    "i want to die",        "i wanna die",          "i'm gonna die",
    "i am going to die",    "i'm going to die",
    "end my life",          "end it all",
    "take my life",         "taking my life",
    "cutting myself",
    "want to kill myself",  "going to kill myself",  "going to end my life",
    "plan to end my life",  "thinking of ending my life",
    "no reason to live",    "not worth living",
    "can't go on",          "cant go on",
    "don't want to be here","dont want to be here",
    "wish i was dead",      "wish i were dead",
    "better off dead",      "better off without me",
    # NOTE: bare "want to die" / "wanna die" are omitted — "I dont want to die"
    # contains "want to die" as a substring.  The "i want to die" form above
    # only matches when the subject is "I", which cannot follow a negation.
    # The improved LLM prompt handles the remaining ambiguous forms in context.
)

_WANTS_COUNSELOR_KEYWORDS: tuple[str, ...] = (
    # Explicit human / professional requests
    "talk to a human",      "talk to human",
    "speak to a human",     "speak to human",
    "talk to a person",     "speak to a person",
    "talk to a real person","speak to a real person",
    "talk to a counselor",  "speak to a counselor",
    "talk to a therapist",  "speak to a therapist",
    "talk to a doctor",     "speak to a doctor",
    "want a counselor",     "need a counselor",
    "want a therapist",     "need a therapist",
    "connect me to a human","connect me to a counselor",
    "get me a human",       "get me a counselor",
    "i want to talk to human",
    "i want to speak to human",
    "i want human",
    "i need a human",
    "real person",          "real human",
    "actual person",        "human agent",
    "professional help",    "i want professional",
)


def _pre_screen(text: str) -> tuple[bool | None, bool | None]:
    """
    Returns (is_crisis, wants_counselor) overrides.
    None means "let the LLM decide".
    True means "force this value regardless of LLM output".
    """
    lower = text.lower()
    crisis = any(kw in lower for kw in _CRISIS_KEYWORDS)
    counselor = any(kw in lower for kw in _WANTS_COUNSELOR_KEYWORDS)
    return (True if crisis else None, True if counselor else None)


async def synthesize_consensus(text: str, roberta_emotion: str, roberta_score: float, history: str | None = None) -> dict:
    """
    Executes the LLM Sentiment & Crisis Synthesizer (GPT-4o-mini).
    Returns a structured dictionary with:
      - llm_sentiment: The verified emotional state
      - category: The dynamic conversational theme (e.g. 'burnout', 'grief')
      - is_crisis: True if active suicidal ideation or threat to life
      - wants_counselor: True if the user wants to speak with a human
      - reasoning: Explanation for the synthesis
    """
    # ── Step 1: deterministic pre-screen ─────────────────────────────────────
    # We still use a small pre-screen for BLUNT, UNAMBIGUOUS signals to ensure
    # speed and prevent probabilistic "swallowing" of direct crisis words.
    forced_crisis, forced_counselor = _pre_screen(text)

    # ── Step 2: LLM consensus ────────────────────────────────────────────────
    client = _get_client()

    system_prompt = (
        "You are a Clinical Crisis Triage AI. Your goal is to analyze user text and conversation context "
        "to determine if there is an ACTIVE crisis (suicidal intent, self-harm plan) or a request for human help.\n\n"
        
        "You must respond in strictly valid JSON:\n"
        '{"llm_sentiment": "string", "category": "string", "is_crisis": boolean, "wants_counselor": boolean, "reasoning": "string"}\n\n'

        "CRITICAL RULES FOR IS_CRISIS:\n"
        "1. CONTEXT IS KING: Use the provided 'Conversation History' to understand ambiguous answers (e.g., if the user says 'yes' to 'Do you have a plan?').\n"
        "2. NEGATION DETECTION: Set is_crisis=false if the user is expressing a desire to live or denying intent (e.g., 'I don't want to die', 'I'm not suicidal', 'I want to stay alive').\n"
        "3. IDIOM DETECTION: Distinguish between metaphors ('this is killing me', 'I feel dead inside') and literal intent.\n"
        "4. FEAR vs INTENT: 'I'm scared of dying' is NOT a crisis. 'I want to die' IS a crisis.\n"
        "5. INTENT/PLAN: Any expression of current intent, plan, or desire to end one's life or self-harm must be is_crisis=true.\n"
        "6. GREETINGS: Do NOT set is_crisis=true for isolated greetings (e.g., 'hi', 'hello', 'hey', 'good morning') even if the history contains a past crisis. Only escalate if the current message continues the crisis or expresses new distress.\n"
        "7. INFORMATIONAL/ADVICE/COPING REQUESTS: Do NOT set is_crisis=true if the user is asking for general information, definitions, advice, coping strategies, or guidance regarding thoughts of self-harm or suicide (e.g., 'how can I manage suicidal thoughts', 'is it possible to get advice on self-harm thoughts'). Only escalate if they express a personal, active plan, intent, or imminent danger to harm themselves right now.\n\n"

        "CRITICAL RULES FOR WANTS_COUNSELOR:\n"
        "1. CURRENT MESSAGE ONLY: Evaluate ONLY the 'Current User Text' to determine if they want a counselor. Do NOT set wants_counselor=true if they only asked in the Conversation History but not in the current message.\n"
        "2. EXPLICIT REQUESTS: Set wants_counselor=true if the Current User Text explicitly asks for a person, human, counselor, doctor, or therapist.\n"
        "3. AI PREFERENCE: Set wants_counselor=false if the user explicitly states they want to talk ONLY to you/AI (e.g., 'I don't want a human', 'just talk to me', 'I only trust you').\n"
        "4. AMBIGUITY: If the user says 'I need help', check history. If it's help with a problem, keep false. If they want 'someone to talk to', keep false unless they specify a person.\n"
    )

    history_block = f"\nConversation History:\n{history}\n" if history else ""
    user_prompt = f"{history_block}Current User Text: \"{text}\"\nRaw Emotion Model Data: {roberta_emotion} (score: {roberta_score:.2f})"

    try:
        response = await client.chat.completions.create(
            model=settings.SYNTHESIZER_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0, # Zero temperature for max consistency
            max_tokens=300,
        )

        content = response.choices[0].message.content
        result = json.loads(content)

        # Pre-screen overrides still apply for blunt force safety
        is_crisis = forced_crisis if forced_crisis is not None else bool(result.get("is_crisis", False))
        wants_counselor = forced_counselor if forced_counselor is not None else bool(result.get("wants_counselor", False))

        return {
            "llm_sentiment":    result.get("llm_sentiment", "unknown"),
            "category":         result.get("category", "general"),
            "is_crisis":        is_crisis,
            "wants_counselor":  wants_counselor,
            "reasoning":        result.get("reasoning", ""),
            "intensity":        "high" if is_crisis else "moderate",
            "message_class":    "crisis" if is_crisis else "emotional_ongoing",
            "recommended_tone": "validating",
            "token_budget":     200 if is_crisis else 320,
            "crisis_type":      result.get("category") if is_crisis else None,
        }

    except Exception as e:
        logger.error(f"Consensus Synthesizer failed: {e}")
        # Conservative fail-safe: unknown safety state → treat as crisis.
        # A false-positive escalation is recoverable; a false-negative during
        # an API outage is not.
        return {
            "llm_sentiment":   "unknown",
            "category":        "technical_error",
            "is_crisis":       True,
            "wants_counselor": False,
            "intensity":       "high",
            "recommended_tone": "validating",
            "message_class":   "crisis",
            "token_budget":    200,
            "reasoning":       "Safety check unavailable — escalating out of caution.",
        }
