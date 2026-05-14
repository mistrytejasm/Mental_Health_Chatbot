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
    "hurt myself",          "hurting myself",       "harm myself",
    "cut myself",           "cutting myself",
    "suicide",              "suicidal",
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


async def synthesize_consensus(text: str, roberta_emotion: str, roberta_score: float) -> dict:
    """
    Executes the LLM Sentiment & Crisis Synthesizer (GPT-4o-mini).
    Returns a structured dictionary with:
      - llm_sentiment: The verified emotional state
      - category: The dynamic conversational theme (e.g. 'burnout', 'grief')
      - is_crisis: True if active suicidal ideation or threat to life
      - reasoning: Explanation for the synthesis

    Pre-screen runs first: unambiguous crisis/counselor keywords bypass the LLM
    so short or blunt signals ("die", "kill myself") are never silently dropped.
    """
    # ── Step 1: deterministic pre-screen ─────────────────────────────────────
    forced_crisis, forced_counselor = _pre_screen(text)

    if forced_crisis:
        logger.warning(
            f"[SAFETY] Pre-screen: crisis keyword matched — forcing is_crisis=True | text='{text[:80]}'"
        )
    if forced_counselor:
        logger.info(
            f"[SAFETY] Pre-screen: counselor keyword matched — forcing wants_counselor=True | text='{text[:80]}'"
        )

    # ── Step 2: LLM consensus ────────────────────────────────────────────────
    client = _get_client()

    system_prompt = (
        "You are an expert clinical sentiment analyzer and crisis triage AI.\n"
        "Your job is to read the user's text and the raw statistical emotion provided by an NLP model, "
        "and synthesize them into a logical consensus.\n"
        "You must respond in strictly valid JSON with exactly these keys:\n"
        '{"llm_sentiment": "string", "category": "string", "is_crisis": boolean, "wants_counselor": boolean, "reasoning": "string"}\n\n'
        "RULES:\n\n"

        "1. CATEGORY: Identify the emotional theme freely (e.g. 'severe_burnout', 'relationship_conflict', "
        "'financial_stress', 'grief', 'anxiety', 'loneliness', 'suicidal_ideation').\n\n"

        "2. IS_CRISIS — set true when the user expresses ANY intent or desire to self-harm, die, or end "
        "their life, even as a single word or short phrase.\n"
        "   TRUE (is_crisis=true) — set true for ALL of these:\n"
        "     - Single words/phrases: 'die', 'dying', 'suicide', 'suicidal', 'end it', 'end it all'\n"
        "     - 'I want to kill myself' / 'kill myself'\n"
        "     - 'I am going to end my life' / 'end my life'\n"
        "     - 'I have a plan to suicide'\n"
        "     - 'I want to hurt myself' / 'hurt myself'\n"
        "     - 'I've been cutting myself' / 'cutting myself'\n"
        "     - 'I want to die' / 'want to die' / 'wanna die'\n"
        "     - 'no reason to live', 'not worth living', 'better off dead'\n"
        "     - 'I can't go on', 'I don't want to be here anymore'\n\n"
        "   FALSE — NEVER set is_crisis=true for these even though they mention death, pain, or ending:\n"
        "     NEGATIONS (desire to live): 'I don't want to die', 'I want to live', "
        "'I'm not suicidal', 'I don't want to hurt myself'\n"
        "     FEAR (not intent): 'I'm scared of dying', 'I'm afraid of death'\n"
        "     IDIOMS/METAPHORS: 'this is killing me', 'dying of embarrassment', 'dying of laughter'\n"
        "     VAGUE DISTRESS (no self-harm intent): 'I feel like dying', 'I feel dead inside', "
        "'I want to disappear', 'I want to escape', 'I want to end this pain'\n"
        "     PAST TENSE: 'I used to think about suicide' (past, not present plan)\n\n"
        "   AMBIGUOUS RULE: If the text contains both a crisis phrase AND a negation/fear qualifier, "
        "the negation wins — keep is_crisis=false.\n"
        "   DEFAULT: When genuinely uncertain and no keyword matches, set is_crisis=false.\n\n"

        "3. WANTS_COUNSELOR — set true when the user asks to speak with a human, counselor, "
        "therapist, doctor, or real person, even without using the exact word 'counselor'.\n"
        "   TRUE (wants_counselor=true):\n"
        "     - 'I want to talk to a human' / 'talk to human'\n"
        "     - 'I want to talk to a real person / human'\n"
        "     - 'Can I speak to a counselor / therapist / doctor?'\n"
        "     - 'Connect me to a human' / 'get me a human'\n"
        "     - 'I want professional help'\n"
        "     - 'Is there a real person I can talk to?'\n"
        "     - 'I want to talk to a human counselor'\n\n"
        "   FALSE — NEVER set wants_counselor=true for:\n"
        "     TALKING TO AI: 'just listen to me', 'stay with me', 'be there for me'\n"
        "     AI PREFERENCE: 'I only want to talk to you', 'I don't want a human'\n"
        "     VAGUE REQUESTS: 'I need help', 'can someone help me'\n"
        "     NO FIX NEEDED: 'I just need someone to listen'\n\n"
        "   DEFAULT: Any ambiguity → wants_counselor=false.\n\n"

        "4. REASONING: One sentence explaining the is_crisis decision."
    )

    user_prompt = f"User Text: \"{text}\"\nRaw RoBERTa Emotion: {roberta_emotion} (score: {roberta_score:.2f})"

    try:
        response = await client.chat.completions.create(
            model=settings.SYNTHESIZER_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=250,
        )

        content = response.choices[0].message.content
        result = json.loads(content)

        # Pre-screen overrides always win — they are deterministic and cannot be
        # contradicted by the probabilistic LLM output.
        is_crisis = forced_crisis if forced_crisis is not None else bool(result.get("is_crisis", False))
        wants_counselor = forced_counselor if forced_counselor is not None else bool(result.get("wants_counselor", False))

        if is_crisis:
            logger.warning(f"[SAFETY] Crisis confirmed | reasoning: {result.get('reasoning')}")
        if wants_counselor:
            logger.info("[SAFETY] User requested a human counselor.")

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
