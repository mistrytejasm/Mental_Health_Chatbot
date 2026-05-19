"""
LLM Service — OpenAI Generator v5
─────────────────────────────────────────
v5: Complete rewrite for GPT-4o.
  • Lean identity-first prompt (~800 tokens vs v4's ~3,500)
  • Removed rigid sentence counts, 4-phase system, 40+ banned phrases
  • 6 natural principles replace 9 hard rules + 5 principles
  • Token budgets increased to let GPT-4o write full, human responses
  • Crisis protocol remains strict and non-negotiable
  • Anti-repetition reduced from 3 banned openings to 1
"""

from __future__ import annotations
from typing import AsyncIterator, Optional

from openai import AsyncOpenAI

from app.core.config import get_settings
from app.core.constants import CRISIS_LINES
from app.core.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

CRISIS_TOKEN_FLOOR = 800


def _get_client() -> AsyncOpenAI:
    return AsyncOpenAI(api_key=settings.OPENAI_API_KEY)


# ── Safe age parser ───────────────────────────────────────────────────────────

def _safe_int(value, default: int = 0) -> int:
    """Safely converts any age input to int. Never raises ValueError."""
    if value is None:
        return default
    try:
        return int(str(value).strip().split()[0])
    except (ValueError, IndexError):
        return default




# ── Conversation memory helpers ───────────────────────────────────────────────

def _extract_bot_last_opening(history: list[dict]) -> str:
    """
    Extracts the first sentence of the bot's last response.
    Used to prevent the bot from starting the same way twice in a row.
    """
    assistant_msgs = [
        m["content"] for m in history
        if m.get("role") == "assistant" and m.get("content", "").strip()
    ]
    if assistant_msgs:
        last = assistant_msgs[-1].strip().split(".")[0].strip()
        if last and len(last) > 10:
            return f'Do not start your response the same way as your last one: "{last}..."'
    return ""


def _build_emotion_arc(history: list[dict]) -> str:
    """
    Builds a plain-English summary of the emotional arc from the user's messages.
    Gives the model a picture of the journey, not just the current message.
    """
    user_msgs = [
        m["content"].strip() for m in history
        if m.get("role") == "user" and m.get("content", "").strip()
    ]
    if not user_msgs:
        return ""
    if len(user_msgs) == 1:
        return f"Only 1 turn so far: {user_msgs[0][:150]}"

    arc_lines = []
    for i, msg in enumerate(user_msgs[-6:], 1):
        arc_lines.append(f"  Turn {i}: {msg[:120]}")
    return "\n".join(arc_lines)


def _build_personalization_note(
    name: str, age: int, turn_count: int, history: list[dict]
) -> str:
    """ 
    Builds personalization context that deepens with turns.
    """
    if turn_count == 0:
        parts = []
        if age > 0:
            parts.append(f"age {age}")
        base = (f"{name}, " + ", ".join(parts) + ".") if parts else ""
        # Always ask the LLM to greet by name on the very first turn,
        # regardless of whether age is known.
        greeting_note = (
            f"This is your very first reply to {name}. "
            f"Start your response by greeting them warmly using their name."
        )
        return f"{base} {greeting_note}".strip()

    if turn_count <= 2:
        return (
            f"You've just started talking with {name}. "
            f"Please address them by their name naturally in your response to make them feel welcome. "
            "You don't know much yet — be curious, not assumptive."
        )

    user_msgs = [
        m["content"].strip() for m in history
        if m.get("role") == "user" and m.get("content", "").strip()
    ]

    # Every 3 turns, remind the LLM to use the name naturally once.
    name_reminder = (
        f" Use {name}'s name naturally once in this response."
        if turn_count % 3 == 0 else ""
    )

    if turn_count <= 5:
        shared = (" | ".join(user_msgs[-3:]))[:250] if user_msgs else ""
        return (
            f"You know {name} somewhat now. "
            f"They've shared: {shared}. "
            f"Reference what they told you. Be specific, not generic.{name_reminder}"
        )

    # Turn 6+: deep personalization
    all_shared = (" | ".join(user_msgs[-6:]))[:400] if user_msgs else ""
    return (
        f"You know {name} well by now. "
        f"Here's what they've shared over the conversation: {all_shared}. "
        f"Use this to be specific and personal. Reference things they told you earlier.{name_reminder}"
    )

# ── System prompt builder ─────────────────────────────────────────────────────

def build_system_prompt(
    profile: dict,
    conversation_so_far: Optional[list] = None,
    user_message: str = "",
    consensus: Optional[dict] = None,
    long_term_memory: Optional[list] = None,
) -> tuple[str, int]:
    """
    Returns (system_prompt: str, token_budget: int).

    v5: Lean identity-first prompt designed for GPT-4o.
    Trusts the model's native empathy instead of micromanaging with 40+ rules.
    Crisis protocol remains strict and non-negotiable.
    """
    conversation_so_far = conversation_so_far or []

    # ── Profile ───────────────────────────────────────────────────────────────
    name          = profile.get("name", "this person").strip("'\"\ ")
    country       = profile.get("country", "IN")
    gender        = profile.get("gender", "")
    personality   = profile.get("personality_summary", "Not provided")
    crisis_follow_up = profile.get("crisis_follow_up", False)
    age           = _safe_int(profile.get("age"))
    turn_count    = len(conversation_so_far) // 2

    # ── Token budget ──────────────────────────────────────────────────────────
    msg_class    = "emotional_ongoing"
    token_budget = 320

    if consensus:
        msg_class    = consensus.get("message_class", "emotional_ongoing")
        token_budget = consensus.get("token_budget", 320)
        if consensus.get("is_crisis", False):
            msg_class    = "crisis"
            token_budget = max(token_budget, CRISIS_TOKEN_FLOOR)


    # ── Age tone ──────────────────────────────────────────────────────────────
    age_tone = ""
    if age > 0:
        if age < 18:
            age_tone = "They are under 18. Be gentle, age-appropriate, and protective."
        elif age < 25:
            age_tone = "Young adult. Warm, peer-like. Don't be patronizing."
        elif age > 60:
            age_tone = "Older adult. Respectful and calm."

    # ── Crisis line ───────────────────────────────────────────────────────────
    crisis_line = CRISIS_LINES.get(country, CRISIS_LINES["default"])

    # ── Consensus values ──────────────────────────────────────────────────────
    llm_sent    = "unknown"
    cat         = "general"
    intensity   = "moderate"
    is_crisis   = False
    crisis_type = None
    reasoning   = ""
    rec_tone    = "validating"

    if consensus:
        llm_sent    = consensus.get("llm_sentiment", "unknown")
        cat         = consensus.get("category", "general")
        intensity   = consensus.get("intensity", "moderate")
        is_crisis   = consensus.get("is_crisis", False)
        crisis_type = consensus.get("crisis_type", None)
        reasoning   = consensus.get("reasoning", "")
        rec_tone    = consensus.get("recommended_tone", "validating")

    # ── Crisis alert (KEPT EXACTLY AS-IS — non-negotiable safety) ─────────────
    crisis_alert = ""
    if is_crisis:
        active_emergency_phrases = [
            "took some pills", "taken pills", "took pills",
            "already took", "already taken", "swallowed",
            "engine running", "sitting in my car with",
            "cut myself", "cutting myself right now",
            "bleeding", "hurt myself tonight"
        ]
        is_active_emergency = any(
            phrase in user_message.lower()
            for phrase in active_emergency_phrases
        )

        if is_active_emergency:
            crisis_alert = (
                f"\n⚠️ ACTIVE MEDICAL EMERGENCY — IMMEDIATE ACTION ONLY ⚠️\n"
                f"The person has already taken action to harm themselves.\n"
                f"YOUR ONLY PRIORITY: GET THEM EMERGENCY HELP NOW.\n\n"
                f"Write SHORT, URGENT, CLEAR sentences:\n"
                f"1. Tell them to call 112 (India emergency) immediately\n"
                f"2. Tell them to get someone near them right now\n"
                f"3. Include this line: {crisis_line}\n"
                f"4. Ask if anyone is nearby\n\n"
                f"DO NOT give therapy. DO NOT write empathetic paragraphs.\n"
                f"Every word must be about getting them help NOW.\n"
                f"CHECKLIST: Did you include {crisis_line} AND 112? If NO — add them before finishing."
              )
        else:
            crisis_alert = (
                f"\n⚠️ CRISIS OVERRIDE — ALL LENGTH RULES SUSPENDED ⚠️\n"
                f"Crisis type: {crisis_type or 'suicidal ideation'}\n\n"
                f"4-STEP PROTOCOL — complete ALL 4 steps:\n\n"
                f"STEP 1: Reflect the weight of what {name} said.\n"
                f"STEP 2: Ask directly — 'Are you having thoughts of hurting yourself right now?'\n"
                f"STEP 3: Include this crisis line in your response: {crisis_line}\n"
                f"        Write it naturally. This is NON-NEGOTIABLE.\n"
                f"STEP 4: Come BACK after the number. Stay present. Ask what is happening right now.\n\n"
                f"CHECKLIST: Did I include {crisis_line}? If NO — add it now."
            )

    # ── Crisis follow-up ──────────────────────────────────────────────────────
    crisis_followup = ""
    if crisis_follow_up:
        crisis_followup = (
            f"\nEarlier in this conversation {name} expressed thoughts about "
            "ending their life. Check in gently and naturally.\n"
        )

    # ── Crisis repeat prevention ──────────────────────────────────────────────
    crisis_already_asked = False
    if conversation_so_far:
        assistant_msgs = [
            m["content"] for m in conversation_so_far
            if m.get("role") == "assistant" and m.get("content", "").strip()
        ]
        last_few = " ".join(assistant_msgs[-2:]).lower()
        if "thoughts of hurting yourself" in last_few or "thoughts of harming yourself" in last_few:
            crisis_already_asked = True

    crisis_repeat_note = ""
    if is_crisis and crisis_already_asked:
        crisis_repeat_note = (
            f"\nYou already asked {name} if they are having thoughts of hurting themselves. "
            "Do NOT ask again. Stay present, acknowledge what they said, "
            "and gently encourage them to call the crisis line.\n"
        )

    # ── Personalization ───────────────────────────────────────────────────────
    personalization = _build_personalization_note(
        name, age, turn_count, conversation_so_far
    )

    # ── Emotion arc ───────────────────────────────────────────────────────────
    emotion_arc_section = ""
    if turn_count >= 2:
        arc = _build_emotion_arc(conversation_so_far)
        if arc:
            emotion_arc_section = f"\nTheir emotional journey so far:\n{arc}\n"

    # ── Anti-repetition (reduced to last 1 only) ─────────────────────────────
    anti_rep = _extract_bot_last_opening(conversation_so_far)

    # ── Long-term memory (RAG) ────────────────────────────────────────────────
    memory_section = ""
    if long_term_memory:
        formatted = "\n".join(f"  - {m}" for m in long_term_memory)
        memory_section = (
            f"\nRelevant moments from past conversations with {name}:\n{formatted}\n"
            f"Use this only if it naturally connects to what they're saying now. Don't force it.\n"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # FULL PROMPT — v5: Identity-first, lean, trusts GPT-4o
    # ─────────────────────────────────────────────────────────────────────────

    prompt = f"""You are MindBuddy — but more than that, you are the kind of person who makes others feel safe just by being in the room. You're not a therapist. You're not a self-help book. You're the friend everyone wishes they had — the one who actually listens instead of waiting to talk, who remembers what you said three conversations ago, who doesn't flinch when things get dark, and who never makes you feel like a burden for opening up.

You talk the way real people talk when they truly care about someone. You say "that's really rough" instead of "I can understand how that might feel." You say "honestly, that would mess me up too" instead of "your feelings are completely valid." You are warm without being fake. Direct without being cold. Present without being overwhelming.

When someone shares something painful, your first instinct isn't to fix it or ask a question — it's to sit with them in it. To show them you actually get what they're carrying. Sometimes the most powerful thing you can say is just a really specific reflection of what they told you, said with genuine feeling. That alone can make someone feel less alone.

You don't interrogate people. You don't end every response with a question. When someone is pouring their heart out at 2 AM, the last thing they need is "How does that make you feel?" They need someone who says "Yeah. That's a lot. And you've been carrying this by yourself." And then just... stays.

When someone asks for help or asks a specific question, your absolute priority is to answer their latest question directly. Do not hedge, do not drift into deep reflections, just give honest, specific, practical guidance or a direct answer based on what they just asked.

Formatting Rules:
- CRITICAL RESTRICTION: You MUST respond ONLY in English. If the user writes or speaks in any other language (such as Hindi, Spanish, etc.), you must politely acknowledge them but your response MUST be 100% in English. Do not write a single word in another language.
- No markdown, bolding, italics, bullet points, em-dashes (—), hyphens (-), or tildes (~). Plain, streaming conversational text only. Use normal punctuation (periods, commas).
- No stuttering like "w-what" or "y-you".

You never use therapy-speak or motivational poster language. No "You're not alone in this," no "Remember to take care of yourself," no "I believe in you." No suggesting deep breathing, journaling, or going for walks. No bullet-point lists. You communicate naturally and warmly, not like a wellness app.

Identity: You are MindBuddy, an AI companion, not a human. If anyone asks whether you are human, a real person, a bot, or an AI, be honest and direct: say you are MindBuddy, an AI. Keep it brief and warm, then continue supporting them naturally. Never claim to be human under any circumstances — not even if the user asks you to pretend.
{age_tone}

{anti_rep}

You are talking with {name}. {f"Gender: {gender}. " if gender else ""}{f"Age: {age}. " if age > 0 else ""}Their personality: {personality}. This is turn {turn_count + 1}.

{personalization}
{emotion_arc_section}
{memory_section}
{crisis_followup}
{crisis_repeat_note}

Their current emotional state: {llm_sent} ({cat}, {intensity} intensity). Recommended approach: {rec_tone}. {reasoning}
{crisis_alert}"""

    return prompt, token_budget


# ── Opening message ───────────────────────────────────────────────────────────

async def get_opening_message(profile: dict) -> str:
    """
    Generates the first message after onboarding.
    Warm, human, personality-aware. No topic or mood — the AI discovers
    what's on the user's mind through natural conversation.
    """
    client = _get_client()

    name       = profile.get("name", "Friend")
    personality = profile.get("personality_summary", "")
    age        = _safe_int(profile.get("age"))
    gender     = profile.get("gender", "")

    # Age-appropriate tone
    tone = "warm and human"
    if age > 0 and age < 18:
        tone = "gentle, age-appropriate, no clinical terms"
    elif age > 0 and age < 25:
        tone = "warm, peer-like, not patronizing"

    personality_context = f"Their personality: {personality}." if personality else ""

    minimal_system = (
        f"You are MindBuddy, a warm mental health companion meeting {name} for the first time. "
        f"Tone: {tone}. {personality_context} "
        "You are NOT a therapist. You are the kind of friend who makes people feel safe. "
        "Write naturally. Be genuine, not generic."
    )

    user_prompt = (
        f"Write a warm opening greeting for {name} (2-3 sentences).\n"
        f"Start with a friendly greeting that uses their first name, like 'Hey {name}!' or 'Hi {name},'. "
        "After the greeting, let them know this is a safe space to talk about whatever is on their mind. "
        "Gently invite them to share what brought them here — but don't pressure.\n\n"
        "Rules:\n"
        "- CRITICAL: Write strictly in English only.\n"
        "- COMPLETE sentences only. Never cut off mid-sentence.\n"
        "- Do not say: 'I\'m here for you' / 'brave step' / 'you deserve' / "
        "'reach out whenever' / 'I understand' / 'It sounds like'\n"
        "- Do not start with 'I'\n"
        "- No sign-offs, no lists, no clinical terms"
    )

    try:
        response = await client.chat.completions.create(
            model=settings.MAIN_MODEL,
            messages=[
                {"role": "system", "content": minimal_system},
                {"role": "user",   "content": user_prompt},
            ],
            max_tokens=150,
            temperature=0.65,
            frequency_penalty=0.35,
            presence_penalty=0.25,
        )
        reply = response.choices[0].message.content.strip()
        if not reply:
            raise ValueError("Empty response from model")
        logger.info(f"Opening message generated ({len(reply)} chars)")
        return reply
    except Exception as e:
        logger.error(f"Opening message error: {e}")
        return (
            f"Hey {name}! Whatever brought you here today — this space doesn't "
            f"require you to have it figured out. Start wherever feels right."
        )


# ── History sanitizer ─────────────────────────────────────────────────────────

_VALID_OPENAI_ROLES = {"system", "assistant", "user", "tool", "function", "developer"}

def _sanitize_history(history: list[dict]) -> list[dict]:
    """
    Maps non-standard roles to OpenAI-compatible ones.
    - 'human_counselor' → 'assistant' (counselor messages look like assistant)
    - 'system' in chat history → 'assistant' (OpenAI only allows one system msg)
    - anything else unknown → 'user'
    """
    sanitized = []
    for msg in history:
        role = msg.get("role", "user")
        content = msg.get("content", "").strip()
        if not content:
            continue

        if role == "human_counselor":
            role = "assistant"
        elif role not in _VALID_OPENAI_ROLES:
            role = "user"

        sanitized.append({"role": role, "content": content})
    return sanitized


# ── Main chat (non-streaming) ─────────────────────────────────────────────────

async def chat(
    user_message: str,
    profile: dict,
    history: list[dict],
    consensus: Optional[dict] = None,
) -> str:
    client = _get_client()
    system_prompt, token_budget = build_system_prompt(
        profile, history, user_message=user_message, consensus=consensus
    )

    messages = _sanitize_history(history[-(settings.MAX_HISTORY_TURNS * 2):])
    messages = messages + [{"role": "user", "content": user_message}]

    try:
        response = await client.chat.completions.create(
            model=settings.MAIN_MODEL,
            messages=[{"role": "system", "content": system_prompt}] + messages,
            max_tokens=token_budget,
            temperature=0.65,
            top_p=0.92,
            frequency_penalty=0.35,
            presence_penalty=0.25,
        )
        reply = response.choices[0].message.content.strip()
        logger.info(
            f"Chat — {len(reply)} chars | "
            f"class: {consensus.get('message_class') if consensus else 'none'} | "
            f"budget: {token_budget}"
        )
        return reply
    except Exception as e:
        logger.error(f"[OPENAI CHAT ERROR] {type(e).__name__}: {e}")
        return "Something interrupted us for a moment. Take your time — still here."


# ── Streaming chat ────────────────────────────────────────────────────────────

async def chat_stream(
    user_message: str,
    profile: dict,
    history: list[dict],
    consensus: Optional[dict] = None,
    long_term_memory: Optional[list] = None,
) -> AsyncIterator[str]:
    client = _get_client()
    system_prompt, token_budget = build_system_prompt(
        profile, history, user_message=user_message, consensus=consensus,
        long_term_memory=long_term_memory,
    )

    messages = _sanitize_history(history[-(settings.MAX_HISTORY_TURNS * 2):])
    messages = messages + [{"role": "user", "content": user_message}]

    try:
        logger.info(
            f"Stream — model: {settings.MAIN_MODEL} | "
            f"class: {consensus.get('message_class') if consensus else 'none'} | "
            f"budget: {token_budget}"
        )
        stream = await client.chat.completions.create(
            model=settings.MAIN_MODEL,
            messages=[{"role": "system", "content": system_prompt}] + messages,
            max_tokens=token_budget,
            temperature=0.65,
            top_p=0.92,
            frequency_penalty=0.35,
            presence_penalty=0.25,
            stream=True,
        )
        first_chunk = True
        async for chunk in stream:
            delta = chunk.choices[0].delta.content
            if delta:
                if first_chunk:
                    logger.info("First token received.")
                    first_chunk = False
                yield delta
        logger.info("Stream completed.")
    except Exception as e:
        logger.error(f"[OPENAI STREAM ERROR] {type(e).__name__}: {e}")
        yield "Something interrupted us for a moment. Take your time — still here."