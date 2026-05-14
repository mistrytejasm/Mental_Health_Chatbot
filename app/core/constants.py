"""
Core Constants
──────────────
Centralised constants used across the application.
Crisis lines are verified and mapped by ISO 3166-1 alpha-2 country codes.
"""
from app.core.logger import get_logger

logger = get_logger(__name__)
logger.debug("Loading core constants")

# ── Crisis Lines ──────────────────────────────────────────────────────────────
# Keyed by ISO country code (uppercase 2-letter).
# The system prompt injects the matching line based on profile.country.
# Verified lines only — do not add unverified numbers.

CRISIS_LINES = {
    # Asia
    "IN": "iCall (India): 9152987821 | Vandrevala Foundation: 1860-2662-345 (24/7)",
    "SG": "Samaritans of Singapore: 1767 (24/7)",
    "PH": "Hopeline Philippines: 8804-4673 or text HELLO to 4673",
    "MY": "Befrienders Kuala Lumpur: 03-7627-2929",
    "JP": "Inochi no Denwa (Japan): 0120-783-556",
    "PK": "Umang Pakistan: 0317-4288665",
    "BD": "Kaan Pete Roi (Bangladesh): 01779-554391",

    # North America
    "US": "911 Emergency: Call 911 immediately | Crisis Text Line: text HOME to 741741",
    "CA": "Crisis Services Canada: 1-833-456-4566 | Talk Suicide Canada: 1-833-456-4566",
    "MX": "SAPTEL (Mexico): 55 5259-8121 (24/7)",

    # Europe
    "GB": "Samaritans UK: 116 123 (free, 24/7) | Crisis Text Line: text SHOUT to 85258",
    "UK": "Samaritans UK: 116 123 (free, 24/7) | Crisis Text Line: text SHOUT to 85258",  # alias
    "IE": "Samaritans Ireland: 116 123 | Pieta House: 116 123",
    "DE": "Telefonseelsorge Germany: 0800 111 0 111 (free, 24/7)",
    "FR": "Numéro National Prévention Suicide (France): 3114 (24/7)",
    "NL": "113 Zelfmoordpreventie (Netherlands): 113 or 0800-0113",
    "BE": "Centre de Prévention du Suicide (Belgium): 0800 32 123",
    "ES": "Teléfono de la Esperanza (Spain): 717 003 717",
    "IT": "Telefono Amico (Italy): 02 2327 2327",
    "PT": "SOS Voz Amiga (Portugal): 213 544 545",
    "SE": "Mind Självmordslinjen (Sweden): 90101",
    "NO": "Mental Helse (Norway): 116 123",
    "DK": "Livslinien (Denmark): 70 201 201",
    "FI": "Mieli Mental Health Finland: 09 2525 0111",
    "PL": "Telefon Zaufania (Poland): 116 123",
    "CH": "Die Dargebotene Hand (Switzerland): 143",
    "AT": "Telefonseelsorge Austria: 142",
    "RU": "Russian Psychological Help: 8-800-2000-122",

    # Oceania
    "AU": "Lifeline Australia: 13 11 14 | Beyond Blue: 1300 22 4636",
    "NZ": "Lifeline New Zealand: 0800 543 354 | Need to Talk: text or call 1737",

    # Middle East & Africa
    "ZA": "SADAG South Africa: 0800 456 789 | Suicide Crisis Line: 0800 567 567",
    "IL": "ERAN Israel: 1201 (24/7, multilingual)",
    "AE": "Counselling & Well-Being Line UAE: 800 HOPE (4673)",

    # Latin America
    "BR": "CVV Brazil: 188 (24/7) | chat at cvv.org.br",
    "AR": "Centro de Asistencia al Suicida Argentina: 135",
    "CL": "Fono Salud Responde Chile: 600 360 7777",

    # Default fallback (shown when country not mapped)
    "default": (
        "Please reach out to a crisis line in your country. "
        "You can find your local line at findahelpline.com"
    ),
}