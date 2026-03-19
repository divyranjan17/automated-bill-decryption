"""
interpreter.py — Phase 1 deterministic password hint extractor and parser.

Extracts a password instruction from an email body and parses it into a
structured PasswordRule-compatible dict using regex patterns only.
No LLM calls in Phase 1.
"""

import re
import logging
from typing import Optional

from src.schemas.password_rule import PasswordRule
from src.constants.failure_reasons import FailureReason

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WORD_TO_INT: dict[str, int] = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}

_PASSWORD_KEYWORDS = [
    "password is", "password:", "the password", "your password",
    "use your", "enter your", "enter the", "access your",
]

# Ordered longest-first to prevent partial matches (e.g. "ddmm" before "ddmmyyyy")
_DATE_FORMAT_MAP: dict[str, str] = {
    "ddmmyyyy": "DDMMYYYY",
    "ddmmyy": "DDMMYY",
    "ddmm": "DDMM",
    "mmddyyyy": "MMDDYYYY",
    "mmdd": "MMDD",
}

# Regex fragments
_NUM_RE = r"(\d+|one|two|three|four|five|six|seven|eight|nine|ten)"
_MODIFIER_RE = r"(?:(?:upper\s*case|uppercase|capital|lower\s*case|small)\s+)?"
_UNIT_RE = r"(?:letters?|chars?|characters?|digits?)"
_DATE_FMT_RE = r"(ddmmyyyy|ddmmyy|ddmm|mmddyyyy|mmdd)"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_int(token: str) -> Optional[int]:
    """Convert a numeric word or digit string to int."""
    token = token.lower()
    if token.isdigit():
        return int(token)
    return _WORD_TO_INT.get(token)


def _get_transform(text: str) -> Optional[str]:
    """Return 'upper', 'lower', or None based on modifier keywords in text."""
    if re.search(r"\b(?:upper\s*case|uppercase|capital\s+letters?|capitals?)\b", text):
        return "upper"
    if re.search(r"\b(?:lower\s*case|lowercase|small\s+(?:letters?|case))\b", text):
        return "lower"
    return None


def _make_component(
    field: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
    transform: Optional[str] = None,
    date_format: Optional[str] = None,
) -> dict:
    return {
        "field": field,
        "slice": {"start": start, "end": end} if (start is not None or end is not None) else None,
        "transform": transform,
        "date_format": date_format,
    }


def _make_variant(components: list[dict], separator: str = "") -> dict:
    """Build a PasswordRuleVariant-compatible dict (components + separator only)."""
    return {"components": components, "separator": separator}


def _make_rule(
    components: list[dict],
    separator: str = "",
    ambiguous: bool = False,
    confidence: str = "high",
    reasoning: str = "",
    fallback_candidates: Optional[list[dict]] = None,
) -> dict:
    return {
        "components": components,
        "separator": separator,
        "ambiguous": ambiguous,
        "confidence": confidence,
        "reasoning": reasoning,
        "requires_static_password": False,
        "fallback_candidates": fallback_candidates or [],
    }


# ---------------------------------------------------------------------------
# Pattern matchers (tried in priority order, most specific first)
# ---------------------------------------------------------------------------

_NAME_FIELD_RE = (
    r"first\s+" + _NUM_RE +
    r"\s+" + _MODIFIER_RE +
    _UNIT_RE +
    r"\s+of\s+(?:your\s+)?(?:(first|last)\s+)?name"
)

_DOB_FIELD_RE = (
    r"(?:date\s+of\s+birth|dob)[\s\(\[]*(?:in\s+)?" + _DATE_FMT_RE
)


def _parse_name_component(m: re.Match, full_text: str) -> tuple[dict, Optional[str]]:
    """
    Extract (component_dict, transform) from a name regex match.

    Returns:
        A tuple of (component dict, transform string or None).
    """
    n = _to_int(m.group(1))
    qualifier = (m.group(2) or "").strip()
    field = "last_name" if qualifier == "last" else "first_name"
    transform = _get_transform(full_text)
    return _make_component(field, 0, n, transform), transform


def _try_ambiguous_options(text: str) -> Optional[dict]:
    """
    Handles emails with 'Option 1: ...' and 'Option 2: ...' structures
    (e.g. Axis Bank). Returns an ambiguous rule with fallback_candidates.
    """
    if "option 1" not in text or "option 2" not in text:
        return None

    parts = re.split(r"option\s+\d+\s*:", text)
    if len(parts) < 3:
        return None

    variants: list[dict] = []
    for option_text in parts[1:]:
        parsed = (
            _try_name_plus_dob(option_text)
            or _try_name_plus_card_digits(option_text)
            or _try_name_plus_birth_year(option_text)
            or _try_name_only(option_text)
        )
        if parsed:
            variants.append(_make_variant(parsed["components"], parsed["separator"]))

    if len(variants) < 2:
        return None

    return _make_rule(
        components=variants[0]["components"],
        ambiguous=True,
        confidence="medium",
        reasoning=f"Ambiguous instruction: {len(variants)} options detected.",
        fallback_candidates=variants,
    )


def _try_name_plus_dob(text: str) -> Optional[dict]:
    """
    Handles: "first N letters of name [modifier] ... date of birth in FORMAT"
    E.g.: Axis Bank Option 1, ICICI Bank.
    """
    name_m = re.search(_NAME_FIELD_RE, text)
    dob_m = re.search(_DOB_FIELD_RE, text)
    if not name_m or not dob_m:
        return None

    n = _to_int(name_m.group(1))
    if n is None:
        return None

    qualifier = (name_m.group(2) or "").strip()
    field = "last_name" if qualifier == "last" else "first_name"
    transform = _get_transform(text)
    date_fmt = _DATE_FORMAT_MAP.get(dob_m.group(1))

    return _make_rule(
        [
            _make_component(field, 0, n, transform),
            _make_component("dob", date_format=date_fmt),
        ],
        separator="",
        reasoning=(
            f"First {n} chars of {field} (transform={transform}) "
            f"+ DOB in {date_fmt}."
        ),
    )


def _try_name_plus_birth_year(text: str) -> Optional[dict]:
    """
    Handles: "first N letters of name followed by birth year / year of birth"
    """
    name_m = re.search(_NAME_FIELD_RE, text)
    year_m = re.search(r"(?:birth\s+year|year\s+of\s+birth)", text)
    if not name_m or not year_m:
        return None

    n = _to_int(name_m.group(1))
    if n is None:
        return None

    qualifier = (name_m.group(2) or "").strip()
    field = "last_name" if qualifier == "last" else "first_name"
    transform = _get_transform(text)

    return _make_rule(
        [
            _make_component(field, 0, n, transform),
            _make_component("dob", date_format="YYYY"),
        ],
        separator="",
        reasoning=f"First {n} chars of {field} (transform={transform}) + birth year.",
    )


def _try_name_plus_card_digits(text: str) -> Optional[dict]:
    """
    Handles: "first N letters of name + last M digits of credit card"
    E.g.: Axis Bank Option 2.
    """
    name_m = re.search(_NAME_FIELD_RE, text)
    card_m = re.search(
        r"last\s+" + _NUM_RE + r"\s+digits?\s+of\s+(?:your\s+)?(?:credit\s+)?card",
        text,
    )
    if not name_m or not card_m:
        return None

    name_n = _to_int(name_m.group(1))
    card_n = _to_int(card_m.group(1))
    if name_n is None or card_n is None:
        return None

    qualifier = (name_m.group(2) or "").strip()
    field = "last_name" if qualifier == "last" else "first_name"
    transform = _get_transform(text)

    return _make_rule(
        [
            _make_component(field, 0, name_n, transform),
            _make_component("card_masked", -card_n, None),
        ],
        separator="",
        reasoning=(
            f"First {name_n} chars of {field} (transform={transform}) "
            f"+ last {card_n} digits of card."
        ),
    )


def _try_pan(text: str) -> Optional[dict]:
    """
    Handles:
    - "first N characters of PAN"
    - "PAN number in upper case" (full PAN)
    E.g.: NSE Alerts.
    """
    # Sliced PAN takes priority
    m = re.search(
        r"first\s+" + _NUM_RE + r"\s+" + _MODIFIER_RE + _UNIT_RE +
        r"\s+of\s+(?:your\s+)?pan\b",
        text,
    )
    if m:
        n = _to_int(m.group(1))
        if n is not None:
            transform = _get_transform(text)
            return _make_rule(
                [_make_component("pan", 0, n, transform)],
                reasoning=f"First {n} characters of PAN (transform={transform}).",
            )

    # Full PAN
    if re.search(r"\bpan\b", text):
        transform = _get_transform(text)
        return _make_rule(
            [_make_component("pan", transform=transform)],
            reasoning=f"Full PAN (transform={transform}).",
        )

    return None


def _try_dob_only(text: str) -> Optional[dict]:
    """Handles: "date of birth in FORMAT" as a standalone rule."""
    m = re.search(_DOB_FIELD_RE, text)
    if not m:
        return None
    date_fmt = _DATE_FORMAT_MAP.get(m.group(1))
    return _make_rule(
        [_make_component("dob", date_format=date_fmt)],
        reasoning=f"DOB in {date_fmt} format.",
    )


def _try_mobile(text: str) -> Optional[dict]:
    """Handles: "last N digits of mobile/phone number"."""
    m = re.search(
        r"last\s+" + _NUM_RE +
        r"\s+digits?\s+of\s+(?:your\s+)?(?:mobile|phone)(?:\s+number)?",
        text,
    )
    if not m:
        return None
    n = _to_int(m.group(1))
    if n is None:
        return None
    return _make_rule(
        [_make_component("mobile", -n, None)],
        reasoning=f"Last {n} digits of mobile number.",
    )


def _try_name_only(text: str) -> Optional[dict]:
    """
    Handles: "first N [modifier] letters of your [first|last] name [modifier]"
    E.g.: standalone name-only rules.
    """
    m = re.search(_NAME_FIELD_RE, text)
    if not m:
        return None
    n = _to_int(m.group(1))
    if n is None:
        return None
    qualifier = (m.group(2) or "").strip()
    field = "last_name" if qualifier == "last" else "first_name"
    transform = _get_transform(text)
    return _make_rule(
        [_make_component(field, 0, n, transform)],
        reasoning=f"First {n} chars of {field} (transform={transform}).",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_password_hint(body_text: str) -> Optional[str]:
    """
    Scans an email body for the sentence or paragraph containing the
    password instruction. Returns the extracted hint string, or None if
    no password-related content is found.

    Args:
        body_text: Raw email body text (plain text or pre-stripped HTML).

    Returns:
        A single hint string (normalized, whitespace collapsed), or None.
    """
    if not body_text or not body_text.strip():
        return None

    # Paragraph-level search (captures multi-sentence instructions like Axis Bank)
    paragraphs = re.split(r"\n{2,}", body_text)
    for para in paragraphs:
        if any(kw in para.lower() for kw in _PASSWORD_KEYWORDS):
            return re.sub(r"\s+", " ", para.strip())

    # Fallback: sentence-level search
    normalized = re.sub(r"\s+", " ", body_text.strip())
    for sentence in re.split(r"(?<=[.!?])\s+", normalized):
        if any(kw in sentence.lower() for kw in _PASSWORD_KEYWORDS):
            return sentence.strip()

    return None


def interpret_instruction(instruction: str, user_data: dict) -> dict:
    """
    Parses a normalized password instruction string into a structured
    rule dict matching the PasswordRule schema.

    Args:
        instruction: Extracted hint string (e.g. "first 4 letters of
                     your name followed by birth year").
        user_data:   User profile dict (reserved for Phase 3 — not used here).

    Returns:
        Dict with keys: components, separator, ambiguous, confidence,
        reasoning, requires_static_password, fallback_candidates.

    Raises:
        ValueError("HINT_FOUND_BUT_UNPARSABLE") if no pattern matches or
        if the produced dict fails PasswordRule schema validation.
    """
    if not instruction or not instruction.strip():
        raise ValueError(FailureReason.HINT_FOUND_BUT_UNPARSABLE.value)

    # Normalize: lowercase, collapse whitespace, strip select punctuation
    text = re.sub(r"\s+", " ", instruction.strip().lower())
    text = re.sub(r"[()&\[\]]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    result = (
        _try_ambiguous_options(text)
        or _try_name_plus_dob(text)
        or _try_name_plus_birth_year(text)
        or _try_name_plus_card_digits(text)
        or _try_pan(text)
        or _try_dob_only(text)
        or _try_mobile(text)
        or _try_name_only(text)
    )

    if result is None:
        logger.warning("No pattern matched instruction: %r", instruction)
        raise ValueError(FailureReason.HINT_FOUND_BUT_UNPARSABLE.value)

    # Validate against Pydantic schema before returning
    try:
        PasswordRule(**result)
    except Exception as exc:
        logger.error("Schema validation failed for parsed rule: %s", exc)
        raise ValueError(FailureReason.HINT_FOUND_BUT_UNPARSABLE.value) from exc

    return result
