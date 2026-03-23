"""
rule_engine.py — Deterministic password candidate generator.

Takes a validated PasswordRule-compatible dict and a user profile dict,
and produces a list of candidate password strings. Zero external
dependencies; no LLM knowledge.
"""

import re
import logging
from datetime import datetime
from typing import Optional

from src.constants.failure_reasons import FailureReason

logger = logging.getLogger(__name__)

_NAME_FIELDS = {"name"}


def _normalize_name(value: str) -> str:
    """Strip periods and spaces from a name string before slicing.

    Axis Bank (and similar institutions) instruct users to ignore spaces
    and periods when deriving the password from their name.
    E.g. "C.K. Ajay Kumar" → "CKAjayKumar".
    """
    return re.sub(r"[.\s]", "", value)


def format_date(dob: str, date_format: str) -> str:
    """
    Formats a stored DOB string (YYYY-MM-DD) to the requested format.

    Args:
        dob: Date string in ISO format YYYY-MM-DD.
        date_format: One of DDMM, DDMMYY, DDMMYYYY, MMDD, MMDDYYYY, YYYY.

    Returns:
        Formatted date string.

    Raises:
        ValueError for invalid DOB strings or unsupported format strings.
    """
    try:
        dt = datetime.strptime(dob, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(
            f"Invalid DOB format: {dob!r}. Expected YYYY-MM-DD (ISO)."
        ) from exc

    fmt_map: dict[str, str] = {
        "DDMM": f"{dt.day:02d}{dt.month:02d}",
        "DDMMYY": f"{dt.day:02d}{dt.month:02d}{str(dt.year)[-2:]}",
        "DDMMYYYY": f"{dt.day:02d}{dt.month:02d}{dt.year:04d}",
        "MMDD": f"{dt.month:02d}{dt.day:02d}",
        "MMDDYYYY": f"{dt.month:02d}{dt.day:02d}{dt.year:04d}",
        "YYYY": f"{dt.year:04d}",
    }

    if date_format not in fmt_map:
        raise ValueError(
            f"Unsupported date_format: {date_format!r}. "
            f"Allowed: {list(fmt_map.keys())}"
        )

    return fmt_map[date_format]


def _get(obj, key):
    """Get a value from a dict or Pydantic model attribute."""
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def build_candidate(rule: dict, user: dict) -> str:
    """
    Generates a single password string from a non-ambiguous rule + user data.

    Args:
        rule: Dict with keys: components, separator (matches PasswordRule /
              PasswordRuleVariant schema).
        user: User profile dict mapping field names to string values.

    Returns:
        A single candidate password string.

    Raises:
        ValueError(FailureReason.REQUIRED_USER_DATA_MISSING) if a
        required field is absent from the user dict.
    """
    parts: list[str] = []
    components = _get(rule, "components") or []
    missing = [] # storing missing fields for prompting user for manual escalation

    for component in components:
        field = _get(component, "field")
        # Support both AllowedField enum and raw string
        field_str = field.value if hasattr(field, "value") else str(field)

        if field_str not in user:
            # raise ValueError(FailureReason.REQUIRED_USER_DATA_MISSING.value)
            missing.append(field_str)
            continue

        value: str = user[field_str]

        # Normalize name fields: strip periods and spaces before any slicing
        if field_str in _NAME_FIELDS:
            value = _normalize_name(value)

        date_format = _get(component, "date_format")
        if date_format:
            fmt_str = date_format.value if hasattr(date_format, "value") else str(date_format)
            value = format_date(value, fmt_str)

        slice_rule = _get(component, "slice")
        if slice_rule is not None:
            start = _get(slice_rule, "start")
            end = _get(slice_rule, "end")
            value = value[start:end]

        transform = _get(component, "transform")
        transform_str = transform.value if hasattr(transform, "value") else transform
        if transform_str == "upper":
            value = value.upper()
        elif transform_str == "lower":
            value = value.lower()
        # None / "none" → no-op

        parts.append(value)

    if missing:
        raise ValueError(
            f"{FailureReason.REQUIRED_USER_DATA_MISSING.value}:{','.join(missing)}"
        )
    separator = _get(rule, "separator") or ""
    return separator.join(parts)


def build_candidates(rule: dict, user: dict) -> list[str]:
    """
    Generates the full candidate list from a PasswordRule dict.

    - If ambiguous=False: returns [build_candidate(rule, user)]
    - If ambiguous=True:  iterates fallback_candidates and returns
      [build_candidate(variant, user) for variant in fallback_candidates]

    Args:
        rule: Full PasswordRule-compatible dict (may include fallback_candidates).
        user: User profile dict.

    Returns:
        Non-empty list of candidate password strings.

    Raises:
        ValueError(FailureReason.REQUIRED_USER_DATA_MISSING) if a
        required user field is absent.
    """
    ambiguous = _get(rule, "ambiguous")

    if ambiguous:
        fallback = _get(rule, "fallback_candidates") or []
        if not fallback:
            logger.warning(
                "Rule is ambiguous but has no fallback_candidates; "
                "falling back to main rule components."
            )
            return [build_candidate(rule, user)]
        return [build_candidate(variant, user) for variant in fallback]

    return [build_candidate(rule, user)]
