"""handle_missing_user_data.py — prompting user for missing fields.

Prompts the user interactively for any fields flagged as missing by
build_candidates(). Validates known fields against expected formats
before accepting. Returns an updated user dict without mutating the
original. No database imports.
"""

from __future__ import annotations

import re
from typing import Optional

# (label, format_hint, regex_pattern or None)
FIELD_PROMPTS: dict[str, tuple[str, str, Optional[str]]] = {
    "name":           ("Full name",                "e.g. John Doe",   None),
    "dob":            ("Date of birth",            "YYYY-MM-DD",      r"^\d{4}-\d{2}-\d{2}$"),
    "mobile":         ("Mobile number",            "10 digits",       r"^\d{10}$"),
    "pan":            ("PAN number",               "ABCDE1234F",      r"^[A-Z]{5}[0-9]{4}[A-Z]$"),
    "card_masked":    ("Last 8 digits of card",    "8 digits",        r"^\d{8}$"),
    "account_masked": ("Last 8 digits of account", "8 digits",        r"^\d{8}$"),
}


def prompt_missing_fields(missing_fields: list[str], user: dict) -> dict:
    """Prompt the CLI user to supply values for each missing field.

    For fields listed in FIELD_PROMPTS, shows a label and format hint
    and re-prompts until the value matches the expected regex (if any).
    For unknown fields, shows a generic prompt with no validation.

    Args:
        missing_fields: Ordered list of field names to prompt for.
        user: Current user dict (not mutated).

    Returns:
        A copy of ``user`` with the prompted values merged in.
    """
    updated = dict(user)

    for field in missing_fields:
        if field in FIELD_PROMPTS:
            label, hint, pattern = FIELD_PROMPTS[field]
            while True:
                value = input(f"{label} ({hint}): ").strip()
                if pattern is None or re.fullmatch(pattern, value):
                    break
                print(f"  Invalid format. Expected: {hint}")
        else:
            value = input(f"Enter value for {field}: ").strip()

        updated[field] = value

    return updated
