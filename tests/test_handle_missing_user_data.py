"""Tests for handle_missing_user_data.py."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from handle_missing_user_data import prompt_missing_fields

_BASE_USER = {
    "name": "John Doe",
    "dob": "1990-01-15",
    "mobile": "9876543210",
}


def test_prompt_returns_updated_dict():
    """Returned dict contains the new value for the prompted field."""
    with patch("builtins.input", return_value="Jane Smith"):
        result = prompt_missing_fields(["name"], _BASE_USER)
    assert result["name"] == "Jane Smith"


def test_validation_retries_on_bad_format():
    """Bad regex input causes re-prompt; second valid input is accepted."""
    inputs = iter(["bad-dob", "1990-05-20"])
    with patch("builtins.input", side_effect=inputs), \
         patch("builtins.print"):  # suppress "Invalid format" message
        result = prompt_missing_fields(["dob"], _BASE_USER)
    assert result["dob"] == "1990-05-20"


def test_unknown_field_prompts_generically():
    """Fields not in FIELD_PROMPTS use a generic prompt with no validation."""
    with patch("builtins.input", return_value="XYZ123") as mock_input:
        result = prompt_missing_fields(["unknown_field"], _BASE_USER)
    mock_input.assert_called_once_with("Enter value for unknown_field: ")
    assert result["unknown_field"] == "XYZ123"


def test_existing_fields_preserved():
    """Non-prompted fields from the original user dict are preserved."""
    with patch("builtins.input", return_value="12345678"):
        result = prompt_missing_fields(["card_masked"], _BASE_USER)
    assert result["name"] == _BASE_USER["name"]
    assert result["dob"] == _BASE_USER["dob"]
    assert result["mobile"] == _BASE_USER["mobile"]
    assert result["card_masked"] == "12345678"


def test_empty_missing_list_returns_unchanged_user():
    """No prompts issued and returned dict equals the original when list is empty."""
    with patch("builtins.input") as mock_input:
        result = prompt_missing_fields([], _BASE_USER)
    mock_input.assert_not_called()
    assert result == _BASE_USER


def test_original_user_dict_not_mutated():
    """The original user dict passed in is not modified."""
    original = dict(_BASE_USER)
    with patch("builtins.input", return_value="ABCDE1234F"):
        prompt_missing_fields(["pan"], original)
    assert original == _BASE_USER
