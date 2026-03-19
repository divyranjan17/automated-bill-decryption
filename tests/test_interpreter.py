"""Tests for interpreter.py — extract_password_hint and interpret_instruction."""

import pytest

from interpreter import extract_password_hint, interpret_instruction
from src.schemas.password_rule import PasswordRule, PasswordRuleVariant


# ---------------------------------------------------------------------------
# extract_password_hint
# ---------------------------------------------------------------------------

class TestExtractPasswordHint:
    def test_no_hint_returns_none(self):
        body = "Dear Customer, your account statement is now available."
        assert extract_password_hint(body) is None

    def test_empty_body_returns_none(self):
        assert extract_password_hint("") is None
        assert extract_password_hint("   ") is None

    def test_none_body_returns_none(self):
        assert extract_password_hint(None) is None

    def test_extracts_hint_sentence(self):
        body = (
            "Dear Customer.\n\n"
            "Password is first 4 letters of your name followed by birth year.\n\n"
            "Thank you."
        )
        hint = extract_password_hint(body)
        assert hint is not None
        assert "password is" in hint.lower()
        assert "4 letters" in hint.lower()

    def test_multiple_hints_returns_first(self):
        body = (
            "Password is first 4 letters of your name.\n\n"
            "Your password has been updated."
        )
        hint = extract_password_hint(body)
        assert hint is not None
        assert "4 letters" in hint.lower()

    def test_whitespace_collapsed(self):
        body = "  Password  is   first 4   letters of your name.  "
        hint = extract_password_hint(body)
        assert hint is not None
        assert "  " not in hint  # no double spaces


# ---------------------------------------------------------------------------
# interpret_instruction
# ---------------------------------------------------------------------------

class TestInterpretInstruction:
    def test_first_n_chars_of_name(self):
        result = interpret_instruction("first 4 letters of your name", {})
        c = result["components"][0]
        assert c["field"] == "name"
        assert c["slice"] == {"start": 0, "end": 4}
        assert c["transform"] is None
        assert result["ambiguous"] is False

    def test_first_n_chars_uppercase(self):
        result = interpret_instruction("first 4 capital letters of your name", {})
        assert result["components"][0]["transform"] == "upper"

    def test_first_n_chars_uppercase_suffix_modifier(self):
        result = interpret_instruction("first four letters of your name in upper case", {})
        assert result["components"][0]["transform"] == "upper"
        assert result["components"][0]["slice"] == {"start": 0, "end": 4}

    def test_dob_ddmm(self):
        result = interpret_instruction("date of birth in DDMM format", {})
        c = result["components"][0]
        assert c["field"] == "dob"
        assert c["date_format"] == "DDMM"

    def test_dob_ddmmyyyy(self):
        result = interpret_instruction("date of birth in DDMMYYYY", {})
        assert result["components"][0]["date_format"] == "DDMMYYYY"

    def test_mobile_last_4(self):
        result = interpret_instruction("last 4 digits of mobile number", {})
        c = result["components"][0]
        assert c["field"] == "mobile"
        assert c["slice"]["start"] == -4
        assert c["slice"]["end"] is None

    def test_pan_first_5(self):
        result = interpret_instruction("first 5 characters of PAN", {})
        c = result["components"][0]
        assert c["field"] == "pan"
        assert c["slice"] == {"start": 0, "end": 5}

    def test_name_plus_birth_year(self):
        result = interpret_instruction(
            "first 4 letters of name followed by birth year", {}
        )
        assert len(result["components"]) == 2
        assert result["components"][0]["field"] == "name"
        assert result["components"][0]["slice"] == {"start": 0, "end": 4}
        assert result["components"][1]["field"] == "dob"
        assert result["components"][1]["date_format"] == "YYYY"

    def test_name_plus_dob_two_component_rule(self):
        result = interpret_instruction(
            "first 4 letters of your name and date of birth in DDMM format", {}
        )
        assert len(result["components"]) == 2
        assert result["components"][0]["field"] == "name"
        assert result["components"][1]["field"] == "dob"
        assert result["components"][1]["date_format"] == "DDMM"

    def test_no_pattern_match_raises(self):
        with pytest.raises(ValueError, match="HINT_FOUND_BUT_UNPARSABLE"):
            interpret_instruction("the document is blah blah blah xyz", {})

    def test_empty_instruction_raises(self):
        with pytest.raises(ValueError):
            interpret_instruction("", {})

    def test_none_equivalent_raises(self):
        with pytest.raises(ValueError):
            interpret_instruction("   ", {})

    def test_output_validates_against_schema(self):
        result = interpret_instruction("first 4 letters of your name", {})
        # Should not raise
        rule = PasswordRule(**result)
        assert rule.ambiguous is False

    def test_ambiguous_axis_bank_style(self):
        """Two-option email produces ambiguous=True with two fallback_candidates."""
        body = (
            "Option 1: enter the first four letters of your name in upper case "
            "and your date of birth in ddmm format. "
            "Option 2: enter the first four letters of your name in upper case "
            "and the last four digits of your credit card number."
        )
        result = interpret_instruction(body, {})
        assert result["ambiguous"] is True
        assert len(result["fallback_candidates"]) == 2

    def test_fallback_candidates_are_variants(self):
        """Each fallback_candidate must validate as PasswordRuleVariant (components + separator only)."""
        body = (
            "Option 1: enter the first four letters of your name in upper case "
            "and your date of birth in ddmm format. "
            "Option 2: enter the first four letters of your name in upper case "
            "and the last four digits of your credit card number."
        )
        result = interpret_instruction(body, {})
        for candidate in result["fallback_candidates"]:
            # Must only contain keys valid for PasswordRuleVariant
            variant = PasswordRuleVariant(**candidate)
            assert len(variant.components) > 0

    def test_pan_full_uppercase(self):
        """Full PAN in upper case (NSE Alerts style)."""
        result = interpret_instruction("the password is your pan number in upper case", {})
        c = result["components"][0]
        assert c["field"] == "pan"
        assert c["slice"] is None
        assert c["transform"] == "upper"
