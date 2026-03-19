"""Tests for rule_engine.py — format_date, build_candidate, build_candidates."""

import pytest

from rule_engine import format_date, build_candidate, build_candidates
from src.constants.failure_reasons import FailureReason


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _component(
    field: str,
    start=None,
    end=None,
    transform=None,
    date_format=None,
) -> dict:
    return {
        "field": field,
        "slice": {"start": start, "end": end} if (start is not None or end is not None) else None,
        "transform": transform,
        "date_format": date_format,
    }


def _rule(components, separator="", ambiguous=False, fallback_candidates=None) -> dict:
    return {
        "components": components,
        "separator": separator,
        "ambiguous": ambiguous,
        "confidence": "high",
        "reasoning": "test",
        "requires_static_password": False,
        "fallback_candidates": fallback_candidates or [],
    }


def _variant(components, separator="") -> dict:
    """Build a PasswordRuleVariant-compatible dict (components + separator only)."""
    return {"components": components, "separator": separator}


# ---------------------------------------------------------------------------
# format_date
# ---------------------------------------------------------------------------

class TestFormatDate:
    def test_ddmm(self):
        assert format_date("1990-01-15", "DDMM") == "1501"

    def test_ddmmyyyy(self):
        assert format_date("1990-01-15", "DDMMYYYY") == "15011990"

    def test_mmdd(self):
        assert format_date("1990-01-15", "MMDD") == "0115"

    def test_ddmmyy(self):
        assert format_date("1990-01-15", "DDMMYY") == "150190"

    def test_mmddyyyy(self):
        assert format_date("1990-01-15", "MMDDYYYY") == "01151990"

    def test_yyyy(self):
        assert format_date("1985-03-22", "YYYY") == "1985"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Unsupported date_format"):
            format_date("1990-01-15", "YYYYMMDD")

    def test_invalid_dob_raises(self):
        with pytest.raises(ValueError, match="Invalid DOB"):
            format_date("15-01-1990", "DDMM")


# ---------------------------------------------------------------------------
# build_candidate
# ---------------------------------------------------------------------------

class TestBuildCandidate:
    def test_simple_name_slice(self):
        rule = _rule([_component("first_name", 0, 4)])
        assert build_candidate(rule, {"first_name": "Suresh"}) == "Sure"

    def test_slice_clamps_to_length(self):
        rule = _rule([_component("first_name", 0, 10)])
        assert build_candidate(rule, {"first_name": "Ali"}) == "Ali"

    def test_transform_upper(self):
        rule = _rule([_component("first_name", 0, 4, transform="upper")])
        assert build_candidate(rule, {"first_name": "suresh"}) == "SURE"

    def test_transform_lower(self):
        rule = _rule([_component("first_name", 0, 4, transform="lower")])
        assert build_candidate(rule, {"first_name": "SURESH"}) == "sure"

    def test_null_transform_no_op(self):
        rule = _rule([_component("first_name", 0, 4, transform=None)])
        assert build_candidate(rule, {"first_name": "Suresh"}) == "Sure"

    def test_dob_formatted_ddmm(self):
        rule = _rule([_component("dob", date_format="DDMM")])
        assert build_candidate(rule, {"dob": "1990-01-15"}) == "1501"

    def test_multi_component_empty_sep(self):
        rule = _rule([
            _component("first_name", 0, 4),
            _component("dob", date_format="DDMM"),
        ], separator="")
        assert build_candidate(rule, {"first_name": "Suresh", "dob": "1990-01-15"}) == "Sure1501"

    def test_multi_component_dash_sep(self):
        rule = _rule([
            _component("first_name", 0, 4),
            _component("dob", date_format="DDMM"),
        ], separator="-")
        assert build_candidate(rule, {"first_name": "Suresh", "dob": "1990-01-15"}) == "Sure-1501"

    def test_missing_user_field_raises(self):
        rule = _rule([_component("mobile", -4, None)])
        with pytest.raises(ValueError, match=FailureReason.REQUIRED_USER_DATA_MISSING.value):
            build_candidate(rule, {"first_name": "Suresh"})

    def test_name_normalization_strips_periods_and_spaces(self):
        """
        Axis Bank: name "C.K. Ajay Kumar" → strip periods+spaces → "CKAjayKumar"
        → first 4 upper → "CKAJ".
        """
        rule = _rule([_component("first_name", 0, 4, transform="upper")])
        assert build_candidate(rule, {"first_name": "C.K. Ajay Kumar"}) == "CKAJ"

    def test_name_normalization_strips_spaces(self):
        rule = _rule([_component("first_name", 0, 4)])
        assert build_candidate(rule, {"first_name": "A B C D"}) == "ABCD"

    def test_birth_year_via_yyyy_date_format(self):
        rule = _rule([_component("dob", date_format="YYYY")])
        assert build_candidate(rule, {"dob": "1985-03-22"}) == "1985"

    def test_name_plus_birth_year(self):
        rule = _rule([
            _component("first_name", 0, 4),
            _component("dob", date_format="YYYY"),
        ], separator="")
        result = build_candidate(rule, {"first_name": "John", "dob": "1985-03-22"})
        assert result == "John1985"

    def test_mobile_last_4(self):
        rule = _rule([_component("mobile", -4, None)])
        assert build_candidate(rule, {"mobile": "9876543210"}) == "3210"


# ---------------------------------------------------------------------------
# build_candidates
# ---------------------------------------------------------------------------

class TestBuildCandidates:
    def test_unambiguous_returns_single(self):
        rule = _rule([_component("first_name", 0, 4)])
        result = build_candidates(rule, {"first_name": "Suresh"})
        assert len(result) == 1
        assert result[0] == "Sure"

    def test_ambiguous_returns_multiple(self):
        variant1 = _variant([
            _component("first_name", 0, 4, transform="upper"),
            _component("dob", date_format="DDMM"),
        ])
        variant2 = _variant([
            _component("first_name", 0, 4, transform="upper"),
            _component("card_masked", -4, None),
        ])
        rule = _rule(
            components=variant1["components"],
            ambiguous=True,
            fallback_candidates=[variant1, variant2],
        )
        user = {"first_name": "C.K. Ajay Kumar", "dob": "1985-02-11", "card_masked": "009001234"}
        result = build_candidates(rule, user)
        assert len(result) == 2

    def test_candidate_values_correct(self):
        """Axis Bank example: CKAJ1102 and CKAJ1234."""
        variant1 = _variant([
            _component("first_name", 0, 4, transform="upper"),
            _component("dob", date_format="DDMM"),
        ])
        variant2 = _variant([
            _component("first_name", 0, 4, transform="upper"),
            _component("card_masked", -4, None),
        ])
        rule = _rule(
            components=variant1["components"],
            ambiguous=True,
            fallback_candidates=[variant1, variant2],
        )
        user = {
            "first_name": "C.K. Ajay Kumar",
            "dob": "1985-02-11",
            "card_masked": "009001234",
        }
        result = build_candidates(rule, user)
        assert result[0] == "CKAJ1102"
        assert result[1] == "CKAJ1234"

    def test_required_data_missing_propagates(self):
        rule = _rule([_component("mobile", -4, None)])
        with pytest.raises(ValueError, match=FailureReason.REQUIRED_USER_DATA_MISSING.value):
            build_candidates(rule, {"first_name": "Suresh"})
