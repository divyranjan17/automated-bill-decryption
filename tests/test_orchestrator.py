"""Tests for orchestrator.py — end-to-end pipeline logic."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import orchestrator
from orchestrator import (
    EmailResult,
    PdfResult,
    _aggregate_results,
    _extract_sender_domain,
    _is_terminal,
    _process_single_email,
    _resolve_email_status,
    _resolve_output_path,
    run_pipeline,
)
from src.constants.failure_reasons import FailureReason


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

USER = {
    "name": "John Doe",
    "dob": "1990-01-15",  # ISO format — as passed to _process_single_email after normalization
    "mobile": "9876543210",
    "pan": "ABCDE1234F",
    "card_masked": "12345678",
    "account_masked": "87654321",
}

# Profile as stored in user_profile.json — dob in DD-MM-YYYY before run_pipeline normalizes it
_PROFILE_DATA = {**USER, "dob": "15-01-1990"}

_PLAIN_EMAIL: dict = {
    "uid": "101",
    "message_id": "<abc@example.com>",
    "sender": "billing@hdfcbank.net",
    "subject": "Statement",
    "body_text": "Password is first 4 letters of your name followed by birth year.",
    "pdf_attachments": [b"%PDF-encrypted"],
    "pdf_filenames": ["statement.pdf"],
}


def _email(**overrides) -> dict:
    data = dict(_PLAIN_EMAIL)
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# _process_single_email — failure paths
# ---------------------------------------------------------------------------


def test_no_pdf_attachment_returns_failure():
    result = _process_single_email(
        _email(pdf_attachments=[], pdf_filenames=[]), USER, "output"
    )
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.NO_PDF_ATTACHMENT.value


def test_no_password_hint_returns_failure():
    with patch.object(orchestrator, "extract_password_hint", return_value=None):
        result = _process_single_email(_email(), USER, "output")
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.NO_PASSWORD_HINT_FOUND.value


def test_unparsable_hint_returns_failure():
    with patch.object(orchestrator, "extract_password_hint", return_value="some hint"), \
         patch.object(
             orchestrator,
             "interpret_instruction",
             side_effect=ValueError(FailureReason.HINT_FOUND_BUT_UNPARSABLE.value),
         ):
        result = _process_single_email(_email(), USER, "output")
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.HINT_FOUND_BUT_UNPARSABLE.value


def test_requires_static_password_returns_failure():
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": True,
        "fallback_candidates": [],
    }
    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule):
        result = _process_single_email(_email(), USER, "output")
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.REQUIRES_STATIC_PASSWORD.value


def test_required_user_data_missing_returns_failure():
    """Error without colon → else branch → immediate failure (no escalation)."""
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(
             orchestrator,
             "build_candidates",
             side_effect=ValueError(FailureReason.REQUIRED_USER_DATA_MISSING.value),
         ):
        result = _process_single_email(_email(), USER, "output")
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.REQUIRED_USER_DATA_MISSING.value


def test_missing_user_data_escalates_and_succeeds(tmp_path):
    """Fields-in-error path: prompt fills data, retry succeeds → pipeline continues."""
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    decrypt_result = {
        "status": "success",
        "output_path": str(tmp_path / "statement.pdf"),
        "attempts": 1,
    }
    updated_user = {**USER, "pan": "XYZAB9876G"}
    missing_error = ValueError(
        f"{FailureReason.REQUIRED_USER_DATA_MISSING.value}:pan"
    )

    build_side_effects = [missing_error, ["XYZAB9876G"]]

    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(
             orchestrator, "build_candidates", side_effect=build_side_effects
         ), \
         patch.object(
             orchestrator, "prompt_missing_fields", return_value=updated_user
         ), \
         patch.object(orchestrator, "update_user_fields"), \
         patch.object(orchestrator, "is_encrypted", return_value=True), \
         patch.object(orchestrator, "decrypt_pdf", return_value=decrypt_result):
        result = _process_single_email(_email(), USER, str(tmp_path))

    assert result.status == "success"


def test_missing_user_data_escalates_retry_fails():
    """Retry build_candidates also raises → returns REQUIRED_USER_DATA_MISSING."""
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    missing_error = ValueError(
        f"{FailureReason.REQUIRED_USER_DATA_MISSING.value}:pan"
    )
    updated_user = {**USER, "pan": ""}

    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(
             orchestrator,
             "build_candidates",
             side_effect=[missing_error, ValueError("still missing")],
         ), \
         patch.object(
             orchestrator, "prompt_missing_fields", return_value=updated_user
         ), \
         patch.object(orchestrator, "update_user_fields"):
        result = _process_single_email(_email(), USER, "output")

    assert result.status == "failure"
    assert result.failure_reason == FailureReason.REQUIRED_USER_DATA_MISSING.value
    assert "still missing" in result.explanation


def test_missing_user_data_no_fields_in_error_returns_failure():
    """Error has no colon (old format) → no escalation, returns failure directly."""
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(
             orchestrator,
             "build_candidates",
             side_effect=ValueError(FailureReason.REQUIRED_USER_DATA_MISSING.value),
         ), \
         patch.object(
             orchestrator, "prompt_missing_fields"
         ) as mock_prompt:
        result = _process_single_email(_email(), USER, "output")

    mock_prompt.assert_not_called()
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.REQUIRED_USER_DATA_MISSING.value


# ---------------------------------------------------------------------------
# _process_single_email — success path
# ---------------------------------------------------------------------------


def test_success_path_returns_success(tmp_path):
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    decrypt_result = {
        "status": "success",
        "output_path": str(tmp_path / "statement.pdf"),
        "attempts": 1,
    }
    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(orchestrator, "build_candidates", return_value=["JOHN1990"]), \
         patch.object(orchestrator, "is_encrypted", return_value=True), \
         patch.object(orchestrator, "decrypt_pdf", return_value=decrypt_result):
        result = _process_single_email(_email(), USER, str(tmp_path))
    assert result.status == "success"
    assert result.failure_reason is None
    assert len(result.pdf_results) == 1
    assert result.pdf_results[0].status == "success"
    assert result.pdf_results[0].candidates_tried == 1


def test_all_candidates_exhausted_returns_failure(tmp_path):
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    decrypt_result = {
        "status": "failure",
        "failure_reason": FailureReason.WRONG_PASSWORD.value,
        "attempts": 1,
    }
    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(
             orchestrator, "build_candidates", return_value=["JOHN1990", "john1990"]
         ), \
         patch.object(orchestrator, "is_encrypted", return_value=True), \
         patch.object(orchestrator, "decrypt_pdf", return_value=decrypt_result):
        result = _process_single_email(_email(), USER, str(tmp_path))
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.CANDIDATE_LIST_EXHAUSTED.value
    assert result.pdf_results[0].candidates_tried == 2


def test_unencrypted_pdf_returns_pdf_not_encrypted(tmp_path):
    rule = {
        "components": [],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    with patch.object(orchestrator, "extract_password_hint", return_value="hint"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(orchestrator, "build_candidates", return_value=["JOHN1990"]), \
         patch.object(orchestrator, "is_encrypted", return_value=False):
        result = _process_single_email(_email(), USER, str(tmp_path))
    assert result.status == "failure"
    assert result.pdf_results[0].failure_reason == FailureReason.PDF_NOT_ENCRYPTED.value
    assert result.pdf_results[0].candidates_tried == 0


# ---------------------------------------------------------------------------
# _resolve_output_path — dedup logic
# ---------------------------------------------------------------------------


def test_resolve_output_path_returns_base_path_when_not_exists(tmp_path):
    path = _resolve_output_path(str(tmp_path), "billing@hdfcbank.net", "statement.pdf")
    assert path == str(tmp_path / "hdfcbank.net" / "statement.pdf")


def test_resolve_output_path_deduplicates_with_counter(tmp_path):
    domain_dir = tmp_path / "hdfcbank.net"
    domain_dir.mkdir(parents=True)
    (domain_dir / "statement.pdf").write_bytes(b"existing")

    path = _resolve_output_path(str(tmp_path), "billing@hdfcbank.net", "statement.pdf")
    assert path == str(tmp_path / "hdfcbank.net" / "statement_1.pdf")


def test_resolve_output_path_increments_counter_past_existing(tmp_path):
    domain_dir = tmp_path / "hdfcbank.net"
    domain_dir.mkdir(parents=True)
    (domain_dir / "statement.pdf").write_bytes(b"existing")
    (domain_dir / "statement_1.pdf").write_bytes(b"existing")

    path = _resolve_output_path(str(tmp_path), "billing@hdfcbank.net", "statement.pdf")
    assert path == str(tmp_path / "hdfcbank.net" / "statement_2.pdf")


# ---------------------------------------------------------------------------
# _extract_sender_domain
# ---------------------------------------------------------------------------


def test_extract_sender_domain_returns_domain_portion():
    assert _extract_sender_domain("billing@hdfcbank.net") == "hdfcbank.net"


def test_extract_sender_domain_returns_sender_when_no_at_sign():
    assert _extract_sender_domain("hdfcbank.net") == "hdfcbank.net"


# ---------------------------------------------------------------------------
# _is_terminal
# ---------------------------------------------------------------------------


def test_is_terminal_true_for_success():
    result = EmailResult(
        uid="1", sender="s", subject="sub", status="success",
        failure_reason=None, explanation="ok",
    )
    assert _is_terminal(result) is True


@pytest.mark.parametrize(
    "failure_reason",
    [
        FailureReason.NOT_A_BILL_EMAIL.value,
        FailureReason.NO_PDF_ATTACHMENT.value,
        FailureReason.PDF_NOT_ENCRYPTED.value,
        FailureReason.INVALID_RULE.value,
        FailureReason.REQUIRES_STATIC_PASSWORD.value,
    ],
)
def test_is_terminal_true_for_terminal_failures(failure_reason):
    result = EmailResult(
        uid="1", sender="s", subject="sub", status="failure",
        failure_reason=failure_reason, explanation="",
    )
    assert _is_terminal(result) is True


@pytest.mark.parametrize(
    "failure_reason",
    [
        FailureReason.CANDIDATE_LIST_EXHAUSTED.value,
        FailureReason.REQUIRED_USER_DATA_MISSING.value,
        FailureReason.NO_PASSWORD_HINT_FOUND.value,
        FailureReason.HINT_FOUND_BUT_UNPARSABLE.value,
    ],
)
def test_is_terminal_false_for_retryable_failures(failure_reason):
    result = EmailResult(
        uid="1", sender="s", subject="sub", status="failure",
        failure_reason=failure_reason, explanation="",
    )
    assert _is_terminal(result) is False


# ---------------------------------------------------------------------------
# run_pipeline — multi-batch loop + terminal labeling
# ---------------------------------------------------------------------------


def test_run_pipeline_terminates_when_fetch_returns_empty(tmp_path):
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_PROFILE_DATA), encoding="utf-8")

    with patch.object(orchestrator, "init_db"), \
         patch.object(orchestrator, "ensure_user", return_value=1), \
         patch.object(orchestrator, "get_last_fetched_date", return_value=None), \
         patch.object(orchestrator, "fetch_emails", return_value=[]) as mock_fetch, \
         patch.object(orchestrator, "update_last_fetched_date") as mock_update:
        results = run_pipeline(
            output_dir=str(tmp_path / "output"),
            profile_path=str(profile_path),
        )

    mock_fetch.assert_called_once()
    mock_update.assert_called_once()
    assert results == []


def test_run_pipeline_processes_multiple_batches(tmp_path):
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_PROFILE_DATA), encoding="utf-8")

    email_a = _email(uid="101")
    email_b = _email(uid="202")

    # fetch_emails: batch 1, batch 2, then empty
    fetch_side_effect = [[email_a], [email_b], []]

    terminal_result = EmailResult(
        uid="x", sender="s", subject="sub", status="failure",
        failure_reason=FailureReason.NO_PDF_ATTACHMENT.value,
        explanation="no pdf",
    )

    with patch.object(orchestrator, "init_db"), \
         patch.object(orchestrator, "ensure_user", return_value=1), \
         patch.object(orchestrator, "get_last_fetched_date", return_value=None), \
         patch.object(orchestrator, "email_exists_in_db", return_value=None), \
         patch.object(orchestrator, "record_email_result"), \
         patch.object(orchestrator, "update_last_fetched_date"), \
         patch.object(
             orchestrator, "fetch_emails", side_effect=fetch_side_effect
         ), \
         patch.object(
             orchestrator,
             "_process_single_email",
             return_value=terminal_result,
         ) as mock_process, \
         patch.object(orchestrator, "mark_email_processed") as mock_label:
        results = run_pipeline(
            output_dir=str(tmp_path / "output"),
            profile_path=str(profile_path),
        )

    assert mock_process.call_count == 2
    # Both results are terminal → both should be labeled
    assert mock_label.call_count == 2


def test_run_pipeline_labels_only_terminal_results(tmp_path):
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_PROFILE_DATA), encoding="utf-8")

    terminal_result = EmailResult(
        uid="101", sender="s", subject="sub", status="failure",
        failure_reason=FailureReason.NO_PDF_ATTACHMENT.value,
        explanation="terminal",
    )
    retryable_result = EmailResult(
        uid="202", sender="s", subject="sub", status="failure",
        failure_reason=FailureReason.CANDIDATE_LIST_EXHAUSTED.value,
        explanation="retryable",
    )

    email_a = _email(uid="101")
    email_b = _email(uid="202")

    with patch.object(orchestrator, "init_db"), \
         patch.object(orchestrator, "ensure_user", return_value=1), \
         patch.object(orchestrator, "get_last_fetched_date", return_value=None), \
         patch.object(orchestrator, "email_exists_in_db", return_value=None), \
         patch.object(orchestrator, "record_email_result"), \
         patch.object(orchestrator, "update_last_fetched_date"), \
         patch.object(
             orchestrator, "fetch_emails", side_effect=[[email_a, email_b], []]
         ), \
         patch.object(
             orchestrator,
             "_process_single_email",
             side_effect=[terminal_result, retryable_result],
         ), \
         patch.object(orchestrator, "mark_email_processed") as mock_label:
        run_pipeline(
            output_dir=str(tmp_path / "output"),
            profile_path=str(profile_path),
        )

    # Only the terminal result (uid=101) should be labeled
    mock_label.assert_called_once_with("101")


# ---------------------------------------------------------------------------
# _resolve_email_status
# ---------------------------------------------------------------------------


def test_resolve_email_status_success():
    result = EmailResult(
        uid="1", sender="s", subject="sub", status="success",
        failure_reason=None, explanation="ok",
    )
    assert _resolve_email_status(result) == "SUCCESS"


@pytest.mark.parametrize(
    "failure_reason",
    [
        FailureReason.NOT_A_BILL_EMAIL.value,
        FailureReason.NO_PDF_ATTACHMENT.value,
        FailureReason.PDF_NOT_ENCRYPTED.value,
        FailureReason.INVALID_RULE.value,
        FailureReason.REQUIRES_STATIC_PASSWORD.value,
    ],
)
def test_resolve_email_status_terminal(failure_reason):
    result = EmailResult(
        uid="1", sender="s", subject="sub", status="failure",
        failure_reason=failure_reason, explanation="",
    )
    assert _resolve_email_status(result) == "FAILURE_TERMINAL"


@pytest.mark.parametrize(
    "failure_reason",
    [
        FailureReason.CANDIDATE_LIST_EXHAUSTED.value,
        FailureReason.REQUIRED_USER_DATA_MISSING.value,
        FailureReason.NO_PASSWORD_HINT_FOUND.value,
        FailureReason.HINT_FOUND_BUT_UNPARSABLE.value,
    ],
)
def test_resolve_email_status_retryable(failure_reason):
    result = EmailResult(
        uid="1", sender="s", subject="sub", status="failure",
        failure_reason=failure_reason, explanation="",
    )
    assert _resolve_email_status(result) == "FAILURE_RETRYABLE"


def test_run_pipeline_skips_already_processed_email(tmp_path):
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_PROFILE_DATA), encoding="utf-8")

    email_a = _email(uid="101")

    with patch.object(orchestrator, "init_db"), \
         patch.object(orchestrator, "ensure_user", return_value=1), \
         patch.object(orchestrator, "get_last_fetched_date", return_value=None), \
         patch.object(
             orchestrator,
             "email_exists_in_db",
             return_value={"id": 5, "status": "SUCCESS", "failure_reason": None},
         ), \
         patch.object(orchestrator, "record_email_result") as mock_record, \
         patch.object(orchestrator, "update_last_fetched_date"), \
         patch.object(orchestrator, "fetch_emails", side_effect=[[email_a], []]), \
         patch.object(orchestrator, "_process_single_email") as mock_process:
        run_pipeline(
            output_dir=str(tmp_path / "output"),
            profile_path=str(profile_path),
        )

    mock_process.assert_not_called()
    mock_record.assert_not_called()


# ---------------------------------------------------------------------------
# _aggregate_results
# ---------------------------------------------------------------------------


def test_aggregate_results_success_if_any_pdf_decrypted():
    pdf_results = [
        PdfResult("a.pdf", "failure", None, FailureReason.CANDIDATE_LIST_EXHAUSTED.value, 2),
        PdfResult("b.pdf", "success", "/out/b.pdf", None, 1),
    ]
    result = _aggregate_results("1", "s@example.com", "sub", pdf_results)
    assert result.status == "success"


def test_aggregate_results_failure_if_all_pdfs_failed():
    pdf_results = [
        PdfResult("a.pdf", "failure", None, FailureReason.CANDIDATE_LIST_EXHAUSTED.value, 2),
    ]
    result = _aggregate_results("1", "s@example.com", "sub", pdf_results)
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.CANDIDATE_LIST_EXHAUSTED.value


def test_aggregate_results_empty_pdf_list_returns_failure():
    result = _aggregate_results("1", "s@example.com", "sub", [])
    assert result.status == "failure"
    assert result.failure_reason == FailureReason.CANDIDATE_LIST_EXHAUSTED.value


# ---------------------------------------------------------------------------
# Structured logging — caplog assertions
# ---------------------------------------------------------------------------


def test_happy_path_emits_structured_events_in_order(tmp_path, caplog):
    """Happy path run emits all key structured events and in correct order."""
    import logging
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_PROFILE_DATA), encoding="utf-8")

    uid = "501"
    email = _email(uid=uid)

    rule = {
        "components": [{"field": "name"}],
        "separator": "",
        "ambiguous": False,
        "confidence": "high",
        "reasoning": "",
        "requires_static_password": False,
        "fallback_candidates": [],
    }
    decrypt_result = {
        "status": "success",
        "output_path": str(tmp_path / "statement.pdf"),
        "attempts": 1,
    }

    with caplog.at_level(logging.INFO, logger="orchestrator"), \
         patch.object(orchestrator, "init_db"), \
         patch.object(orchestrator, "ensure_user", return_value=7), \
         patch.object(orchestrator, "get_last_fetched_date", return_value=None), \
         patch.object(orchestrator, "email_exists_in_db", return_value=None), \
         patch.object(orchestrator, "record_email_result"), \
         patch.object(orchestrator, "update_last_fetched_date"), \
         patch.object(orchestrator, "fetch_emails", side_effect=[[email], []]), \
         patch.object(orchestrator, "mark_email_processed"), \
         patch.object(orchestrator, "extract_password_hint", return_value="hint text"), \
         patch.object(orchestrator, "interpret_instruction", return_value=rule), \
         patch.object(orchestrator, "build_candidates", return_value=["JOHN1990"]), \
         patch.object(orchestrator, "is_encrypted", return_value=True), \
         patch.object(orchestrator, "decrypt_pdf", return_value=decrypt_result):
        run_pipeline(
            output_dir=str(tmp_path / "output"),
            profile_path=str(profile_path),
        )

    text = caplog.text
    expected_events = [
        "event=PIPELINE_START",
        "event=EMAIL_START",
        "event=HINT_EXTRACTED",
        "event=RULE_BUILT",
        "event=CANDIDATES_BUILT",
        "event=DECRYPT_ATTEMPT",
        "event=DECRYPT_SUCCESS",
        "event=EMAIL_DONE",
        "event=EMAIL_LABELED",
        "event=PIPELINE_DONE",
    ]
    for event_str in expected_events:
        assert event_str in text, f"Expected '{event_str}' in caplog output"

    # Ordering invariant: EMAIL_DONE must precede EMAIL_LABELED for same uid
    done_marker = f"event=EMAIL_DONE uid={uid}"
    labeled_marker = f"event=EMAIL_LABELED uid={uid}"
    assert done_marker in text, f"Expected '{done_marker}' in caplog output"
    assert labeled_marker in text, f"Expected '{labeled_marker}' in caplog output"
    assert text.index(done_marker) < text.index(labeled_marker), (
        "EMAIL_DONE must appear before EMAIL_LABELED for the same uid"
    )


def test_no_pdf_attachment_emits_structured_events_in_order(tmp_path, caplog):
    """NO_PDF_ATTACHMENT terminal failure emits EMAIL_START, EMAIL_DONE, EMAIL_LABELED in order."""
    import logging
    profile_path = tmp_path / "profile.json"
    profile_path.write_text(json.dumps(_PROFILE_DATA), encoding="utf-8")

    uid = "601"
    email = _email(uid=uid, pdf_attachments=[], pdf_filenames=[])

    with caplog.at_level(logging.INFO, logger="orchestrator"), \
         patch.object(orchestrator, "init_db"), \
         patch.object(orchestrator, "ensure_user", return_value=7), \
         patch.object(orchestrator, "get_last_fetched_date", return_value=None), \
         patch.object(orchestrator, "email_exists_in_db", return_value=None), \
         patch.object(orchestrator, "record_email_result"), \
         patch.object(orchestrator, "update_last_fetched_date"), \
         patch.object(orchestrator, "fetch_emails", side_effect=[[email], []]), \
         patch.object(orchestrator, "mark_email_processed"):
        run_pipeline(
            output_dir=str(tmp_path / "output"),
            profile_path=str(profile_path),
        )

    text = caplog.text

    # All three key events must appear
    assert "event=EMAIL_START" in text
    assert f"failure_reason=NO_PDF_ATTACHMENT" in text
    assert "event=EMAIL_LABELED" in text

    # Ordering invariant: EMAIL_DONE must precede EMAIL_LABELED for same uid
    done_marker = f"event=EMAIL_DONE uid={uid}"
    labeled_marker = f"event=EMAIL_LABELED uid={uid}"
    assert done_marker in text, f"Expected '{done_marker}' in caplog output"
    assert labeled_marker in text, f"Expected '{labeled_marker}' in caplog output"
    assert text.index(done_marker) < text.index(labeled_marker), (
        "EMAIL_DONE must appear before EMAIL_LABELED for the same uid"
    )
