"""Tests for persistence.py — SQLite persistence layer."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import persistence
from persistence import (
    _get_connection,
    email_exists,
    ensure_user,
    get_last_fetched_date,
    init_db,
    insert_attempt,
    insert_document,
    insert_email,
    record_email_result,
    update_document_status,
    update_email_status,
    update_last_fetched_date,
    update_user_fields,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_USER_DATA = {
    "name": "John Doe",
    "dob": "1990-01-15",
    "mobile": "9876543210",
    "pan": "ABCDE1234F",
    "card_masked": "12345678",
    "account_masked": "87654321",
}


def _make_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "test.db")
    init_db(db_path)
    return db_path


def _seed_user(db_path: str, email: str = "user@example.com") -> int:
    with patch.dict("os.environ", {"EMAIL_USERNAME": email}):
        return ensure_user(_USER_DATA, db_path)


def _seed_email(
    db_path: str,
    user_id: int,
    uid: str = "101",
    status: str = "SUCCESS",
    failure_reason: str | None = None,
) -> int:
    return insert_email(
        user_id=user_id,
        uid=uid,
        message_id=f"<{uid}@example.com>",
        sender="billing@bank.com",
        subject="Your Statement",
        received_at=None,
        status=status,
        failure_reason=failure_reason,
        db_path=db_path,
    )


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


class TestInitDb:
    def test_creates_db_file(self, tmp_path):
        db_path = str(tmp_path / "sub" / "bill.db")
        init_db(db_path)
        assert Path(db_path).exists()

    def test_all_tables_exist(self, tmp_path):
        db_path = _make_db(tmp_path)
        conn = _get_connection(db_path)
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        finally:
            conn.close()
        assert {"user", "email", "document", "decryption_attempt", "pipeline_state"}.issubset(
            tables
        )

    def test_idempotent_on_second_call(self, tmp_path):
        db_path = _make_db(tmp_path)
        # Should not raise
        init_db(db_path)


# ---------------------------------------------------------------------------
# ensure_user
# ---------------------------------------------------------------------------


class TestEnsureUser:
    def test_inserts_user_and_returns_id(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        assert isinstance(user_id, int)
        assert user_id > 0

    def test_idempotent_returns_same_id(self, tmp_path):
        db_path = _make_db(tmp_path)
        id1 = _seed_user(db_path)
        id2 = _seed_user(db_path)
        assert id1 == id2

    def test_stores_email_from_env(self, tmp_path):
        db_path = _make_db(tmp_path)
        _seed_user(db_path, email="test@example.com")
        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT email FROM user").fetchone()
        finally:
            conn.close()
        assert row["email"] == "test@example.com"

    def test_raises_if_email_env_missing(self, tmp_path):
        db_path = _make_db(tmp_path)
        with patch.dict("os.environ", {}, clear=True), \
             patch("persistence.load_dotenv"):
            with pytest.raises(RuntimeError, match="EMAIL_USERNAME"):
                ensure_user(_USER_DATA, db_path)

    def test_stores_user_fields(self, tmp_path):
        db_path = _make_db(tmp_path)
        _seed_user(db_path)
        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT * FROM user").fetchone()
        finally:
            conn.close()
        assert row["name"] == "John Doe"
        assert row["dob"] == "1990-01-15"
        assert row["mobile"] == "9876543210"


# ---------------------------------------------------------------------------
# email_exists
# ---------------------------------------------------------------------------


class TestEmailExists:
    def test_returns_none_when_not_found(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        result = email_exists("uid999", "<missing@x.com>", user_id, db_path)
        assert result is None

    def test_finds_by_uid(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        _seed_email(db_path, user_id, uid="101")
        result = email_exists("101", "", user_id, db_path)
        assert result is not None
        assert result["status"] == "SUCCESS"

    def test_finds_by_message_id(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        _seed_email(db_path, user_id, uid="101")
        result = email_exists("UID_UNKNOWN", "<101@example.com>", user_id, db_path)
        assert result is not None
        assert result["status"] == "SUCCESS"

    def test_empty_message_id_does_not_match_other_nulls(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        _seed_email(db_path, user_id, uid="101")
        # Should not match uid=999 via empty message_id
        result = email_exists("999", "", user_id, db_path)
        assert result is None

    def test_returns_status_and_failure_reason(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        _seed_email(
            db_path, user_id, uid="101",
            status="FAILURE_RETRYABLE",
            failure_reason="CANDIDATE_LIST_EXHAUSTED",
        )
        result = email_exists("101", "", user_id, db_path)
        assert result["status"] == "FAILURE_RETRYABLE"
        assert result["failure_reason"] == "CANDIDATE_LIST_EXHAUSTED"


# ---------------------------------------------------------------------------
# insert_email / update_email_status
# ---------------------------------------------------------------------------


class TestEmailCrud:
    def test_insert_email_returns_id(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        email_id = _seed_email(db_path, user_id)
        assert isinstance(email_id, int)
        assert email_id > 0

    def test_insert_email_failure_requires_reason(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        with pytest.raises(ValueError, match="failure_reason must be set"):
            insert_email(
                user_id=user_id, uid="101", message_id=None,
                sender=None, subject=None, received_at=None,
                status="FAILURE_TERMINAL", failure_reason=None,
                db_path=db_path,
            )

    def test_insert_email_success_forbids_reason(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        with pytest.raises(ValueError, match="failure_reason must be NULL"):
            insert_email(
                user_id=user_id, uid="101", message_id=None,
                sender=None, subject=None, received_at=None,
                status="SUCCESS", failure_reason="SOME_REASON",
                db_path=db_path,
            )

    def test_update_email_status(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        email_id = _seed_email(
            db_path, user_id, status="FAILURE_RETRYABLE",
            failure_reason="CANDIDATE_LIST_EXHAUSTED",
        )
        update_email_status(email_id, "SUCCESS", db_path=db_path)
        conn = _get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT status, failure_reason FROM email WHERE id = ?", (email_id,)
            ).fetchone()
        finally:
            conn.close()
        assert row["status"] == "SUCCESS"
        assert row["failure_reason"] is None


# ---------------------------------------------------------------------------
# insert_document / update_document_status
# ---------------------------------------------------------------------------


class TestDocumentCrud:
    def test_insert_document_returns_id(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        email_id = _seed_email(db_path, user_id)
        doc_id = insert_document(
            user_id=user_id, email_id=email_id,
            filename="statement.pdf", is_encrypted=1,
            status="DECRYPTED", output_path="/out/statement.pdf",
            db_path=db_path,
        )
        assert doc_id > 0

    def test_update_document_status(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        email_id = _seed_email(db_path, user_id)
        doc_id = insert_document(
            user_id=user_id, email_id=email_id,
            filename="bill.pdf", is_encrypted=1,
            status="FAILED", db_path=db_path,
        )
        update_document_status(doc_id, "DECRYPTED", "/out/bill.pdf", db_path=db_path)
        conn = _get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT status, output_path FROM document WHERE id = ?", (doc_id,)
            ).fetchone()
        finally:
            conn.close()
        assert row["status"] == "DECRYPTED"
        assert row["output_path"] == "/out/bill.pdf"


# ---------------------------------------------------------------------------
# insert_attempt
# ---------------------------------------------------------------------------


class TestAttemptCrud:
    def test_insert_attempt_returns_id(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        email_id = _seed_email(db_path, user_id)
        doc_id = insert_document(
            user_id=user_id, email_id=email_id,
            filename="bill.pdf", is_encrypted=1, status="DECRYPTED",
            db_path=db_path,
        )
        attempt_id = insert_attempt(
            user_id=user_id, document_id=doc_id,
            attempt_number=1, outcome="SUCCESS", db_path=db_path,
        )
        assert attempt_id > 0

    def test_failed_attempt_stores_failure_reason(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        email_id = _seed_email(db_path, user_id)
        doc_id = insert_document(
            user_id=user_id, email_id=email_id,
            filename="bill.pdf", is_encrypted=1, status="FAILED",
            db_path=db_path,
        )
        attempt_id = insert_attempt(
            user_id=user_id, document_id=doc_id,
            attempt_number=1, outcome="FAILED",
            failure_reason="CANDIDATE_LIST_EXHAUSTED",
            db_path=db_path,
        )
        conn = _get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT outcome, failure_reason FROM decryption_attempt WHERE id = ?",
                (attempt_id,),
            ).fetchone()
        finally:
            conn.close()
        assert row["outcome"] == "FAILED"
        assert row["failure_reason"] == "CANDIDATE_LIST_EXHAUSTED"


# ---------------------------------------------------------------------------
# pipeline_state round-trip
# ---------------------------------------------------------------------------


class TestPipelineState:
    def test_get_returns_none_when_no_row(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        result = get_last_fetched_date(user_id, db_path)
        assert result is None

    def test_update_and_get_round_trip(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        ts = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        update_last_fetched_date(user_id, ts, db_path)
        result = get_last_fetched_date(user_id, db_path)
        assert result is not None
        assert result.tzinfo is not None
        assert result.replace(microsecond=0) == ts

    def test_update_is_upsert(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        ts1 = datetime(2026, 3, 1, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 20, tzinfo=timezone.utc)
        update_last_fetched_date(user_id, ts1, db_path)
        update_last_fetched_date(user_id, ts2, db_path)
        result = get_last_fetched_date(user_id, db_path)
        assert result.replace(microsecond=0) == ts2

    def test_naive_timestamp_treated_as_utc(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        naive_ts = datetime(2026, 3, 20, 12, 0)
        update_last_fetched_date(user_id, naive_ts, db_path)
        result = get_last_fetched_date(user_id, db_path)
        assert result.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# record_email_result
# ---------------------------------------------------------------------------


def _make_pdf_result(
    filename: str = "bill.pdf",
    status: str = "success",
    output_path: str | None = "/out/bill.pdf",
    failure_reason: str | None = None,
    candidates_tried: int = 1,
):
    from orchestrator import PdfResult
    return PdfResult(
        filename=filename,
        status=status,
        output_path=output_path,
        failure_reason=failure_reason,
        candidates_tried=candidates_tried,
    )


def _make_email_result(
    uid: str = "101",
    status: str = "success",
    failure_reason: str | None = None,
    pdf_results: list | None = None,
):
    from orchestrator import EmailResult
    return EmailResult(
        uid=uid,
        sender="billing@bank.com",
        subject="Statement",
        status=status,
        failure_reason=failure_reason,
        explanation="ok",
        pdf_results=pdf_results or [],
    )


_EMAIL_DATA = {
    "uid": "101",
    "message_id": "<101@example.com>",
    "sender": "billing@bank.com",
    "subject": "Statement",
}


class TestRecordEmailResult:
    def test_success_inserts_email_row(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        pdf = _make_pdf_result()
        result = _make_email_result(pdf_results=[pdf])

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT status FROM email WHERE uid = '101'").fetchone()
        finally:
            conn.close()
        assert row["status"] == "SUCCESS"

    def test_failure_inserts_correct_status(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        result = _make_email_result(
            status="failure",
            failure_reason="NO_PDF_ATTACHMENT",
        )

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT status, failure_reason FROM email").fetchone()
        finally:
            conn.close()
        assert row["status"] == "FAILURE_TERMINAL"
        assert row["failure_reason"] == "NO_PDF_ATTACHMENT"

    def test_retryable_failure_stored_correctly(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        result = _make_email_result(
            status="failure",
            failure_reason="CANDIDATE_LIST_EXHAUSTED",
        )

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT status FROM email").fetchone()
        finally:
            conn.close()
        assert row["status"] == "FAILURE_RETRYABLE"

    def test_inserts_document_row_per_pdf(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        pdfs = [
            _make_pdf_result("a.pdf"),
            _make_pdf_result("b.pdf"),
        ]
        result = _make_email_result(pdf_results=pdfs)

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
        finally:
            conn.close()
        assert count == 2

    def test_inserts_attempt_row_per_pdf(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        pdfs = [
            _make_pdf_result("a.pdf"),
            _make_pdf_result("b.pdf"),
        ]
        result = _make_email_result(pdf_results=pdfs)

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM decryption_attempt"
            ).fetchone()[0]
            attempt_numbers = [
                row[0]
                for row in conn.execute(
                    "SELECT attempt_number FROM decryption_attempt"
                ).fetchall()
            ]
        finally:
            conn.close()
        assert count == 2
        assert all(n == 1 for n in attempt_numbers)

    def test_failed_pdf_inserts_failed_attempt(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        pdf = _make_pdf_result(
            status="failure",
            output_path=None,
            failure_reason="CANDIDATE_LIST_EXHAUSTED",
        )
        result = _make_email_result(
            status="failure",
            failure_reason="CANDIDATE_LIST_EXHAUSTED",
            pdf_results=[pdf],
        )

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            attempt = conn.execute(
                "SELECT outcome, failure_reason FROM decryption_attempt"
            ).fetchone()
            doc = conn.execute("SELECT status FROM document").fetchone()
        finally:
            conn.close()
        assert attempt["outcome"] == "FAILED"
        assert attempt["failure_reason"] == "CANDIDATE_LIST_EXHAUSTED"
        assert doc["status"] == "FAILED"

    def test_no_pdf_results_still_inserts_email_row(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        result = _make_email_result(
            status="failure",
            failure_reason="NO_PDF_ATTACHMENT",
            pdf_results=[],
        )

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            email_count = conn.execute("SELECT COUNT(*) FROM email").fetchone()[0]
            doc_count = conn.execute("SELECT COUNT(*) FROM document").fetchone()[0]
        finally:
            conn.close()
        assert email_count == 1
        assert doc_count == 0

# ---------------------------------------------------------------------------
# update_user_fields
# ---------------------------------------------------------------------------


class TestUpdateUserFields:
    def test_updates_allowed_columns(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        update_user_fields(user_id, {"mobile": "1234567890", "pan": "ZZZZZ9999Z"}, db_path)
        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT mobile, pan FROM user WHERE id = ?", (user_id,)).fetchone()
        finally:
            conn.close()
        assert row["mobile"] == "1234567890"
        assert row["pan"] == "ZZZZZ9999Z"

    def test_ignores_unknown_keys(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        # customer_id is not an allowed column — should be silently dropped
        update_user_fields(user_id, {"mobile": "0000000000", "customer_id": "abc"}, db_path)
        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT mobile FROM user WHERE id = ?", (user_id,)).fetchone()
        finally:
            conn.close()
        assert row["mobile"] == "0000000000"

    def test_empty_dict_is_noop(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        # Should not raise and original values unchanged
        update_user_fields(user_id, {}, db_path)
        conn = _get_connection(db_path)
        try:
            row = conn.execute("SELECT name FROM user WHERE id = ?", (user_id,)).fetchone()
        finally:
            conn.close()
        assert row["name"] == _USER_DATA["name"]


class TestRecordEmailResultEncryption:
    def test_pdf_not_encrypted_is_not_encrypted_0(self, tmp_path):
        db_path = _make_db(tmp_path)
        user_id = _seed_user(db_path)
        pdf = _make_pdf_result(
            status="failure",
            output_path=None,
            failure_reason="PDF_NOT_ENCRYPTED",
        )
        result = _make_email_result(
            status="failure",
            failure_reason="PDF_NOT_ENCRYPTED",
            pdf_results=[pdf],
        )

        record_email_result(user_id, _EMAIL_DATA, result, db_path)

        conn = _get_connection(db_path)
        try:
            doc = conn.execute("SELECT is_encrypted FROM document").fetchone()
        finally:
            conn.close()
        assert doc["is_encrypted"] == 0
