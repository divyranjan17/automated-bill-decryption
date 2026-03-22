import importlib
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class FakeIMAP:
    def __init__(self, search_uids=b"", fetch_map=None):
        self.search_uids = search_uids
        self.fetch_map = fetch_map or {}
        self.uid = MagicMock(side_effect=self._uid)
        self.login = MagicMock()
        self.select = MagicMock(return_value=("OK", [b"1"]))
        self.logout = MagicMock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.logout()
        return False

    def _uid(self, command, *args):
        if command == "SEARCH":
            return "OK", [self.search_uids]
        if command == "FETCH":
            uid = args[0]
            return "OK", [(b"1 (RFC822 {1})", self.fetch_map[uid])]
        if command == "STORE":
            return "OK", [b"stored"]
        raise AssertionError(f"Unexpected IMAP UID command: {command}")


def _build_plain_text_message():
    message = EmailMessage()
    message["Message-ID"] = "<abc123@example.com>"
    message["From"] = "Billing Team <billing@example.com>"
    message["Subject"] = "Statement"
    message.set_content("Password is your DOB.")
    message.add_attachment(
        b"%PDF-1.4...",
        maintype="application",
        subtype="pdf",
        filename="statement.pdf",
    )
    return message.as_bytes()


def _build_html_only_message():
    message = EmailMessage()
    message["Message-ID"] = "<html@example.com>"
    message["From"] = "billing@example.com"
    message["Subject"] = "March bill"
    message.add_alternative(
        "<html><body><p>Pay before <b>due date</b></p></body></html>",
        subtype="html",
    )
    message.add_attachment(
        b"%PDF-1.4...",
        maintype="application",
        subtype="pdf",
        filename="bill.pdf",
    )
    return message.as_bytes()


def _build_missing_message_id():
    message = EmailMessage()
    message["From"] = "billing@example.com"
    message["Subject"] = "Missing id"
    message.set_content("Your bill is attached.")
    return message.as_bytes()


def _load_module(
    monkeypatch,
    fake_imap,
    *,
    max_emails_per_run="50",
    lookback_cap_days="365",
):
    monkeypatch.setenv("EMAIL_IMAP_HOST", "imap.gmail.com")
    monkeypatch.setenv("EMAIL_USERNAME", "user@example.com")
    monkeypatch.setenv("EMAIL_APP_PASSWORD", "app-password")
    monkeypatch.setenv("EMAIL_MAILBOX", "INBOX")
    monkeypatch.setenv("EMAIL_PROCESSED_LABEL", "bill-processed")
    monkeypatch.setenv("MAX_EMAILS_PER_RUN", max_emails_per_run)
    monkeypatch.setenv("EMAIL_LOOKBACK_CAP_DAYS", lookback_cap_days)

    sys.modules.pop("email_fetcher", None)
    module = importlib.import_module("email_fetcher")
    monkeypatch.setattr(module.imaplib, "IMAP4_SSL", lambda host: fake_imap)
    return module


def test_fetch_emails_returns_normalized_messages(monkeypatch):
    fake_imap = FakeIMAP(
        search_uids=b"101",
        fetch_map={"101": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap)

    messages = module.fetch_emails()

    assert messages == [
        {
            "uid": "101",
            "message_id": "<abc123@example.com>",
            "sender": "billing@example.com",
            "subject": "Statement",
            "body_text": "Password is your DOB.",
            "pdf_attachments": [b"%PDF-1.4..."],
            "pdf_filenames": ["statement.pdf"],
        }
    ]


def test_fetch_emails_contract_includes_message_id(monkeypatch):
    fake_imap = FakeIMAP(
        search_uids=b"101",
        fetch_map={"101": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap)

    message = module.fetch_emails()[0]

    assert set(message) == {
        "uid",
        "message_id",
        "sender",
        "subject",
        "body_text",
        "pdf_attachments",
        "pdf_filenames",
    }


def test_fetch_emails_searches_for_unprocessed_messages(monkeypatch):
    fake_imap = FakeIMAP(
        search_uids=b"101",
        fetch_map={"101": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap, lookback_cap_days="30")
    monkeypatch.setattr(
        module,
        "_utc_now",
        lambda: datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc),
    )

    module.fetch_emails()

    fake_imap.login.assert_called_once_with("user@example.com", "app-password")
    fake_imap.select.assert_called_once_with("INBOX")
    fake_imap.uid.assert_any_call(
        "SEARCH",
        "X-GM-RAW",
        '"after:2026/02/15 -label:bill-processed category:primary"',
    )


def test_fetch_emails_uses_search_after_parameter_for_incremental_search(
    monkeypatch,
):
    fake_imap = FakeIMAP(
        search_uids=b"101",
        fetch_map={"101": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap)
    search_after = datetime(2026, 3, 10, 9, 30, tzinfo=timezone.utc)

    module.fetch_emails(search_after=search_after)

    fake_imap.uid.assert_any_call(
        "SEARCH",
        "X-GM-RAW",
        '"after:2026/03/10 -label:bill-processed category:primary"',
    )


def test_fetch_emails_limits_processing_to_max_emails_per_run(
    monkeypatch,
):
    fake_imap = FakeIMAP(
        search_uids=b"101 102 103",
        fetch_map={
            "101": _build_plain_text_message(),
            "102": _build_plain_text_message(),
            "103": _build_plain_text_message(),
        },
    )
    module = _load_module(
        monkeypatch,
        fake_imap,
        max_emails_per_run="2",
    )

    messages = module.fetch_emails()

    assert [message["uid"] for message in messages] == ["101", "102"]
    fetch_calls = [
        call for call in fake_imap.uid.call_args_list if call.args[0] == "FETCH"
    ]
    assert [call.args[1] for call in fetch_calls] == ["101", "102"]



def test_fetch_emails_extracts_sender_subject_body_and_attachments(monkeypatch):
    fake_imap = FakeIMAP(
        search_uids=b"101",
        fetch_map={"101": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap)

    result = module.fetch_emails()

    assert result[0]["message_id"] == "<abc123@example.com>"
    assert result[0]["sender"] == "billing@example.com"
    assert result[0]["subject"] == "Statement"
    assert result[0]["body_text"] == "Password is your DOB."
    assert result[0]["pdf_attachments"] == [b"%PDF-1.4..."]


def test_fetch_emails_falls_back_to_html_body_text(monkeypatch):
    fake_imap = FakeIMAP(
        search_uids=b"202",
        fetch_map={"202": _build_html_only_message()},
    )
    module = _load_module(monkeypatch, fake_imap)

    result = module.fetch_emails()

    assert result[0]["body_text"] == "Pay before due date"


def test_fetch_emails_logs_and_skips_messages_missing_message_id(
    monkeypatch, caplog
):
    fake_imap = FakeIMAP(
        search_uids=b"303",
        fetch_map={"303": _build_missing_message_id()},
    )
    module = _load_module(monkeypatch, fake_imap)

    result = module.fetch_emails()

    assert result == []
    assert "message-id" in caplog.text.lower()


def test_fetch_emails_logs_and_skips_malformed_messages(
    monkeypatch, caplog
):
    fake_imap = FakeIMAP(
        search_uids=b"404",
        fetch_map={"404": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap)
    monkeypatch.setattr(
        module.email,
        "message_from_bytes",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("boom")),
    )

    result = module.fetch_emails()

    assert result == []
    assert "failed to parse" in caplog.text.lower()


def test_fetch_emails_does_not_mark_messages_processed_during_ingestion(
    monkeypatch,
):
    fake_imap = FakeIMAP(
        search_uids=b"101",
        fetch_map={"101": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap)

    module.fetch_emails()

    assert not any(
        call.args[0] == "STORE" for call in fake_imap.uid.call_args_list
    )


def test_mark_email_processed_applies_bill_processed_label(monkeypatch):
    fake_imap = FakeIMAP()
    module = _load_module(monkeypatch, fake_imap)

    module.mark_email_processed("101")

    fake_imap.uid.assert_any_call(
        "STORE",
        "101",
        "+X-GM-LABELS",
        "(bill-processed)",
    )


def test_extract_pdf_attachments_returns_only_application_pdf_parts(monkeypatch):
    fake_imap = FakeIMAP()
    module = _load_module(monkeypatch, fake_imap)

    message = EmailMessage()
    message.add_attachment(
        b"%PDF-1.4...",
        maintype="application",
        subtype="pdf",
        filename="bill.pdf",
    )
    message.add_attachment(
        b"hello",
        maintype="text",
        subtype="plain",
        filename="readme.txt",
    )

    pdf_part, txt_part = list(message.iter_attachments())

    attachments, filenames = module.extract_pdf_attachments([pdf_part, txt_part])
    assert attachments == [b"%PDF-1.4..."]
    assert filenames == ["bill.pdf"]


def test_extract_pdf_attachments_uses_get_content_for_pdf_parts(monkeypatch):
    fake_imap = FakeIMAP()
    module = _load_module(monkeypatch, fake_imap)

    class FakePdfPart:
        def get_content_type(self):
            return "application/pdf"

        def get_filename(self):
            return "bill.pdf"

        def get_content(self):
            return b"%PDF-1.7..."

        def get_payload(self, decode=False):
            raise AssertionError("legacy get_payload should not be used")

    attachments, filenames = module.extract_pdf_attachments([FakePdfPart()])

    assert attachments == [b"%PDF-1.7..."]
    assert filenames == ["bill.pdf"]


def test_extract_pdf_attachments_octet_stream_mime_and_pdf_extension_returns_bytes(monkeypatch):
    fake_imap = FakeIMAP()
    module = _load_module(monkeypatch, fake_imap)

    message = EmailMessage()
    message.add_attachment(
        b"%PDF-1.4...",
        maintype="application",
        subtype="octet-stream",
        filename="bill.pdf",
    )
    pdf_part = next(message.iter_attachments())

    attachments, filenames = module.extract_pdf_attachments([pdf_part])
    assert attachments == [b"%PDF-1.4..."]
    assert filenames == ["bill.pdf"]


def test_extract_pdf_attachments_returns_empty_list_when_no_pdfs_exist(monkeypatch):
    fake_imap = FakeIMAP()
    module = _load_module(monkeypatch, fake_imap)

    from email.message import Message
    txt_part = Message()
    txt_part.set_type("text/plain")
    txt_part.add_header("Content-Disposition", "attachment", filename="readme.txt")
    txt_part.set_payload(b"hello")

    attachments, filenames = module.extract_pdf_attachments([txt_part])
    assert attachments == []
    assert filenames == []


def test_parse_message_uses_iter_attachments_instead_of_legacy_attachment_helper(
    monkeypatch,
):
    fake_imap = FakeIMAP()
    module = _load_module(monkeypatch, fake_imap)
    iter_attachments_called = False

    attachment_part = EmailMessage()
    attachment_part.add_attachment(
        b"%PDF-1.4...",
        maintype="application",
        subtype="pdf",
        filename="statement.pdf",
    )

    class FakeMessage:
        def get(self, key):
            headers = {
                "Message-ID": "<iter@example.com>",
                "From": "Billing Team <billing@example.com>",
                "Subject": "Statement",
            }
            return headers.get(key)

        def iter_attachments(self):
            nonlocal iter_attachments_called
            iter_attachments_called = True
            return [next(attachment_part.iter_attachments())]

    monkeypatch.setattr(
        module.email,
        "message_from_bytes",
        lambda *args, **kwargs: FakeMessage(),
    )
    monkeypatch.setattr(
        module,
        "_extract_body_text",
        lambda message: "Password is your DOB.",
    )

    parsed = module._parse_message("101", b"raw")

    assert iter_attachments_called is True
    assert parsed["pdf_attachments"] == [b"%PDF-1.4..."]
    assert parsed["pdf_filenames"] == ["statement.pdf"]


def test_fetch_emails_returns_pdf_attachments_contract(monkeypatch):
    fake_imap = FakeIMAP(
        search_uids=b"101",
        fetch_map={"101": _build_plain_text_message()},
    )
    module = _load_module(monkeypatch, fake_imap)

    messages = module.fetch_emails()
    assert "pdf_attachments" in messages[0]
    assert messages[0]["pdf_attachments"] == [b"%PDF-1.4..."]
    assert "attachments" not in messages[0]
    assert "pdf_filenames" in messages[0]
    assert messages[0]["pdf_filenames"] == ["statement.pdf"]


