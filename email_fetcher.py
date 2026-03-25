"""Fetch normalized Gmail messages via IMAP for the sequential bill pipeline."""

from __future__ import annotations

import email
import imaplib
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from email import policy
from email.header import decode_header, make_header
from email.message import Message
from email.utils import parseaddr
from html import unescape
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False

logger = logging.getLogger(__name__)

DEFAULT_MAILBOX = "INBOX"
DEFAULT_PROCESSED_LABEL = "bill-processed"
DEFAULT_CATEGORY = "primary"
DEFAULT_LOOKBACK_CAP_DAYS = 30
UNPROCESSED_SEARCH_TEMPLATE = 'after:{after_date} -label:{label} category:{category}'
REQUIRED_KEYS = {
    "uid",
    "message_id",
    "sender",
    "subject",
    "body_text",
    "pdf_attachments",
    "pdf_filenames",
}


def fetch_emails(search_after: Optional[datetime] = None) -> list[dict]:
    """Fetch normalized email data for unprocessed Gmail messages.

    Args:
        search_after: Fetch emails received after this timestamp. When None,
                      falls back to ``lookback_cap_days`` days ago.

    Returns:
        List of normalized email dicts.
    """
    config = _load_config()
    logger.info(f"Connecting to IMAP host {config["host"]}, search_after:{search_after}")

    if search_after is not None:
        search_start = search_after
    else:
        search_start = _utc_now() - timedelta(days=config["lookback_cap_days"])

    messages: list[dict] = []
    with imaplib.IMAP4_SSL(config["host"]) as client:
        client.login(config["username"], config["password"])
        logger.info("Authenticated IMAP user %s", config["username"])

        status, _ = client.select(config["mailbox"])
        if status != "OK":
            logger.error("Failed to select mailbox %s", config["mailbox"])
            return []
        logger.info("Selected mailbox %s", config["mailbox"])

        search_query = UNPROCESSED_SEARCH_TEMPLATE.format(
            after_date=search_start.strftime("%Y/%m/%d"),
            label=config["processed_label"],
            category=config["category"],
        )
        # print(f"search query: {search_query}")
        status, search_data = client.uid(
            "SEARCH",
            "X-GM-RAW",
            f'"{search_query}"'
        )
        if status != "OK":
            logger.error("Failed to search mailbox %s", config["mailbox"])
            return []

        candidate_uids = _parse_search_uids(search_data)
        selected_uids = candidate_uids
        logger.info(
            "Fetched %s candidate UIDs for mailbox %s; processing %s",
            len(candidate_uids),
            config["mailbox"],
            len(selected_uids),
        )
        for uid in selected_uids:
            logger.info(f"Processing email uid={uid}")
            parsed = _fetch_and_parse_message(client, uid)
            if parsed is not None:
                logger.info(
                    "Successfully parsed uid=%s | sender=%s | subject=%s | pdf_attachments=%s",
                    uid,
                    parsed["sender"],
                    parsed["subject"],
                    len(parsed["pdf_attachments"]),
                )
                messages.append(parsed)
            else:
                logger.warning(f"Skipping uid={uid} - failed to parse")
    return messages


def extract_pdf_attachments(
    raw_parts: list[Message],
) -> tuple[list[bytes], list[str]]:
    """Filter and extract PDF payloads and filenames from a list of message parts."""
    pdf_attachments: list[bytes] = []
    pdf_filenames: list[str] = []
    for part in raw_parts:
        mime_type = part.get_content_type()
        raw_filename = part.get_filename() or ""
        filename_lower = raw_filename.lower()

        is_pdf = (
            mime_type == "application/pdf"
            or (
                mime_type in (
                    "application/octet-stream",
                    "application/download",
                    "application/force-download",
                )
                and filename_lower.endswith(".pdf")
            )
        )

        if not is_pdf:
            continue

        try:
            payload = part.get_content()
        except Exception:
            logger.warning("Failed to extract content from attachment part")
            continue

        if isinstance(payload, bytes) and payload:
            pdf_attachments.append(payload)
            pdf_filenames.append(raw_filename or "attachment.pdf")

    return pdf_attachments, pdf_filenames


def mark_email_processed(uid: str) -> None:
    """Apply the processed Gmail label after terminal pipeline completion."""
    config = _load_config()
    logger.info("Marking uid=%s as processed", uid)

    with imaplib.IMAP4_SSL(config["host"]) as client:
        client.login(config["username"], config["password"])
        status, _ = client.select(config["mailbox"])
        if status != "OK":
            logger.error("Failed to select mailbox %s", config["mailbox"])
            raise RuntimeError("Unable to select mailbox for processed label")

        status, _ = client.uid(
            "STORE",
            uid,
            "+X-GM-LABELS",
            f"({config['processed_label']})",
        )
        if status != "OK":
            logger.error("Failed to apply processed label to uid=%s", uid)
            raise RuntimeError("Unable to apply processed label")


def _load_config() -> dict:
    load_dotenv()
    host = _get_required_env("EMAIL_IMAP_HOST")
    username = _get_required_env("EMAIL_USERNAME")
    password = _get_required_env("EMAIL_APP_PASSWORD")

    return {
        "host": host,
        "username": username,
        "password": password,
        "mailbox": os.getenv("EMAIL_MAILBOX", DEFAULT_MAILBOX),
        "processed_label": os.getenv(
            "EMAIL_PROCESSED_LABEL",
            DEFAULT_PROCESSED_LABEL,
        ),
        "category": os.getenv("EMAIL_CATEGORY", DEFAULT_CATEGORY),
        "lookback_cap_days": _get_int_env(
            "EMAIL_LOOKBACK_CAP_DAYS",
            DEFAULT_LOOKBACK_CAP_DAYS,
        ),
    }


def _get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        logger.error("Missing required environment variable %s", name)
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default

    try:
        value = int(raw_value)
    except ValueError as exc:
        logger.error("Invalid integer environment variable %s=%s", name, raw_value)
        raise RuntimeError(f"Invalid integer environment variable: {name}") from exc

    if value <= 0:
        logger.error("Environment variable %s must be positive", name)
        raise RuntimeError(f"Environment variable must be positive: {name}")
    return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_timestamp(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _parse_search_uids(search_data: list) -> list[str]:
    if not search_data:
        return []

    raw_uids = search_data[0]
    if isinstance(raw_uids, bytes):
        raw_uids = raw_uids.decode("utf-8", errors="ignore")

    return [uid for uid in str(raw_uids).split() if uid]


def _fetch_and_parse_message(
    client: imaplib.IMAP4_SSL, uid: str
) -> Optional[dict]:
    status, fetch_data = client.uid("FETCH", uid, "(RFC822)")
    if status != "OK":
        logger.error("Failed to fetch email uid=%s", uid)
        return None

    raw_bytes = _extract_raw_message(fetch_data)
    if raw_bytes is None:
        logger.error("Failed to parse email uid=%s: missing RFC822 payload", uid)
        return None

    try:
        parsed = _parse_message(uid, raw_bytes)
    except Exception as exc:  # pragma: no cover - defensive logging path
        logger.error("Failed to parse email uid=%s: %s", uid, exc)
        return None

    if not _is_valid_normalized_email(parsed):
        logger.error("Failed to parse email uid=%s: invalid normalized shape", uid)
        return None

    return parsed


def _extract_raw_message(fetch_data: list) -> Optional[bytes]:
    for item in fetch_data:
        if isinstance(item, tuple) and len(item) > 1 and isinstance(item[1], bytes):
            return item[1]
    return None


def _parse_message(uid: str, raw_bytes: bytes) -> dict:
    message = email.message_from_bytes(raw_bytes, policy=policy.default)
    message_id = _clean_header(message.get("Message-ID"))
    if not message_id:
        logger.error("Missing Message-ID for uid=%s", uid)
        raise ValueError("message-id missing")

    sender = parseaddr(_clean_header(message.get("From")))[1]
    subject = _decode_header_value(message.get("Subject"))
    body_text = _extract_body_text(message)
    attachment_parts = list(message.iter_attachments())
    # TODO: Phase 3: iter_attachments() skips inline images (content_disposition=inline).
    # Banks like HDFC embed password hints as inline images with a Content-ID header.
    # Phase 3: add a separate iter_parts() pass to extract inline image bytes
    # and return them as inline_images: list[bytes] for vision LLM processing.
    # Not to be taken up till phase 3
    pdf_attachments, pdf_filenames = extract_pdf_attachments(attachment_parts)

    return {
        "uid": uid,
        "message_id": message_id,
        "sender": sender,
        "subject": subject,
        "body_text": body_text,
        "pdf_attachments": pdf_attachments,
        "pdf_filenames": pdf_filenames,
    }


def _clean_header(value: Optional[str]) -> str:
    return str(value).strip() if value else ""


def _decode_header_value(value: Optional[str]) -> str:
    if not value:
        return ""
    return str(make_header(decode_header(str(value)))).strip()


def _extract_body_text(message: Message) -> str:
    plain_text = _extract_text_part(message, preferred_subtype="plain")
    if plain_text:
        return plain_text

    html_text = _extract_text_part(message, preferred_subtype="html")
    return _html_to_text(html_text)


def _extract_text_part(message: Message, preferred_subtype: str) -> str:
    parts = message.walk() if message.is_multipart() else [message]
    for part in parts:
        if part.is_multipart():
            continue
        if part.get_content_maintype() != "text":
            continue
        if part.get_content_subtype() != preferred_subtype:
            continue
        if part.get_content_disposition() == "attachment":
            continue

        payload = part.get_payload(decode=True) or b""
        charset = part.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace").strip()
    return ""


def _html_to_text(value: str) -> str:
    if not value:
        return ""

    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def _is_valid_normalized_email(message: dict) -> bool:
    if set(message) != REQUIRED_KEYS:
        return False
    if not all(
        isinstance(message[key], str)
        for key in REQUIRED_KEYS - {"pdf_attachments", "pdf_filenames"}
    ):
        return False
    if not isinstance(message["pdf_attachments"], list):
        return False
    if not all(isinstance(item, bytes) for item in message["pdf_attachments"]):
        return False
    if not isinstance(message["pdf_filenames"], list):
        return False
    if len(message["pdf_filenames"]) != len(message["pdf_attachments"]):
        return False
    return all(isinstance(item, str) for item in message["pdf_filenames"])
