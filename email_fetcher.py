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
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - fallback for minimal environments
    def load_dotenv() -> bool:
        return False

logger = logging.getLogger(__name__)

DEFAULT_MAILBOX = "INBOX"
DEFAULT_PROCESSED_LABEL = "bill-processed"
DEFAULT_MAX_EMAILS_PER_RUN = 50
DEFAULT_LOOKBACK_CAP_DAYS = 365
DEFAULT_CHECKPOINT_PATH = Path("data") / "email_fetch_checkpoint.txt"
UNPROCESSED_SEARCH_TEMPLATE = 'after:{after_date} -label:{label}'
REQUIRED_KEYS = {
    "uid",
    "message_id",
    "sender",
    "subject",
    "body_text",
    "attachments",
}


def fetch_emails() -> list[dict]:
    """Fetch normalized email data for unprocessed Gmail messages."""
    config = _load_config()
    logger.info("Connecting to IMAP host %s", config["host"])

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
            after_date=_resolve_search_start(config).strftime("%Y/%m/%d"),
            label=config["processed_label"],
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
        selected_uids = candidate_uids[: config["max_emails_per_run"]]
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
                    "Successfully parsed uid=%s | sender=%s | subject=%s | attachments=%s",
                    uid,
                    parsed["sender"],
                    parsed["subject"],
                    len(parsed["attachments"]),
                )
                messages.append(parsed)
            else:
                logger.warning(f"Skipping uid={uid} - failed to parse")
    return messages


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


def commit_fetch_checkpoint(timestamp: Optional[datetime] = None) -> None:
    """Persist the fetch checkpoint after a full pipeline run completes."""
    config = _load_config()
    checkpoint_path = config["checkpoint_path"]
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    checkpoint_value = _normalize_timestamp(timestamp or _utc_now())
    checkpoint_path.write_text(
        checkpoint_value.isoformat(),
        encoding="utf-8",
    )
    logger.info("Committed fetch checkpoint to %s", checkpoint_path)


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
        "max_emails_per_run": _get_int_env(
            "MAX_EMAILS_PER_RUN",
            DEFAULT_MAX_EMAILS_PER_RUN,
        ),
        "lookback_cap_days": _get_int_env(
            "EMAIL_LOOKBACK_CAP_DAYS",
            DEFAULT_LOOKBACK_CAP_DAYS,
        ),
        "checkpoint_path": Path(
            os.getenv("EMAIL_CHECKPOINT_PATH", str(DEFAULT_CHECKPOINT_PATH))
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


def _resolve_search_start(config: dict) -> datetime:
    checkpoint = _read_checkpoint(config["checkpoint_path"])
    if checkpoint is not None:
        return checkpoint
    return _utc_now() - timedelta(days=config["lookback_cap_days"])


def _read_checkpoint(checkpoint_path: Path) -> Optional[datetime]:
    if not checkpoint_path.exists():
        return None

    raw_value = checkpoint_path.read_text(encoding="utf-8").strip()
    if not raw_value:
        return None

    try:
        return _normalize_timestamp(datetime.fromisoformat(raw_value))
    except ValueError as exc:
        logger.error("Invalid checkpoint timestamp in %s", checkpoint_path)
        raise RuntimeError("Invalid fetch checkpoint timestamp") from exc


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
    attachments = _extract_attachments(message, uid)

    return {
        "uid": uid,
        "message_id": message_id,
        "sender": sender,
        "subject": subject,
        "body_text": body_text,
        "attachments": attachments,
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


def _extract_attachments(message: Message, uid: str) -> list[bytes]:
    attachments: list[bytes] = []
    parts = message.walk() if message.is_multipart() else [message]

    for part in parts:
        if part.is_multipart():
            continue

        if part.get_content_disposition() != "attachment":
            continue

        payload = part.get_payload(decode=True)
        if not payload:
            logger.error("Skipping empty attachment for uid=%s", uid)
            continue
        attachments.append(payload)

    return attachments


def _is_valid_normalized_email(message: dict) -> bool:
    if set(message) != REQUIRED_KEYS:
        return False
    if not all(isinstance(message[key], str) for key in REQUIRED_KEYS - {"attachments"}):
        return False
    if not isinstance(message["attachments"], list):
        return False
    return all(isinstance(item, bytes) for item in message["attachments"])
