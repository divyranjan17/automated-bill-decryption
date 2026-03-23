"""persistence.py — SQLite persistence layer for bill decryption pipeline.

Provides schema initialization and CRUD operations for the user, email,
document, decryption_attempt, and pipeline_state tables. Called exclusively
from orchestrator.run_pipeline().
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False

logger = logging.getLogger(__name__)

_DEFAULT_DB_PATH = "data/bill_decryption.db"


def _get_connection(db_path: str) -> sqlite3.Connection:
    """Open a new SQLite connection with foreign key enforcement enabled.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        An open sqlite3.Connection with row_factory set to Row.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str = _DEFAULT_DB_PATH) -> None:
    """Create all tables if they do not already exist.

    Also ensures the parent directory exists before creating the DB file.

    Args:
        db_path: Path to the SQLite database file.
    """
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS user (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            email           TEXT    NOT NULL UNIQUE,
            name            TEXT    NOT NULL,
            dob             TEXT    NOT NULL,
            mobile          TEXT,
            pan             TEXT,
            card_masked     TEXT,
            account_masked  TEXT,
            created_at      TEXT    NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS email (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER NOT NULL REFERENCES user(id),
            uid             TEXT    NOT NULL,
            message_id      TEXT,
            sender          TEXT,
            subject         TEXT,
            received_at     TEXT,
            status          TEXT    NOT NULL,
            failure_reason  TEXT,
            created_at      TEXT    NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            processed_at    TEXT,
            UNIQUE(uid, user_id),
            UNIQUE(message_id, user_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS document (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER NOT NULL REFERENCES user(id),
            email_id        INTEGER NOT NULL REFERENCES email(id),
            filename        TEXT,
            is_encrypted    INTEGER NOT NULL DEFAULT 0,
            output_path     TEXT,
            status          TEXT    NOT NULL,
            created_at      TEXT    NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS decryption_attempt (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER NOT NULL REFERENCES user(id),
            document_id     INTEGER NOT NULL REFERENCES document(id),
            attempt_number  INTEGER NOT NULL,
            failure_reason  TEXT,
            llm_used        INTEGER NOT NULL DEFAULT 0,
            llm_confidence  TEXT,
            llm_reasoning   TEXT,
            outcome         TEXT    NOT NULL,
            created_at      TEXT    NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS pipeline_state (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL REFERENCES user(id),
            key         TEXT    NOT NULL,
            value       TEXT    NOT NULL,
            updated_at  TEXT    NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
            UNIQUE(user_id, key)
        )
        """,
    ]

    conn = _get_connection(db_path)
    try:
        for stmt in ddl_statements:
            conn.execute(stmt)
        conn.commit()
        logger.info("Database initialized at %s", db_path)
    finally:
        conn.close()


def ensure_user(user_data: dict, db_path: str = _DEFAULT_DB_PATH) -> int:
    """Insert the user row if the table is empty and return the user_id.

    Idempotent: if a user row already exists, returns that row's id.
    Reads EMAIL_USERNAME from the environment to populate the email column.

    Args:
        user_data: User profile dict with keys name, dob (ISO YYYY-MM-DD),
                   mobile, pan, card_masked, account_masked.
        db_path: Path to the SQLite database file.

    Returns:
        The integer user_id.

    Raises:
        RuntimeError: If EMAIL_USERNAME environment variable is not set.
    """
    load_dotenv()
    email = os.environ.get("EMAIL_USERNAME", "").strip()
    if not email:
        raise RuntimeError("Missing required environment variable: EMAIL_USERNAME")

    conn = _get_connection(db_path)
    try:
        row = conn.execute("SELECT id FROM user LIMIT 1").fetchone()
        if row is not None:
            return row["id"]

        cursor = conn.execute(
            """
            INSERT INTO user
                (email, name, dob, mobile, pan, card_masked, account_masked)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                email,
                user_data.get("name", ""),
                user_data.get("dob", ""),
                user_data.get("mobile"),
                user_data.get("pan"),
                user_data.get("card_masked"),
                user_data.get("account_masked"),
            ),
        )
        conn.commit()
        logger.info("Inserted user id=%d email=%s", cursor.lastrowid, email)
        return cursor.lastrowid
    finally:
        conn.close()


def email_exists(
    uid: str,
    message_id: str,
    user_id: int,
    db_path: str = _DEFAULT_DB_PATH,
) -> Optional[dict]:
    """Check whether an email has already been processed.

    Matches on (uid, user_id) OR (message_id, user_id) when message_id
    is non-empty (SQLite treats NULLs as distinct in unique indexes).

    Args:
        uid: IMAP UID of the email.
        message_id: RFC 2822 Message-ID (may be empty string).
        user_id: ID of the current user.
        db_path: Path to the SQLite database file.

    Returns:
        Dict with keys id, status, failure_reason if found; else None.
    """
    conn = _get_connection(db_path)
    try:
        row = conn.execute(
            """
            SELECT id, status, failure_reason
            FROM email
            WHERE (uid = ? AND user_id = ?)
               OR (message_id = ? AND message_id != '' AND user_id = ?)
            LIMIT 1
            """,
            (uid, user_id, message_id, user_id),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "status": row["status"],
            "failure_reason": row["failure_reason"],
        }
    finally:
        conn.close()


def insert_email(
    user_id: int,
    uid: str,
    message_id: Optional[str],
    sender: Optional[str],
    subject: Optional[str],
    received_at: Optional[str],
    status: str,
    failure_reason: Optional[str],
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """Insert a new email row and return its id.

    Args:
        user_id: ID of the owning user.
        uid: IMAP UID.
        message_id: RFC 2822 Message-ID.
        sender: Sender email address.
        subject: Email subject line.
        received_at: ISO timestamp when the email was received (may be None).
        status: SUCCESS / FAILURE_TERMINAL / FAILURE_RETRYABLE.
        failure_reason: NULL for SUCCESS; always set for failures.
        db_path: Path to the SQLite database file.

    Returns:
        The new email row id.

    Raises:
        ValueError: If status/failure_reason invariant is violated.
    """
    _validate_status_failure_reason(status, failure_reason)
    conn = _get_connection(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO email
                (user_id, uid, message_id, sender, subject, received_at,
                 status, failure_reason, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id, uid, message_id, sender, subject, received_at,
                status, failure_reason, _utc_now_iso(),
            ),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def update_email_status(
    email_id: int,
    status: str,
    failure_reason: Optional[str] = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """Update the status (and optionally failure_reason) of an email row.

    Args:
        email_id: Row id to update.
        status: New status value.
        failure_reason: New failure reason (NULL for SUCCESS).
        db_path: Path to the SQLite database file.

    Raises:
        ValueError: If status/failure_reason invariant is violated.
    """
    _validate_status_failure_reason(status, failure_reason)
    conn = _get_connection(db_path)
    try:
        conn.execute(
            "UPDATE email SET status = ?, failure_reason = ?, processed_at = ?"
            " WHERE id = ?",
            (status, failure_reason, _utc_now_iso(), email_id),
        )
        conn.commit()
    finally:
        conn.close()


def insert_document(
    user_id: int,
    email_id: int,
    filename: Optional[str],
    is_encrypted: int,
    status: str,
    output_path: Optional[str] = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """Insert a document row and return its id.

    Args:
        user_id: ID of the owning user.
        email_id: ID of the parent email.
        filename: Original PDF filename.
        is_encrypted: 1 if the PDF was encrypted, 0 otherwise.
        status: DECRYPTED / FAILED.
        output_path: Path to the decrypted file (None on failure).
        db_path: Path to the SQLite database file.

    Returns:
        The new document row id.
    """
    conn = _get_connection(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO document
                (user_id, email_id, filename, is_encrypted, status, output_path)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, email_id, filename, is_encrypted, status, output_path),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def update_document_status(
    doc_id: int,
    status: str,
    output_path: Optional[str] = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """Update the status and output_path of a document row.

    Args:
        doc_id: Row id to update.
        status: New status value.
        output_path: Updated output path.
        db_path: Path to the SQLite database file.
    """
    conn = _get_connection(db_path)
    try:
        conn.execute(
            "UPDATE document SET status = ?, output_path = ? WHERE id = ?",
            (status, output_path, doc_id),
        )
        conn.commit()
    finally:
        conn.close()


def insert_attempt(
    user_id: int,
    document_id: int,
    attempt_number: int,
    outcome: str,
    failure_reason: Optional[str] = None,
    db_path: str = _DEFAULT_DB_PATH,
) -> int:
    """Insert a decryption_attempt row and return its id.

    Args:
        user_id: ID of the owning user.
        document_id: ID of the parent document.
        attempt_number: 1-indexed attempt number.
        outcome: SUCCESS / FAILED.
        failure_reason: Failure reason if outcome is FAILED.
        db_path: Path to the SQLite database file.

    Returns:
        The new decryption_attempt row id.
    """
    conn = _get_connection(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO decryption_attempt
                (user_id, document_id, attempt_number, outcome, failure_reason)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, document_id, attempt_number, outcome, failure_reason),
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def get_last_fetched_date(
    user_id: int,
    db_path: str = _DEFAULT_DB_PATH,
) -> Optional[datetime]:
    """Retrieve the last_fetched_date from pipeline_state.

    Args:
        user_id: ID of the owning user.
        db_path: Path to the SQLite database file.

    Returns:
        A timezone-aware datetime in UTC, or None if no row exists.
    """
    conn = _get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM pipeline_state"
            " WHERE user_id = ? AND key = 'last_fetched_date'",
            (user_id,),
        ).fetchone()
        if row is None:
            return None
        dt = datetime.fromisoformat(row["value"])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    finally:
        conn.close()


def update_last_fetched_date(
    user_id: int,
    timestamp: datetime,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """Upsert the last_fetched_date in pipeline_state.

    Args:
        user_id: ID of the owning user.
        timestamp: The timestamp to store.
        db_path: Path to the SQLite database file.
    """
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    value = timestamp.astimezone(timezone.utc).isoformat()
    conn = _get_connection(db_path)
    try:
        conn.execute(
            """
            INSERT INTO pipeline_state (user_id, key, value, updated_at)
            VALUES (?, 'last_fetched_date', ?, ?)
            ON CONFLICT(user_id, key) DO UPDATE
                SET value = excluded.value,
                    updated_at = excluded.updated_at
            """,
            (user_id, value, _utc_now_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def record_email_result(
    user_id: int,
    email_data: dict,
    result: object,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """Persist a complete EmailResult in a single transaction.

    Inserts one email row, one document row per PDF, and one
    decryption_attempt row per PDF (attempt_number=1 always).
    candidates_tried is logged at INFO level only, not stored.

    Args:
        user_id: ID of the owning user.
        email_data: Normalized email dict from email_fetcher.
        result: EmailResult dataclass from orchestrator.
        db_path: Path to the SQLite database file.
    """
    # Lazy import avoids circular dependency (orchestrator imports persistence)
    from orchestrator import _resolve_email_status  # noqa: PLC0415

    email_status = _resolve_email_status(result)
    failure_reason = result.failure_reason if email_status != "SUCCESS" else None

    conn = _get_connection(db_path)
    try:
        with conn:
            email_cursor = conn.execute(
                """
                INSERT INTO email
                    (user_id, uid, message_id, sender, subject, received_at,
                     status, failure_reason, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    email_data.get("uid"),
                    email_data.get("message_id"),
                    email_data.get("sender"),
                    email_data.get("subject"),
                    None,  # received_at not available in current email_data
                    email_status,
                    failure_reason,
                    _utc_now_iso(),
                ),
            )
            email_id = email_cursor.lastrowid

            for pdf_result in result.pdf_results:
                is_encrypted = (
                    0 if pdf_result.failure_reason == "PDF_NOT_ENCRYPTED" else 1
                )
                doc_status = "DECRYPTED" if pdf_result.status == "success" else "FAILED"

                doc_cursor = conn.execute(
                    """
                    INSERT INTO document
                        (user_id, email_id, filename, is_encrypted,
                         status, output_path)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        email_id,
                        pdf_result.filename,
                        is_encrypted,
                        doc_status,
                        pdf_result.output_path,
                    ),
                )
                doc_id = doc_cursor.lastrowid

                attempt_outcome = (
                    "SUCCESS" if pdf_result.status == "success" else "FAILED"
                )
                logger.info(
                    "pdf=%s candidates_tried=%d",
                    pdf_result.filename,
                    pdf_result.candidates_tried,
                )
                conn.execute(
                    """
                    INSERT INTO decryption_attempt
                        (user_id, document_id, attempt_number,
                         outcome, failure_reason)
                    VALUES (?, ?, 1, ?, ?)
                    """,
                    (
                        user_id,
                        doc_id,
                        attempt_outcome,
                        pdf_result.failure_reason,
                    ),
                )

        logger.info(
            "Recorded email uid=%s status=%s",
            email_data.get("uid"),
            email_status,
        )
    finally:
        conn.close()


_ALLOWED_USER_COLUMNS: frozenset[str] = frozenset(
    {"name", "dob", "mobile", "pan", "card_masked", "account_masked"}
)


def update_user_fields(
    user_id: int,
    updates: dict,
    db_path: str = _DEFAULT_DB_PATH,
) -> None:
    """Update specific columns on the user row.

    Only columns present in ALLOWED_USER_COLUMNS are written; unknown
    keys are silently ignored. No-op if ``updates`` is empty after
    filtering.

    Args:
        user_id: The id of the user row to update.
        updates: Dict mapping column names to new values.
        db_path: Path to the SQLite database file.
    """
    safe = {k: v for k, v in updates.items() if k in _ALLOWED_USER_COLUMNS}
    if not safe:
        return

    set_clause = ", ".join(f"{col} = ?" for col in safe)
    values = list(safe.values()) + [user_id]

    conn = _get_connection(db_path)
    try:
        conn.execute(f"UPDATE user SET {set_clause} WHERE id = ?", values)
        conn.commit()
        logger.info("Updated user id=%d columns=%s", user_id, list(safe))
    finally:
        conn.close()


def _utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _validate_status_failure_reason(
    status: str, failure_reason: Optional[str]
) -> None:
    """Enforce the status/failure_reason invariant.

    Raises:
        ValueError: If SUCCESS has a failure_reason, or non-SUCCESS lacks one.
    """
    if status == "SUCCESS" and failure_reason is not None:
        raise ValueError("failure_reason must be NULL for SUCCESS emails")
    if status != "SUCCESS" and not failure_reason:
        raise ValueError("failure_reason must be set for non-SUCCESS emails")
