"""orchestrator.py — End-to-end bill decryption pipeline.

Wires email_fetcher, interpreter, rule_engine, and decryptor into a
sequential pipeline. One public entry point: run_pipeline().
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

_UTC = timezone.utc

from decryptor import decrypt_pdf, is_encrypted
from email_fetcher import fetch_emails, mark_email_processed
from interpreter import extract_password_hint, interpret_instruction
from handle_missing_user_data import prompt_missing_fields
from persistence import (
    email_exists as email_exists_in_db,
    ensure_user,
    get_last_fetched_date,
    init_db,
    record_email_result,
    update_email_status,
    update_last_fetched_date,
    update_user_fields,
)
from rule_engine import build_candidates
from src.constants.failure_reasons import FailureReason
from src.constants.log_events import PipelineEvent

logger = logging.getLogger(__name__)


def _format_log_event(event: PipelineEvent, **kwargs) -> str:
    """Format a logfmt-style key=value log message.

    Args:
        event: The pipeline boundary event to log.
        **kwargs: Additional key=value fields to include.

    Returns:
        A logfmt-formatted string, e.g. ``event=EMAIL_START uid=abc sender=x``.
    """
    parts = [f"event={event.value}"]
    for k, v in kwargs.items():
        s = str(v)
        parts.append(f'{k}="{s}"' if " " in s else f"{k}={s}")
    return " ".join(parts)

_TERMINAL_FAILURE_REASONS: set[str] = {
    "success",
    FailureReason.NOT_A_BILL_EMAIL.value,
    FailureReason.NO_PDF_ATTACHMENT.value,
    FailureReason.PDF_NOT_ENCRYPTED.value,
    FailureReason.INVALID_RULE.value,
    FailureReason.REQUIRES_STATIC_PASSWORD.value,
    FailureReason.NO_PASSWORD_HINT_FOUND.value,
}

MAX_RETRIES: int = 3


@dataclass
class PdfResult:
    filename: str
    status: str                   # "success" | "failure"
    output_path: Optional[str]
    failure_reason: Optional[str]
    candidates_tried: int

@dataclass
class EmailResult:
    uid: str
    sender: str
    subject: str
    status: str                   # "success" | "failure"
    failure_reason: Optional[str]
    explanation: str
    pdf_results: list[PdfResult] = field(default_factory=list)


def run_pipeline(
    output_dir: str = "output/decrypted",
    profile_path: str = "data/user_profile.json",
    db_path: str = "data/bill_decryption.db",
) -> list[EmailResult]:
    """Fetch and process all unprocessed billing emails.

    Args:
        output_dir: Root directory for decrypted PDFs.
        profile_path: Path to the user profile JSON file.
        db_path: Path to the SQLite database file.

    Returns:
        List of EmailResult objects, one per processed email.
    """
    init_db(db_path)

    user = _load_user_profile(profile_path)
    # Normalize DOB from DD-MM-YYYY (user_profile.json) to ISO (YYYY-MM-DD)
    # before passing to ensure_user() or any downstream rule_engine call.
    user["dob"] = datetime.strptime(user["dob"], "%d-%m-%Y").strftime("%Y-%m-%d")

    user_id = ensure_user(user, db_path)
    logger.info(_format_log_event(PipelineEvent.PIPELINE_START, user_id=user_id))
    start_date = get_last_fetched_date(user_id, db_path)

    all_results: list[EmailResult] = []

    batch = fetch_emails(search_after=start_date)
    for email_data in batch:
        existing = email_exists_in_db(
            email_data["uid"],
            email_data.get("message_id", ""),
            user_id,
            db_path,
        )
        if existing:
            if existing["status"] in ("SUCCESS", "FAILURE_TERMINAL"):
                logger.info(
                    _format_log_event(
                        PipelineEvent.EMAIL_SKIP,
                        uid=email_data["uid"],
                        status=existing["status"],
                    )
                )
                continue

            if (
                existing["status"] == "FAILURE_RETRYABLE"
                and existing["retry_count"] >= MAX_RETRIES
            ):
                update_email_status(
                    existing["id"],
                    "FAILURE_TERMINAL",
                    failure_reason=FailureReason.MAX_RETRIES_EXCEEDED.value,
                    db_path=db_path,
                )
                logger.warning(
                    _format_log_event(
                        PipelineEvent.EMAIL_SKIP,
                        uid=email_data["uid"],
                        status="FAILURE_TERMINAL",
                        reason="MAX_RETRIES_EXCEEDED",
                        retry_count=existing["retry_count"],
                    )
                )
                continue

        result = _process_single_email(
            email_data, user, output_dir,
            user_id=user_id, db_path=db_path,
        )
        all_results.append(result)

        record_email_result(user_id, email_data, result, db_path)
        logger.info(
            _format_log_event(
                PipelineEvent.EMAIL_DONE,
                uid=result.uid,
                status=result.status,
                failure_reason=result.failure_reason,
            )
        )

        if _is_terminal(result):
            mark_email_processed(email_data["uid"])
            logger.info(
                _format_log_event(PipelineEvent.EMAIL_LABELED, uid=result.uid)
            )

    success_count = sum(1 for r in all_results if r.status == "success")
    failed_count = len(all_results) - success_count
    logger.info(
        _format_log_event(
            PipelineEvent.PIPELINE_DONE,
            total=len(all_results),
            success=success_count,
            failed=failed_count,
        )
    )
    update_last_fetched_date(user_id, datetime.now(_UTC), db_path)
    return all_results


def _process_single_email(
    email_data: dict,
    user: dict,
    output_dir: str,
    user_id: Optional[int] = None,
    db_path: Optional[str] = None,
) -> EmailResult:
    """Run the full pipeline for one email.

    Args:
        email_data: Normalized email dict from email_fetcher.
        user: User profile dict.
        output_dir: Root directory for decrypted PDFs.

    Returns:
        EmailResult with aggregated status and per-PDF results.
    """
    uid = email_data["uid"]
    sender = email_data["sender"]
    subject = email_data["subject"]
    body_text = email_data["body_text"]
    pdf_attachments: list[bytes] = email_data["pdf_attachments"]
    pdf_filenames: list[str] = email_data["pdf_filenames"]

    logger.info(
        _format_log_event(
            PipelineEvent.EMAIL_START,
            uid=uid,
            sender=sender,
            pdfs=len(pdf_attachments),
        )
    )

    # Step 1: No PDF attachments
    if not pdf_attachments:
        return EmailResult(
            uid=uid,
            sender=sender,
            subject=subject,
            status="failure",
            failure_reason=FailureReason.NO_PDF_ATTACHMENT.value,
            explanation="No PDF attachments found in email.",
        )

    # Step 2: Extract password hint
    hint = extract_password_hint(body_text)
    if hint is None:
        logger.info(_format_log_event(PipelineEvent.HINT_NOT_FOUND, uid=uid))
        return EmailResult(
            uid=uid,
            sender=sender,
            subject=subject,
            status="failure",
            failure_reason=FailureReason.NO_PASSWORD_HINT_FOUND.value,
            explanation="No password hint found in email body.",
        )

    logger.info(
        _format_log_event(
            PipelineEvent.HINT_EXTRACTED,
            uid=uid,
            hint_found=True,
            hint_length=len(hint),
        )
    )

    # Step 3: Parse hint into rule
    try:
        rule = interpret_instruction(hint, user)
    except ValueError as exc:
        logger.warning(
            _format_log_event(PipelineEvent.RULE_FAILED, uid=uid, reason=exc)
        )
        return EmailResult(
            uid=uid,
            sender=sender,
            subject=subject,
            status="failure",
            failure_reason=FailureReason.HINT_FOUND_BUT_UNPARSABLE.value,
            explanation=f"Hint found but could not be parsed: {exc}",
        )

    logger.info(
        _format_log_event(
            PipelineEvent.RULE_BUILT,
            uid=uid,
            components=len(rule.get("components", [])),
            ambiguous=rule.get("ambiguous", False),
            confidence=rule.get("confidence", "unknown"),
        )
    )

    # Step 4: Check for static password requirement
    if rule.get("requires_static_password"):
        logger.info(_format_log_event(PipelineEvent.STATIC_PASSWORD, uid=uid))
        return EmailResult(
            uid=uid,
            sender=sender,
            subject=subject,
            status="failure",
            failure_reason=FailureReason.REQUIRES_STATIC_PASSWORD.value,
            explanation="Email requires a static password that cannot be derived.",
        )

    # Step 5: Build candidate password list
    try:
        candidates = build_candidates(rule, user)
    except ValueError as exc:
        reason, _, fields_str = str(exc).partition(":")
        if reason == FailureReason.REQUIRED_USER_DATA_MISSING.value and fields_str:
            missing_fields = fields_str.split(",")
            logger.warning(
                _format_log_event(
                    PipelineEvent.USER_DATA_MISSING, uid=uid, missing=fields_str
                )
            )
            updated_user = prompt_missing_fields(missing_fields, user)
            if user_id is not None and db_path:
                update_user_fields(user_id, updated_user, db_path)
            try:
                candidates = build_candidates(rule, updated_user)
            except ValueError:
                logger.warning(
                    _format_log_event(
                        PipelineEvent.USER_DATA_MISSING, uid=uid, retry=True
                    )
                )
                return EmailResult(
                    uid=uid,
                    sender=sender,
                    subject=subject,
                    status="failure",
                    failure_reason=FailureReason.REQUIRED_USER_DATA_MISSING.value,
                    explanation="Required user data still missing after prompt.",
                )
            # fall through to Step 6 with updated candidates
        else:
            logger.warning(
                _format_log_event(PipelineEvent.USER_DATA_MISSING, uid=uid)
            )
            return EmailResult(
                uid=uid,
                sender=sender,
                subject=subject,
                status="failure",
                failure_reason=FailureReason.REQUIRED_USER_DATA_MISSING.value,
                explanation=f"Required user data missing: {exc}",
            )

    logger.info(
        _format_log_event(
            PipelineEvent.CANDIDATES_BUILT, uid=uid, count=len(candidates)
        )
    )

    # Step 6: Attempt decryption for each PDF
    pdf_results: list[PdfResult] = []
    for pdf_bytes, pdf_filename in zip(pdf_attachments, pdf_filenames):
        pdf_result = _process_pdf(
            pdf_bytes, pdf_filename, sender, candidates, output_dir, uid
        )
        pdf_results.append(pdf_result)

    # Step 7: Aggregate per-PDF results into EmailResult
    return _aggregate_results(uid, sender, subject, pdf_results)


def _process_pdf(
    pdf_bytes: bytes,
    pdf_filename: str,
    sender: str,
    candidates: list[str],
    output_dir: str,
    uid: str,
) -> PdfResult:
    """Attempt to decrypt a single PDF using the candidate password list.

    Args:
        pdf_bytes: Raw bytes of the encrypted PDF.
        pdf_filename: Original filename of the PDF attachment.
        sender: Email address of the sender (used for output path).
        candidates: Ordered list of candidate passwords to try.
        output_dir: Root directory for decrypted PDFs.
        uid: Correlation key from the parent email (used in log events).

    Returns:
        PdfResult with decryption outcome.
    """
    if not is_encrypted(pdf_bytes):
        logger.info(
            _format_log_event(
                PipelineEvent.PDF_NOT_ENCRYPTED, uid=uid, file=pdf_filename
            )
        )
        return PdfResult(
            filename=pdf_filename,
            status="failure",
            output_path=None,
            failure_reason=FailureReason.PDF_NOT_ENCRYPTED.value,
            candidates_tried=0,
        )

    output_path = _resolve_output_path(output_dir, sender, pdf_filename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        temp_path = tmp.name

    try:
        for i, candidate in enumerate(candidates):
            logger.info(
                _format_log_event(
                    PipelineEvent.DECRYPT_ATTEMPT,
                    uid=uid,
                    file=pdf_filename,
                    attempt=i + 1,
                    total=len(candidates),
                )
            )
            result = decrypt_pdf(temp_path, candidate, output_path)
            if result["status"] == "success":
                logger.info(
                    _format_log_event(
                        PipelineEvent.DECRYPT_SUCCESS,
                        uid=uid,
                        file=pdf_filename,
                        output=result["output_path"],
                    )
                )
                return PdfResult(
                    filename=pdf_filename,
                    status="success",
                    output_path=result["output_path"],
                    failure_reason=None,
                    candidates_tried=i + 1,
                )
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    logger.warning(
        _format_log_event(
            PipelineEvent.DECRYPT_EXHAUSTED,
            uid=uid,
            file=pdf_filename,
            candidates_tried=len(candidates),
        )
    )
    return PdfResult(
        filename=pdf_filename,
        status="failure",
        output_path=None,
        failure_reason=FailureReason.CANDIDATE_LIST_EXHAUSTED.value,
        candidates_tried=len(candidates),
    )


def _aggregate_results(
    uid: str,
    sender: str,
    subject: str,
    pdf_results: list[PdfResult],
) -> EmailResult:
    """Aggregate per-PDF results into a single EmailResult.

    Returns "success" if any PDF was decrypted; otherwise "failure"
    with the first PDF's failure reason.
    """
    if any(r.status == "success" for r in pdf_results):
        return EmailResult(
            uid=uid,
            sender=sender,
            subject=subject,
            status="success",
            failure_reason=None,
            explanation="One or more PDFs decrypted successfully.",
            pdf_results=pdf_results,
        )

    worst_reason = (
        pdf_results[0].failure_reason
        if pdf_results
        else FailureReason.CANDIDATE_LIST_EXHAUSTED.value
    )
    return EmailResult(
        uid=uid,
        sender=sender,
        subject=subject,
        status="failure",
        failure_reason=worst_reason,
        explanation="All PDFs failed to decrypt.",
        pdf_results=pdf_results,
    )


def _load_user_profile(path: str) -> dict:
    """Load user profile from a JSON file.

    Args:
        path: Path to the JSON file.

    Returns:
        Parsed user profile dict.

    Raises:
        RuntimeError if the file is missing or contains invalid JSON.
    """
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as exc:
        raise RuntimeError(f"User profile not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in user profile: {path}") from exc


def _extract_sender_domain(sender: str) -> str:
    """Extract the domain portion of an email address."""
    if "@" in sender:
        return sender.split("@")[-1]
    return sender


def _resolve_output_path(output_dir: str, sender: str, filename: str) -> str:
    """Compute a deduplicated output path for a decrypted PDF.

    Args:
        output_dir: Root output directory.
        sender: Sender email address (domain used as subdirectory).
        filename: Original PDF filename.

    Returns:
        Full path string. If the base path already exists, appends
        ``_1``, ``_2``, etc. until a free path is found.
    """
    domain = _extract_sender_domain(sender)
    base_path = os.path.join(output_dir, domain, filename)

    if not os.path.exists(base_path):
        return base_path

    name, ext = os.path.splitext(filename)
    counter = 1
    while True:
        candidate_path = os.path.join(output_dir, domain, f"{name}_{counter}{ext}")
        if not os.path.exists(candidate_path):
            return candidate_path
        counter += 1


def _is_terminal(result: EmailResult) -> bool:
    """Return True if this result warrants applying the processed label."""
    if result.status == "success":
        return True
    return result.failure_reason in _TERMINAL_FAILURE_REASONS


def _resolve_email_status(result: EmailResult) -> str:
    """Translate an EmailResult to a DB email status string.

    Args:
        result: EmailResult from the pipeline.

    Returns:
        One of SUCCESS / FAILURE_TERMINAL / FAILURE_RETRYABLE.
    """
    if result.status == "success":
        return "SUCCESS"
    if result.failure_reason in _TERMINAL_FAILURE_REASONS:
        return "FAILURE_TERMINAL"
    return "FAILURE_RETRYABLE"
