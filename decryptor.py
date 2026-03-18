"""Decrypt a PDF using one provided password in a single attempt.

This module only attempts decryption with the caller-supplied password
and returns structured status metadata. It does not generate passwords,
does not retry with multiple candidates, and makes a single attempt only.
"""

import os
import logging
import io
import pikepdf
from src.constants.failure_reasons import FailureReason

logger = logging.getLogger(__name__)


def is_encrypted(pdf_bytes: bytes) -> bool:
    """Check if a PDF is password-protected using pikepdf.

    Args:
        pdf_bytes: The raw bytes of the PDF file.

    Returns:
        True if the PDF is encrypted, False otherwise.
    """
    try:
        with pikepdf.open(io.BytesIO(pdf_bytes)):
            return False
    except pikepdf.PasswordError:
        return True


def decrypt_pdf(input_path: str, password: str, output_path: str) -> dict:
    """Attempt to decrypt a password-protected PDF.

    Args:
        input_path: Absolute path to the encrypted input PDF.
        password: Password string to attempt decryption with.
        output_path: Absolute path to save the decrypted PDF.

    Returns:
        A dict with keys:
            - status: "success" | "failure"
            - output_path: str (present only on success)
            - failure_reason: one of FILE_NOT_FOUND | PDF_NOT_ENCRYPTED| WRONG_PASSWORD (present only on failure)
            - attempts: int
    """
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        return {
            "status": "failure",
            "failure_reason": FailureReason.FILE_NOT_FOUND.value,
            "attempts": 0,
        }

    try:
        with open(input_path, "rb") as f:
            pdf_bytes = f.read()
    except Exception as exc:
        logger.error(f"Failed to read input file {input_path}: {exc}")
        raise

    logger.info(f"Checking encryption status for: {input_path}")
    if not is_encrypted(pdf_bytes):
        logger.info(f"PDF at {input_path} is not encrypted. Skipping.")
        return {
            "status": "failure",
            "failure_reason": FailureReason.PDF_NOT_ENCRYPTED.value,
            "attempts": 1,
        }

    logger.info(f"Attempting decryption: {input_path}")
    try:
        # Use io.BytesIO(pdf_bytes) to avoid re-reading from disk
        with pikepdf.open(io.BytesIO(pdf_bytes), password=password) as pdf:
            pdf.save(output_path)
            logger.info(
                f"Decrypted successfully: {input_path} -> {output_path}"
            )
            return {
                "status": "success",
                "output_path": output_path,
                "attempts": 1,
            }
    except pikepdf.PasswordError:
        logger.error(f"Incorrect password for: {input_path}")
        return {
            "status": "failure",
            "failure_reason": FailureReason.WRONG_PASSWORD.value,
            "attempts": 1,
        }