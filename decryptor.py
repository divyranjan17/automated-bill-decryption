"""Decrypt a PDF using one provided password in a single attempt.

This module only attempts decryption with the caller-supplied password
and returns structured status metadata. It does not generate passwords,
does not retry with multiple candidates, and makes a single attempt only.
"""

import os
import logging
import pikepdf

logger = logging.getLogger(__name__)


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
            "failure_reason": "FILE_NOT_FOUND",
            "attempts": 0,
        }

    logger.info(f"Checking encryption status for: {input_path}")
    try:
        with pikepdf.open(input_path):
            logger.info(f"PDF at {input_path} is not encrypted. Skipping.")
            return {
                "status": "failure",
                "failure_reason": "PDF_NOT_ENCRYPTED",
                "attempts": 1,
            }
    except pikepdf.PasswordError:
        pass

    logger.info(f"Attempting decryption: {input_path}")
    try:
        with pikepdf.open(input_path, password=password) as pdf:
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
            "failure_reason": "WRONG_PASSWORD",
            "attempts": 1,
        }