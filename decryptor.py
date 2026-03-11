"""Decrypt a PDF using one provided password in a single attempt.

This module only attempts decryption with the caller-supplied password and
returns structured status metadata. It does not generate passwords, does not retry
with multiple password candidates, and uses a single attempt only.
"""

import pikepdf
import logging

logger = logging.getLogger(__name__)

def decrypt_pdf(input_path: str, password: str, output_path: str="") -> dict:
    """
    Attempts to decrypt a password-protected PDF using the given password
    if wrong password → return failure dict
    if not encrypted → return not_encrypted dict  
    if success → save to output_path, return success dict
    Args:
        input_path: Absolute path to the encrypted input PDF.
        password: Password string to attempt decryption with.
        output_path: Absolute path to save the decrypted PDF.
    Returns:
        A dict with keys:
            - status: "success" | "failure"
            - output_path: str (on success)
            - failure_reason: str (on failure)
            - attempts: int
    """
    attempts = 1 # can be oonfigured later with multiple password candidates
    
    # check if pdf is encrypted or not
    logger.info("attempting pdf decryption without password")
    try:
        with pikepdf.open(input_path) as pdf: # try without password
            logger.info(
                f"PDF at {input_path} is not encrypted. Skipping."
            )
            return {
                "status": "failure",
                "failure_reason": "PDF_NOT_ENCRYPTED",
                "attempts": attempts,
            }
    except pikepdf.PasswordError:
        pass

    try:
        with pikepdf.open(input_path, password) as pdf:
            if not output_path: # output_path is mandatory
                raise ValueError("output_path must be provided for saving decrypted PDF")
            pdf.save(output_path)
            logger.info(f"PDF decrypted successfully from {input_path} -> {output_path} in {attempts} attempts")
            return {
                "status": "success",
                "output_path": output_path,
                "attempts": attempts
            }
    # exception scenarios:
    # wrong password
    except pikepdf.PasswordError:
        logger.error("Incorrect password. Try again")
        return {
            "status": "failure",
            "failure_reason": "WRONG_PASSWORD",
            "attempts": attempts,
        }
