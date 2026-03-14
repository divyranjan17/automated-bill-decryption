# src/constants/failure_reasons.py

from enum import Enum


class FailureReason(str, Enum):
    # Email filtering
    NOT_A_BILL_EMAIL = "NOT_A_BILL_EMAIL"
    
    # Attachment issues
    NO_PDF_ATTACHMENT = "NO_PDF_ATTACHMENT"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    
    # Encryption
    PDF_NOT_ENCRYPTED = "PDF_NOT_ENCRYPTED"
    
    # Password extraction
    NO_PASSWORD_HINT_FOUND = "NO_PASSWORD_HINT_FOUND"
    HINT_FOUND_BUT_UNPARSABLE = "HINT_FOUND_BUT_UNPARSABLE"
    
    # Rule validation
    INVALID_RULE = "INVALID_RULE"
    
    # User data
    REQUIRED_USER_DATA_MISSING = "REQUIRED_USER_DATA_MISSING"
    
    # Decryption
    WRONG_PASSWORD = "WRONG_PASSWORD"
    CANDIDATE_LIST_EXHAUSTED = "CANDIDATE_LIST_EXHAUSTED"
    
    # Static password institutions
    REQUIRES_STATIC_PASSWORD = "REQUIRES_STATIC_PASSWORD"