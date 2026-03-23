"""src/constants/log_events.py — Named pipeline boundary events for structured logging."""

from enum import Enum


class PipelineEvent(str, Enum):
    """Named boundary events emitted at each stage of the pipeline.

    All events are logged via _format_log_event() in orchestrator.py
    using logfmt-style key=value formatting.
    """

    PIPELINE_START = "PIPELINE_START"       # run_pipeline() begins
    PIPELINE_DONE = "PIPELINE_DONE"         # run_pipeline() ends, with totals
    EMAIL_SKIP = "EMAIL_SKIP"               # email already in DB as SUCCESS/FAILURE_TERMINAL
    EMAIL_START = "EMAIL_START"             # _process_single_email() begins
    HINT_EXTRACTED = "HINT_EXTRACTED"       # extract_password_hint() returned a non-None hint
    HINT_NOT_FOUND = "HINT_NOT_FOUND"       # extract_password_hint() returned None
    RULE_BUILT = "RULE_BUILT"               # interpret_instruction() succeeded
    RULE_FAILED = "RULE_FAILED"             # interpret_instruction() raised ValueError
    STATIC_PASSWORD = "STATIC_PASSWORD"     # rule.requires_static_password is True
    CANDIDATES_BUILT = "CANDIDATES_BUILT"   # build_candidates() returned a list
    USER_DATA_MISSING = "USER_DATA_MISSING" # build_candidates() raised ValueError
    PDF_NOT_ENCRYPTED = "PDF_NOT_ENCRYPTED" # is_encrypted() returned False for a PDF
    DECRYPT_ATTEMPT = "DECRYPT_ATTEMPT"     # each call to decrypt_pdf()
    DECRYPT_SUCCESS = "DECRYPT_SUCCESS"     # decrypt_pdf() returned status=success
    DECRYPT_EXHAUSTED = "DECRYPT_EXHAUSTED" # all candidates tried, all failed
    EMAIL_DONE = "EMAIL_DONE"               # terminal outcome for email (success or failure)
    EMAIL_LABELED = "EMAIL_LABELED"         # Gmail label applied via mark_email_processed()
