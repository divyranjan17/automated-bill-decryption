# Automated Bill Decryption Agent

A single-user CLI tool that monitors your Gmail inbox for billing emails containing password-protected PDFs and decrypts them automatically. The password is described in the email body in natural language (e.g. _"Password is first 4 letters of your name followed by birth year"_) — the pipeline extracts the hint, derives candidate passwords from your profile, and decrypts the PDF without any manual effort.

Typical use cases: bank statements, ISP bills, government invoices, and similar documents that financial institutions routinely encrypt.

---

## Table of Contents

- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [User Profile Setup](#user-profile-setup)
- [Running the CLI](#running-the-cli)
- [Architecture](#architecture)
- [Supported Password Patterns](#supported-password-patterns)
- [Failure Modes](#failure-modes)
- [Testing](#testing)
- [Project Roadmap](#project-roadmap)
- [Future Enhancements](#future-enhancements)
- [Troubleshooting](#troubleshooting)
- [Security Notes](#security-notes)

---

## Key Features

- Connects to Gmail via IMAP using an App Password — no OAuth flow required
- Checkpoint-based incremental fetching — only processes new emails each run
- Automatically labels processed emails (`bill-processed`) for idempotency
- Extracts password hints from plain-text and HTML email bodies
- Derives candidate passwords using your personal profile (name, DOB, PAN, mobile, etc.)
- Decrypts PDFs with `pikepdf` and saves clean copies to a local output directory
- SQLite persistence layer — full audit trail of every processed email and decryption attempt
- Retry queue — retryable failures (e.g. missing user data) are reattempted on the next run
- Structured failure reporting — every failure is logged with a reason code, never silently dropped
- Structured logging — logfmt-style events at every pipeline boundary for easy parsing
- Manual escalation — prompts you at runtime if required profile fields are missing
- Strictly sequential, single-user pipeline — no background workers, no message queues

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| CLI | Typer |
| PDF Decryption | pikepdf |
| Schema Validation | Pydantic v2 |
| Email Access | IMAP4_SSL (Gmail) |
| Persistence | SQLite |
| Configuration | python-dotenv |
| Testing | pytest |

---

## Prerequisites

- Python 3.10 or higher
- A Gmail account with:
  - IMAP access enabled (Settings → See all settings → Forwarding and POP/IMAP → Enable IMAP)
  - 2-Step Verification enabled on your Google Account
  - An App Password generated for this application

---

## Getting Started

### 1. Clone the Repository

```bash
git clone <repository-url>
cd automated-bill-decryption
```

### 2. Create a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate   # macOS / Linux
# venv\Scripts\activate    # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example below into a `.env` file in the project root:

```bash
# .env
EMAIL_IMAP_HOST=imap.gmail.com
EMAIL_USERNAME=you@gmail.com
EMAIL_APP_PASSWORD=your-16-char-app-password

# Optional — defaults shown
EMAIL_MAILBOX=INBOX
EMAIL_PROCESSED_LABEL=bill-processed
EMAIL_CATEGORY=primary
MAX_EMAILS_PER_RUN=50
EMAIL_LOOKBACK_CAP_DAYS=365
EMAIL_CHECKPOINT_PATH=data/email_fetch_checkpoint.txt
```

See [Configuration](#configuration) for full details on each variable.

### 5. Set Up Your User Profile

Edit `data/user_profile.json` with your personal details used to derive passwords:

```json
{
  "name": "Your Full Name",
  "dob": "DD-MM-YYYY",
  "mobile": "10-digit-number",
  "pan": "ABCDE1234F",
  "card_masked": "",
  "account_masked": ""
}
```

See [User Profile Setup](#user-profile-setup) for field descriptions.

### 6. Run

```bash
python main.py
```

Decrypted PDFs will be saved to `output/decrypted/` by default.

---

## Configuration

All configuration is provided via environment variables in the `.env` file.

### Required

| Variable | Description |
|---|---|
| `EMAIL_USERNAME` | Your Gmail address |
| `EMAIL_APP_PASSWORD` | 16-character Gmail App Password |

### Optional

| Variable | Default | Description |
|---|---|---|
| `EMAIL_IMAP_HOST` | `imap.gmail.com` | IMAP server hostname |
| `EMAIL_MAILBOX` | `INBOX` | Mailbox folder to scan |
| `EMAIL_PROCESSED_LABEL` | `bill-processed` | Gmail label applied after processing |
| `EMAIL_CATEGORY` | `primary` | Gmail category filter (`primary`, `updates`, etc.) |
| `MAX_EMAILS_PER_RUN` | `50` | Maximum emails to process per invocation |
| `EMAIL_LOOKBACK_CAP_DAYS` | `365` | How far back to search on first run |
| `EMAIL_CHECKPOINT_PATH` | `data/email_fetch_checkpoint.txt` | Path to checkpoint file for incremental fetching |

### Generating a Gmail App Password

1. Go to your Google Account → Security
2. Under "How you sign in to Google", click **2-Step Verification**
3. Scroll to the bottom and click **App passwords**
4. Select "Mail" as the app, then click **Generate**
5. Copy the 16-character password into your `.env` file

---

## User Profile Setup

The agent derives candidate passwords by combining fields from your user profile with the formatting instructions extracted from the email. The profile lives at `data/user_profile.json`.

| Field | Example | Notes |
|---|---|---|
| `name` | `"Lorem Ipsum"` | Full name — used for first-N-letters patterns |
| `dob` | `"15-08-1947"` | Date of birth in `DD-MM-YYYY` format |
| `mobile` | `"9876543210"` | 10-digit mobile number |
| `pan` | `"ABCDE1234F"` | PAN card number (10 characters) |
| `card_masked` | `"1234"` | Last 4 digits of debit/credit card |
| `account_masked` | `"87654321"` | Last 8 digits of bank account |

Fields can be left empty (`""`) if not applicable — the agent will skip patterns that require missing fields.

---

## Running the CLI

```bash
python main.py [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--output-dir` | `-o` | `output/decrypted` | Directory to save decrypted PDFs |
| `--profile` | `-p` | `data/user_profile.json` | Path to user profile JSON |
| `--verbose` | `-v` | `False` | Enable DEBUG-level logging |

### Examples

```bash
# Basic run
python main.py

# Custom output directory
python main.py --output-dir ~/Documents/bills

# Use a different user profile
python main.py --profile data/alt_profile.json

# Verbose logging for debugging
python main.py --verbose
```

**Exit codes**: `0` if at least one email succeeded, `1` if all emails failed.

### Example Run
```shell
python main.py -v
2026-03-25 07:06:21,064 INFO persistence: Database initialized at data/bill_decryption.db
2026-03-25 07:06:21,067 INFO orchestrator: event=PIPELINE_START user_id=1
2026-03-25 07:06:21,067 INFO email_fetcher: Connecting to IMAP host imap.gmail.com, search_after:2026-03-22 11:11:33.845575+00:00
2026-03-25 07:06:22,456 INFO email_fetcher: Authenticated IMAP user user@gmail.com
2026-03-25 07:06:22,888 INFO email_fetcher: Selected mailbox INBOX
2026-03-25 07:06:23,293 INFO email_fetcher: Fetched 5 candidate UIDs for mailbox INBOX; processing 5
2026-03-25 07:06:23,293 INFO email_fetcher: Processing email uid=84853
2026-03-25 07:06:24,129 INFO email_fetcher: Successfully parsed uid=84853 | sender=nse-direct@nse.co.in | subject=Trades executed at NSE | pdf_attachments=1
2026-03-25 07:06:24,129 INFO email_fetcher: Processing email uid=84899
2026-03-25 07:06:24,716 INFO email_fetcher: Successfully parsed uid=84899 | sender=pragmaticengineer+deepdives@substack.com | subject=“How to be a 10x engineer” – interview with a standout dev | pdf_attachments=0
2026-03-25 07:06:24,716 INFO email_fetcher: Processing email uid=84900
2026-03-25 07:06:25,347 INFO email_fetcher: Successfully parsed uid=84900 | sender=no-reply-contract-notes@reportsmailer.zerodha.net | subject=Combined Equity Contract Note for AB1234 - March 24, 2026 | pdf_attachments=1
2026-03-25 07:06:25,347 INFO email_fetcher: Processing email uid=84902
2026-03-25 07:06:25,921 INFO email_fetcher: Successfully parsed uid=84902 | sender=no-reply-margin-statements@reportsmailer.zerodha.net | subject=Daily Equity Margin Statement for AB1234 - March 24, 2026 | pdf_attachments=1
2026-03-25 07:06:25,922 INFO email_fetcher: Processing email uid=84905
2026-03-25 07:06:26,504 INFO email_fetcher: Successfully parsed uid=84905 | sender=nse-direct@nse.co.in | subject=Trades executed at NSE | pdf_attachments=1
2026-03-25 07:06:26,883 INFO orchestrator: event=EMAIL_START uid=84853 sender=nse-direct@nse.co.in pdfs=1
2026-03-25 07:06:26,886 INFO orchestrator: event=HINT_EXTRACTED uid=84853 hint_found=True hint_length=15731
2026-03-25 07:06:26,891 INFO orchestrator: event=RULE_BUILT uid=84853 components=1 ambiguous=False confidence=high
2026-03-25 07:06:26,891 INFO orchestrator: event=CANDIDATES_BUILT uid=84853 count=1
2026-03-25 07:06:26,898 INFO orchestrator: event=DECRYPT_ATTEMPT uid=84853 file=trade_details_CHGXXXXX0P_5850153944.pdf attempt=1 total=1
2026-03-25 07:06:26,898 INFO decryptor: Checking encryption status for: <tmpfile>
2026-03-25 07:06:26,899 INFO decryptor: Attempting decryption: <tmpfile>
2026-03-25 07:06:26,899 ERROR decryptor: Incorrect password for: <tmpfile>
2026-03-25 07:06:26,900 WARNING orchestrator: event=DECRYPT_EXHAUSTED uid=84853 file=trade_details_CHGXXXXX0P_5850153944.pdf candidates_tried=1
2026-03-25 07:06:26,901 INFO persistence: pdf=trade_details_ABCXXXXX0P_5850153944.pdf candidates_tried=1
2026-03-25 07:06:26,902 INFO persistence: Recorded email uid=84853 status=FAILURE_RETRYABLE
2026-03-25 07:06:26,902 INFO orchestrator: event=EMAIL_DONE uid=84853 status=failure failure_reason=CANDIDATE_LIST_EXHAUSTED
2026-03-25 07:06:26,902 INFO orchestrator: event=EMAIL_START uid=84899 sender=pragmaticengineer+deepdives@substack.com pdfs=0
2026-03-25 07:06:26,903 INFO persistence: Recorded email uid=84899 status=FAILURE_TERMINAL
2026-03-25 07:06:26,903 INFO orchestrator: event=EMAIL_DONE uid=84899 status=failure failure_reason=NO_PDF_ATTACHMENT
2026-03-25 07:06:26,904 INFO email_fetcher: Marking uid=84899 as processed
2026-03-25 07:06:29,631 INFO orchestrator: event=EMAIL_LABELED uid=84899
2026-03-25 07:06:29,633 INFO orchestrator: event=EMAIL_START uid=84900 sender=no-reply-contract-notes@reportsmailer.zerodha.net pdfs=1
2026-03-25 07:06:29,634 INFO orchestrator: event=HINT_EXTRACTED uid=84900 hint_found=True hint_length=1428
2026-03-25 07:06:29,634 INFO orchestrator: event=RULE_BUILT uid=84900 components=1 ambiguous=False confidence=high
2026-03-25 07:06:29,634 INFO orchestrator: event=CANDIDATES_BUILT uid=84900 count=1
2026-03-25 07:06:29,638 INFO orchestrator: event=DECRYPT_ATTEMPT uid=84900 file=24-03-2026-contract-notes_AB1234.pdf attempt=1 total=1
2026-03-25 07:06:29,639 INFO decryptor: Checking encryption status for: <tmpfile>
2026-03-25 07:06:29,640 INFO decryptor: Attempting decryption: <tmpfile>
2026-03-25 07:06:29,652 INFO decryptor: Decrypted successfully: <tmpfile> -> output/decrypted/reportsmailer.zerodha.net/24-03-2026-contract-notes_AB1234.pdf
2026-03-25 07:06:29,652 INFO orchestrator: event=DECRYPT_SUCCESS uid=84900 file=24-03-2026-contract-notes_AB1234.pdf output=output/decrypted/reportsmailer.zerodha.net/24-03-2026-contract-notes_AB1234.pdf
2026-03-25 07:06:29,654 INFO persistence: pdf=24-03-2026-contract-notes_AB1234.pdf candidates_tried=1
2026-03-25 07:06:29,654 INFO persistence: Recorded email uid=84900 status=SUCCESS
2026-03-25 07:06:29,654 INFO orchestrator: event=EMAIL_DONE uid=84900 status=success failure_reason=None
2026-03-25 07:06:29,655 INFO email_fetcher: Marking uid=84900 as processed
2026-03-25 07:06:32,217 INFO orchestrator: event=EMAIL_LABELED uid=84900
2026-03-25 07:06:32,219 INFO orchestrator: event=EMAIL_START uid=84902 sender=no-reply-margin-statements@reportsmailer.zerodha.net pdfs=1
2026-03-25 07:06:32,219 INFO orchestrator: event=HINT_NOT_FOUND uid=84902
2026-03-25 07:06:32,221 INFO persistence: Recorded email uid=84902 status=FAILURE_TERMINAL
2026-03-25 07:06:32,221 INFO orchestrator: event=EMAIL_DONE uid=84902 status=failure failure_reason=NO_PASSWORD_HINT_FOUND
2026-03-25 07:06:32,222 INFO email_fetcher: Marking uid=84902 as processed
2026-03-25 07:06:34,898 INFO orchestrator: event=EMAIL_LABELED uid=84902
2026-03-25 07:06:34,899 INFO orchestrator: event=EMAIL_START uid=84905 sender=nse-direct@nse.co.in pdfs=1
2026-03-25 07:06:34,901 INFO orchestrator: event=HINT_EXTRACTED uid=84905 hint_found=True hint_length=15731
2026-03-25 07:06:34,903 INFO orchestrator: event=RULE_BUILT uid=84905 components=1 ambiguous=False confidence=high
2026-03-25 07:06:34,903 INFO orchestrator: event=CANDIDATES_BUILT uid=84905 count=1
2026-03-25 07:06:34,905 INFO orchestrator: event=DECRYPT_ATTEMPT uid=84905 file=trade_details_CHGXXXXX0P_5854393337.pdf attempt=1 total=1
2026-03-25 07:06:34,905 INFO decryptor: Checking encryption status for: <tmpfile>
2026-03-25 07:06:34,906 INFO decryptor: Attempting decryption: <tmpfile>
2026-03-25 07:06:34,907 ERROR decryptor: Incorrect password for: <tmpfile>
2026-03-25 07:06:34,907 WARNING orchestrator: event=DECRYPT_EXHAUSTED uid=84905 file=trade_details_CHGXXXXX0P_5854393337.pdf candidates_tried=1
2026-03-25 07:06:34,908 INFO persistence: pdf=trade_details_CHGXXXXX0P_5854393337.pdf candidates_tried=1
2026-03-25 07:06:34,909 INFO persistence: Recorded email uid=84905 status=FAILURE_RETRYABLE
2026-03-25 07:06:34,909 INFO orchestrator: event=EMAIL_DONE uid=84905 status=failure failure_reason=CANDIDATE_LIST_EXHAUSTED
2026-03-25 07:06:34,909 INFO orchestrator: event=PIPELINE_DONE total=5 success=1 failed=4

========================================================================
UID          SENDER                       STATUS     FAILURE_REASON
------------------------------------------------------------------------
84853        nse-direct@nse.co.in         failure    CANDIDATE_LIST_EXHAUSTED
             trade_details_ABCXXXXX0P_5850153944.pdf failure    CANDIDATE_LIST_EXHAUSTED (tried 1)
84899        pragmaticengineer+deepdives@substack.com failure    NO_PDF_ATTACHMENT
84900        no-reply-contract-notes@reportsmailer.zerodha.net success
             24-03-2026-contract-notes_AB1234.pdf success     (tried 1)
84902        no-reply-margin-statements@reportsmailer.zerodha.net failure    NO_PASSWORD_HINT_FOUND
84905        nse-direct@nse.co.in         failure    CANDIDATE_LIST_EXHAUSTED
             trade_details_ABCXXXXX0P_5854393337.pdf failure    CANDIDATE_LIST_EXHAUSTED (tried 1)
========================================================================
Total: 5  |  Success: 1  |  Failed: 4
```
---

## Architecture

### Pipeline Flow

![Pipeline Flow](docs/architecture_flow.png)

The pipeline is strictly sequential — each step must succeed for the next to run.

```
Gmail IMAP
    │
    ▼
1. FETCH        email_fetcher.py   — Connect via IMAP, apply checkpoint filter,
                                     extract PDF attachments, mark as processed
    │
    ▼
2. IDENTIFY     orchestrator.py    — Filter billing emails; reject non-bills,
                                     emails without PDF attachments
    │
    ▼
3. EXTRACT      interpreter.py     — Locate password hint in email body,
                                     parse hint into structured PasswordRule
    │
    ▼
4. GENERATE     rule_engine.py     — Combine PasswordRule + user profile
                                     to produce candidate password list
    │
    ▼
5. DECRYPT      decryptor.py       — Try each candidate with pikepdf until
                                     one succeeds or list exhausted
    │
    ▼
6. SAVE         orchestrator.py    — Write decrypted PDF to output directory,
                                     log result, update audit trail in SQLite
```

### Directory Structure

```
automated-bill-decryption/
│
├── main.py                       # CLI entry point (Typer)
├── orchestrator.py               # Pipeline orchestration
├── email_fetcher.py              # Gmail IMAP connection + attachment extraction
├── interpreter.py                # Password hint extraction + rule parsing
├── rule_engine.py                # Candidate password generation
├── decryptor.py                  # PDF decryption (pikepdf)
├── persistence.py                # SQLite persistence layer
├── handle_missing_user_data.py   # Manual escalation for missing profile fields
├── gmail_connector.py            # Google OAuth connector (Phase 3)
├── pattern_library.py            # Rule caching stub (Phase 3)
│
├── src/
│   ├── schemas/
│   │   └── password_rule.py      # Pydantic PasswordRule model
│   └── constants/
│       ├── failure_reasons.py    # FailureReason enum
│       └── log_events.py         # PipelineEvent enum
│
├── tests/
│   ├── test_decryptor.py
│   ├── test_email_fetcher.py
│   ├── test_interpreter.py
│   ├── test_rule_engine.py
│   ├── test_orchestrator.py
│   ├── test_persistence.py
│   └── test_handle_missing_user_data.py
│
├── data/
│   ├── user_profile.json         # Personal data used for password derivation
│   ├── email_fetch_checkpoint.txt # Last successful fetch timestamp
│   └── bill_decryption.db        # SQLite database (audit trail)
│
├── docs/
│   ├── password_rule_schema.md   # PasswordRule JSON schema reference
│   ├── architecture_flow.png     # Visual pipeline diagram
│   └── plans/                    # Per-task design documents
│
├── output/
│   └── decrypted/                # Decrypted PDFs saved here (git-ignored)
│
├── requirements.txt
├── .env                          # Credentials (git-ignored — never commit this)
└── .gitignore
```

### Module Responsibilities

| Module | Responsibility |
|---|---|
| `email_fetcher.py` | IMAP connection, checkpoint-based incremental fetch, PDF extraction, HTML-to-text conversion, Gmail label application |
| `interpreter.py` | Scan email body for password hint sentences, parse natural-language hint into structured `PasswordRule` dict using regex patterns |
| `rule_engine.py` | Pure function — takes a `PasswordRule` + user profile, applies field extraction, slicing, date formatting, and case transforms to produce candidate strings |
| `decryptor.py` | Single-attempt PDF decryption via `pikepdf`; returns structured result dict |
| `persistence.py` | SQLite schema init, email/document/attempt CRUD, idempotency checks via UID and Message-ID, checkpoint tracking, retry queue queries |
| `orchestrator.py` | Coordinates all modules, handles all failure modes, drains retry queue before new fetches, aggregates per-email and per-PDF results |
| `handle_missing_user_data.py` | Prompts user at runtime for profile fields required by the current rule but absent from `user_profile.json` |
| `src/schemas/` | Pydantic models for `PasswordRule`, `Component`, `SliceRule`; used for rule validation before passing to rule engine |
| `src/constants/` | `FailureReason` enum (all terminal and non-terminal failure codes) and `PipelineEvent` enum (structured log event names) |

### Data Flow

```
email body text
    → extract_password_hint()    → hint string (e.g. "first 4 letters of name + birth year")
    → interpret_instruction()    → PasswordRule dict
    → build_candidates()         → ["Divy1997", "DIVY1997", ...]
    → decrypt_pdf() × N          → first match → decrypted PDF saved to output/
                                             → result recorded in SQLite
```

### Persistence Schema

The SQLite database (`data/bill_decryption.db`) is created automatically on first run. All tables are scoped to `user_id` to support future multi-user extension without schema changes.

| Table | Purpose |
|---|---|
| `user` | Identity fields (name, dob, mobile, pan, card_masked, account_masked) |
| `email` | Incoming email events with status and failure_reason |
| `document` | PDF attachments — filename, encryption status, output path |
| `decryption_attempt` | Per-attempt outcomes (attempt number, candidate, failure reason) |
| `pipeline_state` | Checkpoint tracking (last_fetched_date per user) |
| `pattern_library` | Cached rules per sender domain (stub — activated in Phase 3) |

---

## Supported Password Patterns

The following natural-language patterns are recognized in Phase 1–2. Patterns are attempted in priority order.

| Pattern | Example Hint | Generated Candidate |
|---|---|---|
| Name + DOB | "first 4 letters of name and DOB in DDMM" | `Divy1709` |
| Name + birth year | "first 4 letters of name followed by birth year" | `Divy1997` |
| Name + card last N digits | "name initials and last 4 digits of card" | `DR1234` |
| PAN (full or sliced) | "your PAN number" | `ABCDE1234F` |
| PAN sliced | "first 5 characters of PAN" | `ABCDE` |
| DOB only | "your date of birth in DDMMYYYY" | `17091997` |
| Mobile last N digits | "last 4 digits of mobile number" | `3210` |
| Name only | "first 6 letters of your name" | `DivyRa` |
| Ambiguous / multiple options | "Option 1: name+DOB, Option 2: PAN" | generates all variants |

**Supported date formats**: `DDMM`, `DDMMYY`, `DDMMYYYY`, `MMDD`, `MMDDYYYY`, `YYYY`

**Supported transforms**: `upper` (ABCD), `lower` (abcd), or no transform

---

## Failure Modes

Every email or PDF that cannot be processed is assigned a reason code. Processing continues for remaining items — failures are never silent.

| Reason Code | Type | Meaning |
|---|---|---|
| `NOT_A_BILL_EMAIL` | Terminal | Email does not appear to be a billing email |
| `NO_PDF_ATTACHMENT` | Terminal | Email has no PDF attachment |
| `PDF_NOT_ENCRYPTED` | Terminal | PDF is not password-protected (no decryption needed) |
| `NO_PASSWORD_HINT_FOUND` | Terminal | No password instruction found in email body |
| `HINT_FOUND_BUT_UNPARSABLE` | Terminal | Hint found but no regex pattern matched |
| `REQUIRED_USER_DATA_MISSING` | Retryable | Rule requires a field not in user profile |
| `CANDIDATE_LIST_EXHAUSTED` | Retryable | All generated candidates failed |
| `INVALID_RULE` | Terminal | Rule failed Pydantic schema validation |
| `REQUIRES_STATIC_PASSWORD` | Terminal | Hint implies a fixed password not derivable from profile |

**Terminal** failures are not reattempted. **Retryable** failures are placed in a retry queue and reprocessed on the next run (up to 3 attempts).

Each failure log entry includes: timestamp, email UID, reason code, and a human-readable explanation.

---

## Testing

### Run All Tests

```bash
pytest
```

### Run a Specific Test File

```bash
pytest tests/test_rule_engine.py
pytest tests/test_interpreter.py -v
```

### Run Tests Matching a Pattern

```bash
pytest -k "test_name" -v
```

### Test Structure

```
tests/
├── test_decryptor.py               # PDF encryption detection, decryption attempts, wrong-password handling
├── test_email_fetcher.py           # IMAP mock, PDF extraction, HTML-to-text, checkpoint read/write
├── test_interpreter.py             # Hint extraction patterns, rule parsing, all 8 supported patterns
├── test_rule_engine.py             # Date formatting, slicing, transforms, ambiguous rule expansion
├── test_orchestrator.py            # End-to-end pipeline, failure aggregation, terminal failure detection
├── test_persistence.py             # DB schema init, CRUD operations, idempotency checks, retry queue
└── test_handle_missing_user_data.py  # Manual escalation prompts, missing field detection
```

All tests use mocks for IMAP connections and `pikepdf` — no real Gmail connection or PDF files required to run the test suite.

---

## Project Roadmap

### Phase 1 — Deterministic Pipeline (Complete)

- [x] IMAP email ingestion with checkpoint-based incremental fetching
- [x] PDF attachment extraction and encryption detection
- [x] Regex-based password hint extraction (8 patterns)
- [x] Rule engine for candidate password generation
- [x] pikepdf-based PDF decryption with bounded retries
- [x] CLI orchestrator with structured result reporting

### Phase 2 — Failure Handling & Persistence (Complete)

- [x] Explicit failure taxonomy with terminal vs. retryable distinction
- [x] SQLite persistence layer with full audit trail
- [x] Idempotency via UID, Message-ID, and Gmail label
- [x] Retry queue — retryable failures reprocessed on subsequent runs
- [x] Structured logfmt-style logging at all pipeline boundaries
- [x] Manual escalation — runtime prompt for missing profile fields

### Phase 3 — AI-Assisted Decision Layer (Planned)

- [ ] LLM fallback for ambiguous or unrecognized password hints
- [ ] Schema validation of LLM output before passing to rule engine
- [ ] Image based extraction for hints embedded as images in emails
- [ ] Pattern library integration — cache confirmed rules per sender domain

### Phase 4 — CLI Polish & Demo Readiness (Planned)

- [ ] User onboarding flow — interactive CLI prompts to build `user_profile.json`
- [ ] Dockerfile for containerized execution
- [ ] Full documentation pass and demo script

---

## Future Enhancements

### Email Provider Abstraction

**Status**: Deferred until a second email provider is added.

Currently `email_fetcher.py` is Gmail-specific. The planned shape:

- Keep `fetch_emails()` as the stable public contract (unchanged)
- Introduce a narrow internal provider protocol
- Move `GmailImapProvider` behind that protocol
- Separate MIME normalization from provider-specific label and fetch logic

This abstraction is premature while Gmail via IMAP is the only provider. The Gmail label (`bill-processed`) is currently load-bearing for idempotency — decoupling it prematurely would add structure without reducing complexity.

### Google OAuth Email Connection

**Status**: Deferred to Phase 3.

`gmail_connector.py` exists as a proof-of-concept for full Gmail API access via OAuth. The current IMAP + App Password approach is sufficient for single-user use and requires no server-side redirect handling. OAuth becomes relevant if the tool is extended to multiple users or deployed as a service.

### Multi-Instrument Support

**Status**: Deferred.

The current profile supports one card and one bank account. Some users hold multiple instruments (e.g., multiple credit cards from the same issuer, each with a different last-4). The schema already uses `user_id`-scoped tables — adding a `UserInstrument` table would allow the rule engine to generate candidates across all registered instruments.

### LLM-Assisted Hint Parsing (Phase 3)

**Status**: Planned.

For hints that don't match any of the 8 deterministic patterns, the system will invoke an LLM as a fallback. Key design constraints that are already locked:

- LLM receives the **email body only** — never user secrets
- LLM returns a **structured PasswordRule JSON** — never a password directly
- LLM is **stateless and single-call** per email
- If the LLM is unavailable or returns an invalid rule, the failure is `INVALID_RULE` (terminal) — no silent fallback

### Dockerized Execution

**Status**: Deferred to Phase 4.

The project plan describes a Dockerized deployment as part of the Phase 4 demo readiness pass. The single-user CLI model is self-contained and does not require containerization for local use, but a Dockerfile would simplify setup on a new machine and align with the portfolio narrative.

---

## Troubleshooting

### Authentication Failure

**Error**: `AUTHENTICATE failed` or `LOGIN failed`

**Solution**:
1. Verify `EMAIL_APP_PASSWORD` in `.env` is the 16-character App Password, not your regular Gmail password
2. Confirm IMAP is enabled: Gmail → Settings → See all settings → Forwarding and POP/IMAP → Enable IMAP
3. Ensure 2-Step Verification is active on your Google Account (required for App Passwords)

### No Emails Fetched

**Cause**: Checkpoint file marks everything as already processed.

**Solution**: Delete `data/email_fetch_checkpoint.txt` to reset incremental fetching. The next run will look back up to `EMAIL_LOOKBACK_CAP_DAYS` days.

```bash
rm data/email_fetch_checkpoint.txt
```

### pikepdf Installation Fails

**Error**: `Failed building wheel for pikepdf`

**Solution**: Install system dependencies first.

```bash
# macOS
brew install qpdf

# Ubuntu / Debian
sudo apt-get install libqpdf-dev

# Then retry
pip install pikepdf
```

### PDF Not Decrypted Despite Correct Hint

**Cause**: The rule engine generated candidates in the wrong case or format.

**Solution**: Run with `--verbose` to see all generated candidates and the hint that was extracted:

```bash
python main.py --verbose
```

Look for log lines containing `candidates` and `hint` to compare generated passwords against what the email describes.

### Environment Variables Not Loaded

**Error**: `EMAIL_USERNAME environment variable not set`

**Solution**: Confirm `.env` exists in the project root (not a parent directory) and that `python-dotenv` is installed:

```bash
ls -la .env
pip show python-dotenv
```

### Retryable Email Not Being Retried

**Cause**: Retry count reached the maximum (3 attempts) or the failure reason is terminal.

**Solution**: Check the SQLite database to inspect the current status:

```bash
sqlite3 data/bill_decryption.db "SELECT uid, status, failure_reason, retry_count FROM email ORDER BY id DESC LIMIT 20;"
```

If `retry_count` is 3 or the `failure_reason` is terminal (e.g. `NO_PASSWORD_HINT_FOUND`), the email will not be retried. Update your user profile if the issue is `REQUIRED_USER_DATA_MISSING`, then reset the record's status manually if needed.

### Decrypted PDF Missing From Output Directory

**Cause**: Decryption succeeded but output directory does not exist or has permission issues.

**Solution**: Ensure the output directory is writable and create it if missing:

```bash
mkdir -p output/decrypted
python main.py --verbose
```

---

## Security Notes

- **Never commit `.env`** — it contains your Gmail App Password. It is listed in `.gitignore`.
- **Revoke and regenerate App Passwords** if you suspect exposure. Each App Password can be revoked independently from your Google Account security settings.
- **Decrypted PDFs are saved locally** and never transmitted. They are written to the output directory only.
- **Encrypted PDFs are never exposed** — if decryption fails, the encrypted file is not saved or returned.
- **User secrets are stored minimally** — only the last 4 digits of your card and last 8 digits of your account number are used. Full card or account numbers are never required.
- All credentials must come from environment variables. Hardcoding credentials in source code is explicitly prohibited.
