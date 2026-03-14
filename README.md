# Automated Bill Decryption Agent

A backend-first GenAI-powered agent that automates downloading, decrypting, and organizing password-protected billing statements.

## Architecture
![Pipeline Flow](docs/architecture_flow.png)

## Gmail IMAP Setup

1. Enable IMAP in the Gmail account settings.
2. Turn on 2-Step Verification for the Gmail account.
3. Create a Gmail app password for mail access.
4. Create the `bill-processed` Gmail label.
5. Export these environment variables before running the ingestion pipeline:
   - `EMAIL_FETCHER_IMAP_HOST`
   - `EMAIL_FETCHER_USERNAME`
   - `EMAIL_FETCHER_APP_PASSWORD`
   - `EMAIL_FETCHER_MAILBOX` (optional, defaults to `INBOX`)
   - `EMAIL_FETCHER_PROCESSED_LABEL` (optional, defaults to `bill-processed`)
6. Call `mark_email_processed(uid)` only after a message completes the full success path.
