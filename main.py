"""main.py — CLI entry point for the automated bill decryption pipeline."""

import logging
import sys

import typer
from typer import Option

from orchestrator import EmailResult, run_pipeline

app = typer.Typer(add_completion=False)


@app.command()
def run(
    output_dir: str = Option(
        "output/decrypted",
        "--output-dir",
        "-o",
        help="Directory where decrypted PDFs are saved.",
    ),
    profile: str = Option(
        "data/user_profile.json",
        "--profile",
        "-p",
        help="Path to the user profile JSON file.",
    ),
    verbose: bool = Option(
        False,
        "--verbose",
        "-v",
        help="Enable DEBUG-level logging.",
    ),
) -> None:
    """Run the bill decryption pipeline against unprocessed Gmail messages."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    results = run_pipeline(output_dir=output_dir, profile_path=profile)

    if not results:
        typer.echo("No unprocessed emails found.")
        raise typer.Exit(code=0)

    _print_summary(results)

    all_failed = all(r.status == "failure" for r in results)
    raise typer.Exit(code=1 if all_failed else 0)


def _print_summary(results: list[EmailResult]) -> None:
    """Print a formatted summary table to stdout."""
    width = 72
    typer.echo(f"\n{'=' * width}")
    typer.echo(f"{'UID':<12} {'SENDER':<28} {'STATUS':<10} FAILURE_REASON")
    typer.echo(f"{'-' * width}")
    for r in results:
        failure = r.failure_reason or ""
        typer.echo(f"{r.uid:<12} {r.sender:<28} {r.status:<10} {failure}")
        for pdf in r.pdf_results:
            pdf_failure = pdf.failure_reason or ""
            typer.echo(
                f"  {'':10} {pdf.filename:<26} {pdf.status:<10} {pdf_failure}"
                f" (tried {pdf.candidates_tried})"
            )
    typer.echo(f"{'=' * width}")
    success_count = sum(1 for r in results if r.status == "success")
    failed_count = len(results) - success_count
    typer.echo(
        f"Total: {len(results)}  |  Success: {success_count}  |  Failed: {failed_count}"
    )


if __name__ == "__main__":
    app()
