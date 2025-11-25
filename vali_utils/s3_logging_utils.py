"""
Rich table logging utilities for S3 validation results.
Provides formatted, easy-to-read tables for S3 validation metrics.
"""

from rich.table import Table
from rich.console import Console
from typing import Optional
import bittensor as bt

from vali_utils.s3_utils import S3ValidationResult

console = Console()


def format_size(bytes_value: int) -> str:
    """Format bytes into human-readable size"""
    if bytes_value == 0:
        return "0 B"

    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0
    size = float(bytes_value)

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"


def format_percentage_with_status(
    value: float,
    threshold: float,
    inverse: bool = False,
    show_threshold: bool = True
) -> str:
    """
    Format percentage with color-coded status indicator

    Args:
        value: The percentage value
        threshold: The threshold for pass/fail
        inverse: If True, lower is better (e.g., duplicates)
        show_threshold: Whether to show the threshold in output
    """
    if inverse:
        # Lower is better (duplicates)
        passed = value <= threshold
        status = "✅" if passed else "❌"
        threshold_text = f"under {threshold}%" if show_threshold else ""
    else:
        # Higher is better (job match, scraper)
        passed = value >= threshold
        status = "✅" if passed else "❌"
        threshold_text = f"meets {threshold}%" if show_threshold else ""

    if show_threshold and threshold_text:
        return f"{value:.1f}% {status} ({threshold_text})"
    else:
        return f"{value:.1f}% {status}"


def log_s3_validation_table(
    result: S3ValidationResult,
    uid: int,
    hotkey: str,
    pagination_stats: Optional[dict] = None
):
    """
    Display S3 validation results in a rich table format

    Args:
        result: S3ValidationResult object with validation metrics
        uid: Miner UID
        hotkey: Miner hotkey
        pagination_stats: Optional dict with pagination info (pages, time, etc.)
    """

    # Create main table
    table = Table(
        title=f"[bold cyan]S3 Validation Results - UID {uid}[/bold cyan]",
        show_header=True,
        header_style="bold magenta",
        border_style="bright_blue",
        title_style="bold cyan",
        show_lines=True,
        expand=False,
        width=80
    )

    table.add_column("Metric", style="cyan", width=20)
    table.add_column("Value", style="white", width=56)

    # Overall Status
    status_emoji = "✅" if result.is_valid else "❌"
    status_text = "PASSED" if result.is_valid else "FAILED"
    status_color = "green" if result.is_valid else "red"

    table.add_row(
        "Status",
        f"[{status_color}]{status_emoji} {status_text} ({result.validation_percentage:.1f}%)[/{status_color}]"
    )

    # Miner Info
    table.add_row("Hotkey", f"{hotkey[:20]}...")

    # Job Statistics
    if hasattr(result, 'enhanced_validation') and result.enhanced_validation:
        enhanced = result.enhanced_validation
        total_jobs = enhanced.total_active_jobs
        # Get expected jobs count if available (approximate from validation percentage)
        job_completion_pct = (total_jobs / 15 * 100) if total_jobs > 0 else 0  # Assuming ~15 expected jobs
        table.add_row(
            "Jobs Found",
            f"{total_jobs} active jobs ({job_completion_pct:.0f}% completion)"
        )
    else:
        table.add_row("Jobs Found", f"{result.job_count} jobs")

    # File Statistics
    table.add_row("Total Files", f"{result.total_files:,} files")
    table.add_row("Total Size", format_size(result.total_size_bytes))

    if result.recent_files > 0:
        table.add_row("Recent Activity", f"{result.recent_files:,} files (last 3 hours)")

    # Add separator
    table.add_row("", "")
    table.add_row("[bold]Validation Metrics[/bold]", "")
    table.add_row("", "")

    # Quality Metrics
    if result.quality_metrics:
        dup_pct = result.quality_metrics.get('duplicate_percentage', 0)
        job_match = result.quality_metrics.get('job_match_rate', 0)
        scraper_rate = result.quality_metrics.get('scraper_success_rate', 0)

        # Duplicates (lower is better, threshold 10%)
        if dup_pct > 0:
            dup_status = format_percentage_with_status(dup_pct, 10.0, inverse=True)
            table.add_row("Duplicates", dup_status)

        # Job Match Rate (higher is better, threshold 95%)
        if job_match > 0:
            job_match_status = format_percentage_with_status(job_match, 95.0, inverse=False)
            table.add_row("Job Match Rate", job_match_status)

        # Scraper Success (higher is better, threshold 80%)
        if scraper_rate > 0:
            scraper_status = format_percentage_with_status(scraper_rate, 80.0, inverse=False)
            table.add_row("Scraper Success", scraper_status)

    # Enhanced validation details
    if hasattr(result, 'enhanced_validation') and result.enhanced_validation:
        enhanced = result.enhanced_validation

        # Show entities validated
        if enhanced.entities_validated > 0:
            table.add_row(
                "Entities Validated",
                f"{enhanced.entities_passed_scraper}/{enhanced.entities_validated} passed"
            )

        # Show job match details
        if enhanced.entities_checked_for_job_match > 0:
            table.add_row(
                "Job Content Match",
                f"{enhanced.entities_matched_job}/{enhanced.entities_checked_for_job_match} matched"
            )

    # Pagination stats if provided
    if pagination_stats:
        table.add_row("", "")
        table.add_row("[bold]Pagination Stats[/bold]", "")
        table.add_row("", "")

        if 'pages' in pagination_stats:
            table.add_row("Pages Retrieved", f"{pagination_stats['pages']} pages")

        if 'files_per_page' in pagination_stats:
            table.add_row("Avg Files/Page", f"~{pagination_stats['files_per_page']:,} files")

        if 'retrieval_time' in pagination_stats:
            table.add_row("Retrieval Time", f"{pagination_stats['retrieval_time']:.1f}s")

    # Issues (if any)
    if not result.is_valid and result.issues:
        table.add_row("", "")
        table.add_row("[bold red]Issues[/bold red]", "")
        table.add_row("", "")

        for i, issue in enumerate(result.issues[:3]):  # Show max 3 issues
            table.add_row(f"Issue {i+1}", f"[red]{issue}[/red]")

    # Reason
    if result.reason:
        table.add_row("", "")
        reason_style = "green" if result.is_valid else "yellow"
        table.add_row("Summary", f"[{reason_style}]{result.reason}[/{reason_style}]")

    # Print the table
    console.print(table)
    console.print()  # Add spacing after table


def log_s3_validation_compact(
    result: S3ValidationResult,
    uid: int,
    hotkey: str
):
    """
    Display S3 validation results in a compact single-line format
    Useful for batch processing or quick summaries

    Args:
        result: S3ValidationResult object
        uid: Miner UID
        hotkey: Miner hotkey
    """
    status_emoji = "✅" if result.is_valid else "❌"
    status_color = "green" if result.is_valid else "red"

    # Get key metrics
    dup_pct = result.quality_metrics.get('duplicate_percentage', 0) if result.quality_metrics else 0
    job_match = result.quality_metrics.get('job_match_rate', 0) if result.quality_metrics else 0
    scraper_rate = result.quality_metrics.get('scraper_success_rate', 0) if result.quality_metrics else 0

    size_str = format_size(result.total_size_bytes)

    console.print(
        f"[{status_color}]{status_emoji} UID {uid:3d} | {hotkey[:12]}... | "
        f"{result.validation_percentage:5.1f}% | "
        f"Jobs: {result.job_count:2d} | Files: {result.total_files:5,} ({size_str}) | "
        f"Dup: {dup_pct:4.1f}% | Match: {job_match:5.1f}% | Scraper: {scraper_rate:5.1f}%[/{status_color}]"
    )


def log_s3_validation_header():
    """Print a header for compact validation logs"""
    console.print("[bold cyan]" + "="*100 + "[/bold cyan]")
    console.print("[bold cyan]S3 Validation Results[/bold cyan]")
    console.print("[bold cyan]" + "="*100 + "[/bold cyan]")
    console.print()
