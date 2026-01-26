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
        threshold_text = f"max {threshold}%" if show_threshold else ""
    else:
        # Higher is better (job match, scraper)
        passed = value >= threshold
        status = "✅" if passed else "❌"
        threshold_text = f"min {threshold}%" if show_threshold else ""

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
        width=90
    )

    table.add_column("Metric", style="cyan", width=22)
    table.add_column("Value", style="white", width=64)

    # Overall Status
    status_emoji = "✅" if result.is_valid else "❌"
    status_text = "PASSED" if result.is_valid else "FAILED"
    status_color = "green" if result.is_valid else "red"

    table.add_row(
        "Status",
        f"[{status_color}]{status_emoji} {status_text}[/{status_color}]"
    )

    # Miner Info
    table.add_row("Hotkey", f"{hotkey[:20]}...")

    # Job Statistics
    if hasattr(result, 'enhanced_validation') and result.enhanced_validation:
        enhanced = result.enhanced_validation
        total_jobs = enhanced.total_active_jobs
        expected_jobs = enhanced.expected_jobs_count
        # Calculate correct job completion percentage
        job_completion_pct = (total_jobs / expected_jobs * 100) if expected_jobs > 0 else 0
        table.add_row(
            "Jobs Coverage",
            f"{total_jobs}/{expected_jobs} active jobs ({job_completion_pct:.1f}% coverage)"
        )
    else:
        table.add_row("Jobs Found", f"{result.job_count} jobs")

    # File Statistics
    table.add_row("Files Sampled", f"{result.total_files:,} files (10% random sample)")
    table.add_row("Total Size", format_size(result.total_size_bytes))

    # NEW: Competition Scoring (effective_size and coverage)
    if hasattr(result, 'effective_size_bytes') and hasattr(result, 'job_coverage_rate'):
        coverage = result.job_coverage_rate
        effective_size = result.effective_size_bytes

        table.add_row("", "")
        table.add_row("[bold]Competition Scoring[/bold]", "")
        table.add_row("", "")

        # Coverage with squared penalty explanation
        coverage_penalty = (coverage / 100.0) ** 2
        table.add_row(
            "Job Coverage",
            f"{coverage:.1f}% (penalty: {coverage_penalty:.2f}x)"
        )

        # Effective size (what counts for competition)
        if effective_size > 0:
            table.add_row(
                "Effective Size",
                f"[green]{format_size(int(effective_size))}[/green] = {format_size(result.total_size_bytes)} × {coverage_penalty:.2f}"
            )
        else:
            table.add_row(
                "Effective Size",
                f"[red]0 B[/red] (validation failed)"
            )

    # Add separator
    table.add_row("", "")
    table.add_row("[bold]Validation Checks[/bold]", "")
    table.add_row("", "")

    # Quality Metrics with updated thresholds
    if result.quality_metrics:
        dup_pct = result.quality_metrics.get('duplicate_percentage', 0)
        empty_pct = result.quality_metrics.get('empty_content_rate', 0)
        job_match = result.quality_metrics.get('job_match_rate', 0)
        scraper_rate = result.quality_metrics.get('scraper_success_rate', 0)

        # Duplicates (lower is better, threshold 5%)
        dup_status = format_percentage_with_status(dup_pct, 5.0, inverse=True)
        table.add_row("Duplicate Rate", dup_status)

        # Empty content (lower is better, threshold 10%)
        if empty_pct > 0:
            empty_status = format_percentage_with_status(empty_pct, 10.0, inverse=True)
            table.add_row("Empty Content", empty_status)

        # Job Match Rate (higher is better, threshold 95%)
        if job_match > 0:
            job_match_status = format_percentage_with_status(job_match, 95.0, inverse=False)
            table.add_row("Job Match Rate", job_match_status)

        # Scraper Success (higher is better, threshold 70%)
        if scraper_rate > 0:
            scraper_status = format_percentage_with_status(scraper_rate, 70.0, inverse=False)
            table.add_row("Scraper Success", scraper_status)

    # Enhanced validation details
    if hasattr(result, 'enhanced_validation') and result.enhanced_validation:
        enhanced = result.enhanced_validation

        # Show entities validated
        if enhanced.entities_validated > 0:
            passed_color = "green" if enhanced.entities_passed_scraper >= enhanced.entities_validated * 0.7 else "yellow"
            table.add_row(
                "Entities Validated",
                f"[{passed_color}]{enhanced.entities_passed_scraper}/{enhanced.entities_validated} passed[/{passed_color}]"
            )

        # Show job match details
        if enhanced.entities_checked_for_job_match > 0:
            match_color = "green" if enhanced.entities_matched_job >= enhanced.entities_checked_for_job_match * 0.95 else "yellow"
            table.add_row(
                "Job Content Match",
                f"[{match_color}]{enhanced.entities_matched_job}/{enhanced.entities_checked_for_job_match} matched[/{match_color}]"
            )

        # NEW: Show scraper validation details (why entities passed/failed)
        if hasattr(enhanced, 'sample_validation_results') and enhanced.sample_validation_results:
            table.add_row("", "")
            table.add_row("[bold]Scraper Details[/bold]", "")

            for i, detail in enumerate(enhanced.sample_validation_results):  # Show all validated
                # Format: "✅ x: Good job!" or "❌ reddit: titles don't match"
                table.add_row(f"  Entity {i+1}", detail)

        # NEW: Show job mismatch samples if any
        if hasattr(enhanced, 'sample_job_mismatches') and enhanced.sample_job_mismatches:
            table.add_row("", "")
            table.add_row("[bold yellow]Job Mismatches[/bold yellow]", "")

            for i, mismatch in enumerate(enhanced.sample_job_mismatches[:3]):  # Show max 3
                table.add_row(f"  Mismatch {i+1}", f"[yellow]{mismatch}[/yellow]")

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
        table.add_row("[bold red]Issues Found[/bold red]", "")
        table.add_row("", "")

        for i, issue in enumerate(result.issues[:5]):  # Show max 5 issues
            table.add_row(f"  Issue {i+1}", f"[red]{issue}[/red]")

    # Summary
    if result.reason:
        table.add_row("", "")
        reason_style = "green" if result.is_valid else "yellow"
        table.add_row("Summary", f"[{reason_style}]{result.reason}[/{reason_style}]")

    # Print the table
    console.print(table)
    console.print()  # Add spacing after table
