"""Command-line interface for Meleon using Typer."""

import logging
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from . import __version__
from .config import BatchProcessorConfig
from .services import (
    ProcessingService,
    StatsService,
    TransformationService,
)

app = typer.Typer(
    name="meleon",
    help="🦎 Adaptive OCR data extraction - Transform XML to PyArrow/Parquet",
    add_completion=False,
)
console = Console()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.command()
def parse(
    input_file: Path = typer.Argument(..., help="XML file to parse"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output Parquet file"),
    format: str = typer.Option("auto", "--format", "-f", help="Format: alto, pagexml, auto"),
    level: str = typer.Option("word", "--level", "-l", help="Extraction level: word, line, region"),
    show_stats: bool = typer.Option(False, "--stats", "-s", help="Show statistics"),
):
    """Parse a single XML file to PyArrow/Parquet."""
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Parsing XML file...", total=None)
        service = ProcessingService()

        try:
            table = service.parse_single_file(input_file, format, level)
            progress.update(task, description=f"Parsed {table.num_rows} {level}s")

            if output:
                import pyarrow.parquet as pq

                pq.write_table(table, str(output), compression="snappy")
                console.print(f"[green]✓[/green] Wrote {table.num_rows} rows to {output}")

            if show_stats:
                stats_table = Table(title="Parse Statistics")
                stats_table.add_column("Metric", style="cyan")
                stats_table.add_column("Value", style="magenta")

                stats_table.add_row("Total rows", str(table.num_rows))
                stats_table.add_row("Columns", str(len(table.column_names)))
                stats_table.add_row("Memory usage", f"{table.nbytes / (1024 * 1024):.2f} MB")

                console.print(stats_table)

        except Exception as e:
            console.print(f"[red]✗[/red] Error: {e}")
            raise typer.Exit(code=1)


@app.command()
def batch(
    input_dir: Path = typer.Argument(..., help="Directory with XML files"),
    output: Path = typer.Argument(..., help="Output Parquet file or directory"),
    pattern: str = typer.Option("*.xml", "--pattern", "-p", help="File pattern to match"),
    format: str = typer.Option("auto", "--format", "-f", help="Format: alto, pagexml, auto"),
    level: str = typer.Option("word", "--level", "-l", help="Extraction level: word, line, region"),
    batch_file_size: int = typer.Option(1000, "--batch-files", help="Files to process in parallel"),
    batch_row_size: int = typer.Option(10000, "--batch-rows", help="Rows per batch"),
    shard_size: int = typer.Option(100000, "--shard-size", help="Rows per shard"),
    max_workers: Optional[int] = typer.Option(None, "--workers", "-w", help="Max parallel workers"),
    mode: str = typer.Option(
        "streaming", "--mode", "-m", help="Processing mode: streaming, parallel, hybrid"
    ),
    compression: str = typer.Option(
        "snappy", "--compression", "-c", help="Compression: snappy, gzip, lz4"
    ),
):
    """Batch process XML files to Parquet with streaming."""
    files = list(input_dir.glob(pattern))

    if not files:
        console.print(f"[yellow]No files matching pattern '{pattern}' in {input_dir}[/yellow]")
        raise typer.Exit(code=1)

    console.print(f"[cyan]Found {len(files)} files to process[/cyan]")

    config = BatchProcessorConfig()
    config.processing.batch_file_size = batch_file_size
    config.processing.batch_row_size = batch_row_size
    config.processing.shard_size = shard_size
    config.processing.max_workers = max_workers
    # Cast to proper literal types
    from typing import cast
    from typing import Literal as L

    config.processing.processing_mode = cast(
        L["sequential", "parallel", "streaming", "hybrid"], mode
    )
    config.processing.compression = cast(
        L["snappy", "gzip", "brotli", "lz4", "zstd", "none"], compression
    )

    service = ProcessingService(config)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Processing {len(files)} files...", total=len(files))

        try:
            total_rows = service.batch_process_files(files, output, format, level, mode)
            progress.update(task, completed=len(files))

            console.print(f"[green]✓[/green] Processed {len(files)} files")
            console.print(f"[green]✓[/green] Wrote {total_rows:,} rows to {output}")

        except Exception as e:
            console.print(f"[red]✗[/red] Error: {e}")
            raise typer.Exit(code=1)


@app.command()
def stream(
    input_dir: Path = typer.Argument(..., help="Directory with XML files"),
    output_dir: Path = typer.Argument(..., help="Output directory for shards"),
    pattern: str = typer.Option("*.xml", "--pattern", "-p", help="File pattern"),
    format: str = typer.Option("auto", "--format", "-f", help="Format: alto, pagexml, auto"),
    level: str = typer.Option("word", "--level", "-l", help="Extraction level"),
    shard_size: int = typer.Option(100000, "--shard-size", help="Rows per shard"),
    memory_limit: int = typer.Option(1024, "--memory-limit", help="Memory limit in MB"),
    adaptive: bool = typer.Option(
        True, "--adaptive/--no-adaptive", help="Adapt to system resources"
    ),
):
    """Stream process with memory limits and sharding."""
    files = list(input_dir.glob(pattern))

    if not files:
        console.print(f"[yellow]No files matching '{pattern}'[/yellow]")
        raise typer.Exit(code=1)

    console.print(f"[cyan]Streaming {len(files)} files with {memory_limit}MB limit[/cyan]")

    config = BatchProcessorConfig()
    config.processing.shard_size = shard_size
    service = ProcessingService(config)

    try:
        total_rows = service.stream_process_with_memory_limit(
            files, output_dir, format, level, memory_limit, adaptive
        )
        console.print(f"[green]✓[/green] Streamed {total_rows:,} rows to {output_dir}")

    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def transform(
    input_parquet: Path = typer.Argument(..., help="Input Parquet file or dataset"),
    output: Path = typer.Argument(..., help="Output Parquet file"),
    min_confidence: float = typer.Option(
        0.8, "--min-confidence", help="Minimum confidence threshold"
    ),
    columns: Optional[List[str]] = typer.Option(None, "--columns", "-c", help="Columns to select"),
):
    """Transform Parquet data with filters and projections."""
    console.print(f"[cyan]Transforming {input_parquet}...[/cyan]")

    try:
        num_rows = TransformationService.transform_parquet(
            input_parquet, output, min_confidence, columns
        )
        console.print(f"[green]✓[/green] Transformed {num_rows:,} rows to {output}")

    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(code=1)


@app.command()
def config(
    output: Path = typer.Argument(..., help="Output config file (YAML)"),
    preset: str = typer.Option("default", "--preset", "-p", help="Preset: default, memory, speed"),
):
    """Generate configuration file with presets."""
    config = BatchProcessorConfig()

    if preset == "memory":
        config.processing.batch_file_size = 100
        config.processing.batch_row_size = 1000
        config.processing.memory_limit_mb = 512
        config.processing.processing_mode = "streaming"
    elif preset == "speed":
        config.processing.batch_file_size = 5000
        config.processing.batch_row_size = 50000
        config.processing.processing_mode = "parallel"
        config.processing.max_workers = 16

    config.save_yaml(output)
    console.print(f"[green]✓[/green] Generated config at {output} with preset '{preset}'")


@app.command()
def version():
    """Show version information."""
    console.print(f"Meleon version {__version__}")


@app.command()
def stats(
    parquet_file: Path = typer.Argument(..., help="Parquet file to analyze"),
):
    """Show statistics for a Parquet file or dataset."""
    try:
        stats = StatsService.get_parquet_stats(parquet_file)

        stats_table = Table(title="Parquet Statistics")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="magenta")

        stats_table.add_row("Number of rows", f"{stats['num_rows']:,}")
        stats_table.add_row("Number of columns", str(stats["num_columns"]))
        stats_table.add_row("Number of row groups", str(stats["num_row_groups"]))
        stats_table.add_row("Format version", stats["format_version"])
        stats_table.add_row("Created by", stats["created_by"])
        stats_table.add_row("Total size", f"{stats['total_size_mb']:.2f} MB")
        stats_table.add_row("Compressed size", f"{stats['compressed_size_mb']:.2f} MB")

        if "compression_ratio" in stats:
            stats_table.add_row("Compression ratio", f"{stats['compression_ratio']:.1f}%")

        console.print(stats_table)

    except Exception as e:
        console.print(f"[red]✗[/red] Error: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
