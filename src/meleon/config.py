"""Configuration models for batch processing using Pydantic."""

from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class ProcessingConfig(BaseModel):
    """Configuration for batch processing operations."""

    batch_file_size: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Number of files to process in parallel",
    )

    batch_row_size: int = Field(
        default=10_000,
        ge=100,
        le=1_000_000,
        description="Rows per batch for streaming",
    )

    shard_size: int = Field(
        default=100_000,
        ge=1000,
        description="Rows per Parquet shard file",
    )

    max_workers: Optional[int] = Field(
        default=None,
        ge=1,
        le=128,
        description="Max parallel workers (None = CPU count)",
    )

    memory_limit_mb: int = Field(
        default=1024,
        ge=128,
        le=65536,
        description="Target memory usage in MB",
    )

    compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd", "none"] = Field(
        default="snappy",
        description="Parquet compression codec",
    )

    processing_mode: Literal["sequential", "parallel", "streaming", "hybrid"] = Field(
        default="streaming",
        description="Processing strategy",
    )

    @field_validator("max_workers")
    @classmethod
    def validate_max_workers(cls, v):
        if v is None:
            import os

            return min(32, (os.cpu_count() or 1))
        return v

    @field_validator("batch_row_size")
    @classmethod
    def validate_batch_size_ratio(cls, v, info):
        if "shard_size" in info.data and v > info.data["shard_size"]:
            raise ValueError(
                f"batch_row_size ({v}) must be <= shard_size ({info.data['shard_size']})"
            )
        return v

    model_config = {"validate_assignment": True, "extra": "forbid"}


class StreamingConfig(BaseModel):
    """Configuration specific to streaming operations."""

    buffer_size: int = Field(
        default=10,
        ge=1,
        le=1000,
        description="Number of batches to buffer",
    )

    checkpoint_interval: int = Field(
        default=10000,
        ge=1000,
        description="Rows between checkpoints",
    )

    enable_backpressure: bool = Field(
        default=True,
        description="Slow down if downstream can't keep up",
    )

    incremental_write: bool = Field(
        default=True,
        description="Write data incrementally as it arrives",
    )


class ParquetConfig(BaseModel):
    """Parquet-specific configuration."""

    row_group_size: int = Field(
        default=50000,
        ge=1000,
        description="Rows per row group",
    )

    use_dictionary: bool = Field(
        default=True,
        description="Enable dictionary encoding",
    )

    compression_level: Optional[int] = Field(
        default=None,
        ge=1,
        le=22,
        description="Compression level (codec-specific)",
    )

    write_statistics: bool = Field(
        default=True,
        description="Write column statistics for query optimization",
    )

    use_legacy_dataset: bool = Field(
        default=False,
        description="Use legacy dataset API",
    )

    existing_data_behavior: Literal["error", "overwrite_or_ignore", "delete_matching"] = Field(
        default="overwrite_or_ignore",
        description="How to handle existing data",
    )


class DatasetConfig(BaseModel):
    """Configuration for partitioned dataset operations."""

    partitioning: Optional[list[str]] = Field(
        default=None,
        description="Columns to use for partitioning",
    )

    partitioning_flavor: Optional[str] = Field(
        default=None,
        description="Partitioning scheme (e.g., 'hive')",
    )

    max_partitions: int = Field(
        default=1024,
        ge=1,
        description="Maximum number of partitions",
    )

    max_open_files: int = Field(
        default=1024,
        ge=1,
        description="Maximum number of files open at once",
    )

    max_rows_per_file: int = Field(
        default=1_000_000,
        ge=1000,
        description="Maximum rows per file in dataset",
    )

    min_rows_per_group: int = Field(
        default=10_000,
        ge=100,
        description="Minimum rows per row group",
    )

    max_rows_per_group: int = Field(
        default=100_000,
        ge=1000,
        description="Maximum rows per row group",
    )


class BatchProcessorConfig(BaseModel):
    """Complete configuration for batch processor."""

    processing: ProcessingConfig = Field(
        default_factory=ProcessingConfig,
        description="General processing configuration",
    )

    streaming: StreamingConfig = Field(
        default_factory=StreamingConfig,
        description="Streaming-specific configuration",
    )

    parquet: ParquetConfig = Field(
        default_factory=ParquetConfig,
        description="Parquet output configuration",
    )

    dataset: DatasetConfig = Field(
        default_factory=DatasetConfig,
        description="Dataset configuration",
    )

    output_dir: Optional[Path] = Field(
        default=None,
        description="Output directory for results",
    )

    checkpoint_dir: Optional[Path] = Field(
        default=None,
        description="Directory for checkpoints/recovery",
    )

    @field_validator("output_dir", "checkpoint_dir")
    @classmethod
    def validate_paths(cls, v):
        if v is not None:
            v = Path(v)
            if not v.exists():
                v.mkdir(parents=True, exist_ok=True)
        return v

    def to_dict(self):
        return self.model_dump(exclude_unset=True)

    @classmethod
    def from_yaml(cls, path: Path):
        import yaml

        with open(path) as f:
            return cls(**yaml.safe_load(f))

    def save_yaml(self, path: Path):
        import yaml

        with open(path, "w") as f:
            yaml.dump(self.model_dump(), f)

    def get_parquet_write_options(self) -> dict:
        """Get PyArrow parquet write options from config."""
        return {
            "compression": self.parquet.compression_level or self.processing.compression,
            "compression_level": self.parquet.compression_level,
            "use_dictionary": self.parquet.use_dictionary,
            "write_statistics": self.parquet.write_statistics,
            "row_group_size": self.parquet.row_group_size,
        }

    def get_dataset_write_options(self) -> dict:
        """Get PyArrow dataset write options from config."""
        options = {
            "format": "parquet",
            "existing_data_behavior": self.parquet.existing_data_behavior,
            "use_threads": True,
            "max_partitions": self.dataset.max_partitions,
            "max_open_files": self.dataset.max_open_files,
            "max_rows_per_file": self.dataset.max_rows_per_file,
            "min_rows_per_group": self.dataset.min_rows_per_group,
            "max_rows_per_group": self.dataset.max_rows_per_group,
        }

        if self.dataset.partitioning:
            options["partitioning"] = self.dataset.partitioning
        if self.dataset.partitioning_flavor:
            options["partitioning_flavor"] = self.dataset.partitioning_flavor

        return options
