"""Meleon - Adaptive OCR data extraction library for ALTO and PageXML formats."""

__version__ = "0.3.0"

from .main import (
    parse,
    serialize,
)
from .batch import (
    batch_process,
    read_parquet_batches,
)
from .config import (
    BatchProcessorConfig,
    ProcessingConfig,
    StreamingConfig,
    ParquetConfig,
    DatasetConfig,
)
from .processors import (
    StreamingBatchProcessor,
    HybridProcessor,
    AdaptiveProcessor,
)
from .parsers import ALTOParser, PageXMLParser
from .serializers import ALTOSerializer, PageXMLSerializer
from . import schemas

__all__ = [
    "parse",
    "serialize",
    "schemas",
    # Parsers and Serializers for dependency injection
    "ALTOParser",
    "PageXMLParser",
    "ALTOSerializer",
    "PageXMLSerializer",
    # Batch processing
    "batch_process",
    "read_parquet_batches",
    # Configuration
    "BatchProcessorConfig",
    "ProcessingConfig",
    "StreamingConfig",
    "ParquetConfig",
    "DatasetConfig",
    # Processors
    "StreamingBatchProcessor",
    "HybridProcessor",
    "AdaptiveProcessor",
]
