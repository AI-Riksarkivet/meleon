"""Service layer for business logic separation."""

import logging
from pathlib import Path
from typing import List, Optional, Union

import pyarrow as pa
import pyarrow.parquet as pq

from .config import BatchProcessorConfig
from .parsers import ALTOParser, PageXMLParser
from .parsers.base import BaseParser
from .processors import AdaptiveProcessor, StreamingBatchProcessor
from .schemas import ALTO_SCHEMA, PAGEXML_SCHEMA
from .serializers import ALTOSerializer, PageXMLSerializer
from .serializers.base import BaseSerializer

logger = logging.getLogger(__name__)


class ParserFactory:
    """Factory for creating parser instances."""

    @staticmethod
    def create_parser(format_type: str, schema: pa.Schema, level: str = "word") -> BaseParser:
        """
        Create a parser instance based on format type.

        Args:
            format_type: Type of parser ("alto", "pagexml", "auto")
            schema: PyArrow schema for extraction
            level: Extraction level ("word", "line", "region")

        Returns:
            Configured parser instance

        Raises:
            ValueError: If format_type is not supported
        """
        parsers = {
            "alto": ALTOParser,
            "pagexml": PageXMLParser,
        }

        parser_class = parsers.get(format_type.lower())
        if not parser_class:
            raise ValueError(f"Unsupported format type: {format_type}")

        return parser_class(schema, level)

    @staticmethod
    def auto_detect_format(file_path: Union[str, Path]) -> str:
        """
        Auto-detect file format based on file content or name.

        Args:
            file_path: Path to XML file

        Returns:
            Detected format type ("alto" or "pagexml")
        """
        file_path = Path(file_path)

        # Try content-based detection first
        alto_parser = ALTOParser(ALTO_SCHEMA, "word")
        if alto_parser.detect_format(str(file_path)):
            return "alto"

        pagexml_parser = PageXMLParser(PAGEXML_SCHEMA, "word")
        if pagexml_parser.detect_format(str(file_path)):
            return "pagexml"

        # Fall back to filename-based detection
        if "alto" in str(file_path).lower():
            return "alto"

        return "pagexml"


class SerializerFactory:
    """Factory for creating serializer instances."""

    @staticmethod
    def create_serializer(format_type: str, source_xml: Optional[str] = None) -> BaseSerializer:
        """
        Create a serializer instance based on format type.

        Args:
            format_type: Type of serializer ("alto", "pagexml")
            source_xml: Optional source XML for structure preservation

        Returns:
            Configured serializer instance

        Raises:
            ValueError: If format_type is not supported
        """
        serializers = {
            "alto": ALTOSerializer,
            "pagexml": PageXMLSerializer,
        }

        serializer_class = serializers.get(format_type.lower())
        if not serializer_class:
            raise ValueError(f"Unsupported format type: {format_type}")

        return serializer_class(source_xml)


class ProcessingService:
    """Service for document processing operations."""

    def __init__(self, config: Optional[BatchProcessorConfig] = None):
        """Initialize processing service with configuration."""
        self.config = config or BatchProcessorConfig()

    def parse_single_file(
        self,
        input_file: Path,
        format_type: str = "auto",
        level: str = "word",
    ) -> pa.Table:
        """
        Parse a single XML file to PyArrow table.

        Args:
            input_file: Path to XML file
            format_type: Format type or "auto" for detection
            level: Extraction level

        Returns:
            PyArrow table with extracted data
        """
        if format_type == "auto":
            format_type = ParserFactory.auto_detect_format(input_file)

        schema = ALTO_SCHEMA if format_type == "alto" else PAGEXML_SCHEMA
        parser = ParserFactory.create_parser(format_type, schema, level)

        return parser.parse(str(input_file))

    def batch_process_files(
        self,
        files: List[Union[str, Path]],
        output_path: Path,
        format_type: str = "auto",
        level: str = "word",
        mode: str = "streaming",
    ) -> int:
        """
        Process multiple files in batch mode.

        Args:
            files: List of XML files to process
            output_path: Output path for Parquet file/directory
            format_type: Format type or "auto" for detection
            level: Extraction level
            mode: Processing mode

        Returns:
            Total number of rows processed
        """
        if not files:
            raise ValueError("No files to process")

        if format_type == "auto":
            format_type = ParserFactory.auto_detect_format(files[0])

        schema = ALTO_SCHEMA if format_type == "alto" else PAGEXML_SCHEMA
        parser = ParserFactory.create_parser(format_type, schema, level)

        self.config.processing.processing_mode = mode

        processor = StreamingBatchProcessor(files, parser, self.config)
        return processor.stream_to_parquet(output_path)

    def stream_process_with_memory_limit(
        self,
        files: List[Union[str, Path]],
        output_dir: Path,
        format_type: str = "auto",
        level: str = "word",
        memory_limit_mb: int = 1024,
        adaptive: bool = True,
    ) -> int:
        """
        Stream process files with memory limits.

        Args:
            files: List of XML files to process
            output_dir: Output directory for shards
            format_type: Format type or "auto" for detection
            level: Extraction level
            memory_limit_mb: Memory limit in MB
            adaptive: Use adaptive processing

        Returns:
            Total number of rows processed
        """
        if not files:
            raise ValueError("No files to process")

        if format_type == "auto":
            format_type = ParserFactory.auto_detect_format(files[0])

        schema = ALTO_SCHEMA if format_type == "alto" else PAGEXML_SCHEMA
        parser = ParserFactory.create_parser(format_type, schema, level)

        self.config.processing.memory_limit_mb = memory_limit_mb
        self.config.processing.processing_mode = "streaming"
        self.config.streaming.incremental_write = True

        processor_class = AdaptiveProcessor if adaptive else StreamingBatchProcessor
        processor = processor_class(files, parser, self.config)

        return processor.stream_to_parquet(output_dir)


class TransformationService:
    """Service for data transformation operations."""

    @staticmethod
    def transform_parquet(
        input_parquet: Path,
        output_path: Path,
        min_confidence: float = 0.0,
        columns: Optional[List[str]] = None,
    ) -> int:
        """
        Transform Parquet data with filters and projections.

        Args:
            input_parquet: Input Parquet file or dataset
            output_path: Output Parquet file
            min_confidence: Minimum confidence threshold
            columns: Columns to select

        Returns:
            Number of rows in transformed dataset
        """
        import pyarrow.compute as pc
        import pyarrow.dataset as ds

        dataset = ds.dataset(input_parquet, format="parquet")

        filters = None
        if min_confidence > 0:
            filters = pc.field("confidence") >= min_confidence

        scanner = dataset.scanner(columns=columns, filter=filters)
        table = scanner.to_table()

        pq.write_table(table, str(output_path), compression="snappy")

        return table.num_rows


class StatsService:
    """Service for statistics and analysis."""

    @staticmethod
    def get_parquet_stats(parquet_file: Path) -> dict:
        """
        Get statistics for a Parquet file.

        Args:
            parquet_file: Path to Parquet file

        Returns:
            Dictionary with statistics
        """
        parquet_file_obj = pq.ParquetFile(str(parquet_file))
        metadata = parquet_file_obj.metadata

        total_size = sum(rg.total_byte_size for rg in metadata.row_groups)
        compressed_size = sum(rg.total_compressed_size for rg in metadata.row_groups)

        stats = {
            "num_rows": metadata.num_rows,
            "num_columns": metadata.num_columns,
            "num_row_groups": metadata.num_row_groups,
            "format_version": metadata.format_version,
            "created_by": metadata.created_by or "Unknown",
            "total_size_mb": total_size / (1024 * 1024),
            "compressed_size_mb": compressed_size / (1024 * 1024),
        }

        if total_size > 0:
            stats["compression_ratio"] = (1 - compressed_size / total_size) * 100

        return stats
