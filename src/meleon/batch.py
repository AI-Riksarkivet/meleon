"""Batch processing with streaming and parallel support."""

import logging
from pathlib import Path
from typing import Iterator, List, Union, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from .config import BatchProcessorConfig
from .parsers.base import BaseParser
from .processors import StreamingBatchProcessor

logger = logging.getLogger(__name__)


def batch_process(
    files: List[Union[str, Path]],
    parser: BaseParser,
    output_path: Optional[Union[str, Path]] = None,
    batch_size: int = 10000,
    streaming: bool = True,
    config: Optional[BatchProcessorConfig] = None,
):
    """
    Process files with streaming or batch mode.

    Args:
        files: List of file paths to process
        parser: Configured parser instance (e.g., ALTOParser(schema, level))
        output_path: If provided, write to Parquet; otherwise yield batches
        batch_size: Rows per batch/file for chunking
        streaming: If True, use streaming processor for memory efficiency
        config: Optional configuration object

    Returns:
        Iterator of RecordBatches if no output_path, None if writing to file
    """
    if streaming or config:
        if config is None:
            config = BatchProcessorConfig()
            config.processing.batch_row_size = batch_size

        processor = StreamingBatchProcessor(files, parser, config)

        if output_path:
            processor.stream_to_parquet(output_path)
            return None
        else:
            return processor._process_file_batches()

    def _process_generator():
        for file_path in files:
            try:
                table = parser.parse(str(file_path))
                if table.num_rows > 0:
                    source_array = pa.array([str(file_path)] * len(table))
                    table = table.append_column("source_file", source_array)

                    for batch in table.to_batches(max_chunksize=batch_size):
                        yield batch
            except Exception as e:
                logger.error(f"Error parsing {file_path}: {e}")

    if output_path and not streaming:
        writer = None
        schema = None

        try:
            for file_path in files:
                try:
                    table = parser.parse(str(file_path))
                    if table.num_rows > 0:
                        source_array = pa.array([str(file_path)] * len(table))
                        table = table.append_column("source_file", source_array)

                        if writer is None:
                            schema = table.schema
                            output_path = Path(output_path)

                            if output_path.suffix == ".parquet":
                                writer = pq.ParquetWriter(
                                    str(output_path), schema, compression="snappy"
                                )

                        if writer and output_path.suffix == ".parquet":
                            writer.write_table(table)

                except Exception as e:
                    logger.error(f"Error parsing {file_path}: {e}")
        finally:
            if writer is not None:
                writer.close()

        return None
    else:
        return _process_generator()


def read_parquet_batches(
    parquet_path: Union[str, Path],
    batch_size: int = 10000,
    columns: Optional[List[str]] = None,
    filters: Optional[List] = None,
) -> Iterator[pa.RecordBatch]:
    """
    Read Parquet file(s) in batches using PyArrow's native capabilities.

    Args:
        parquet_path: Path to Parquet file or dataset directory
        batch_size: Rows per batch
        columns: Specific columns to read
        filters: PyArrow filter expressions for pushdown

    Returns:
        Iterator of RecordBatches
    """
    dataset = ds.dataset(parquet_path, format="parquet")
    scanner = dataset.scanner(columns=columns, filter=filters, batch_size=batch_size)

    for batch in scanner.to_batches():
        yield batch
