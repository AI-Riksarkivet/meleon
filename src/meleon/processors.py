"""Streaming and parallel processors for batch operations."""

import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator, List, Optional, Union

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .config import BatchProcessorConfig
from .parsers.base import BaseParser

logger = logging.getLogger(__name__)


class StreamingBatchProcessor:
    """Memory-efficient streaming batch processor."""

    def __init__(
        self,
        files: List[Union[str, Path]],
        parser: BaseParser,
        config: Optional[BatchProcessorConfig] = None,
    ):
        self.files = [Path(f) for f in files]
        self.parser = parser
        self.config = config or BatchProcessorConfig()

    def stream_to_parquet(
        self,
        output_path: Union[str, Path],
    ) -> int:
        """
        Stream process files directly to Parquet without accumulating in memory.

        Returns:
            Total number of rows written
        """
        output_path = Path(output_path)
        total_rows = 0
        current_shard = 0
        rows_in_shard = 0
        writer = None

        try:
            for batch in self._process_file_batches():
                if batch.num_rows == 0:
                    continue

                if writer is None:
                    if output_path.suffix == ".parquet":
                        writer_path = output_path if current_shard == 0 else output_path.with_suffix(f".{current_shard}.parquet")
                    else:
                        output_path.mkdir(parents=True, exist_ok=True)
                        writer_path = output_path / f"shard_{current_shard:05d}.parquet"

                    writer = pq.ParquetWriter(
                        str(writer_path),
                        batch.schema,
                        **self._get_parquet_options(),
                    )

                writer.write_batch(batch)
                rows_in_shard += batch.num_rows
                total_rows += batch.num_rows

                if rows_in_shard >= self.config.processing.shard_size:
                    writer.close()
                    writer = None
                    current_shard += 1
                    rows_in_shard = 0
                    logger.info(f"Completed shard {current_shard - 1} with {rows_in_shard} rows")

        finally:
            if writer is not None:
                writer.close()

        logger.info(f"Processed {total_rows} total rows across {current_shard + 1} shards")
        return total_rows

    def parallel_process(self) -> Iterator[pa.RecordBatch]:
        """
        Process files in parallel and yield batches.

        Yields:
            PyArrow RecordBatches as they're processed
        """
        with ThreadPoolExecutor(max_workers=self.config.processing.max_workers) as executor:
            futures = []

            for chunk in self._chunk_files():
                future = executor.submit(self._process_file_chunk, chunk)
                futures.append(future)

                if len(futures) >= self.config.streaming.buffer_size:
                    for completed in as_completed(futures[:self.config.streaming.buffer_size]):
                        try:
                            table = completed.result()
                            if table and table.num_rows > 0:
                                for batch in table.to_batches(max_chunksize=self.config.processing.batch_row_size):
                                    yield batch
                        except Exception as e:
                            logger.error(f"Error processing chunk: {e}")
                    futures = futures[self.config.streaming.buffer_size:]

            for completed in as_completed(futures):
                try:
                    table = completed.result()
                    if table and table.num_rows > 0:
                        for batch in table.to_batches(max_chunksize=self.config.processing.batch_row_size):
                            yield batch
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")

    def process_with_memory_limit(self) -> Iterator[pa.Table]:
        """
        Process files while respecting memory limits.

        Yields:
            PyArrow Tables that fit within memory constraints
        """
        import psutil

        memory_limit_bytes = self.config.processing.memory_limit_mb * 1024 * 1024
        accumulated_tables = []
        current_memory = 0

        for file_path in self.files:
            try:
                table = self.parser.parse(str(file_path))
                if table.num_rows == 0:
                    continue

                source_array = pa.array([str(file_path)] * len(table))
                table = table.append_column("source_file", source_array)

                estimated_size = table.nbytes

                if current_memory + estimated_size > memory_limit_bytes and accumulated_tables:
                    combined = pa.concat_tables(accumulated_tables)
                    yield combined
                    accumulated_tables = []
                    current_memory = 0

                accumulated_tables.append(table)
                current_memory += estimated_size

            except Exception as e:
                logger.error(f"Error parsing {file_path}: {e}")

        if accumulated_tables:
            yield pa.concat_tables(accumulated_tables)

    def stream_transform_serialize(
        self,
        transformer,
        serializer,
        output_dir: Union[str, Path],
    ) -> int:
        """
        Stream process: parse -> transform -> serialize without accumulating.

        Args:
            transformer: Function to transform PyArrow tables
            serializer: Serializer instance for XML output
            output_dir: Directory for output XML files

        Returns:
            Number of files processed
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        processed = 0

        for file_path in self.files:
            try:
                table = self.parser.parse(str(file_path))

                if transformer:
                    table = transformer(table)

                xml_output = serializer.serialize(table)

                output_path = output_dir / f"{file_path.stem}_transformed.xml"
                output_path.write_text(xml_output)

                processed += 1

            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")

        return processed

    def to_lazy_dataset(self, output_path: Union[str, Path]) -> ds.Dataset:
        """
        Create a lazy dataset that can be queried without loading all data.

        Args:
            output_path: Path to write the dataset

        Returns:
            PyArrow Dataset for lazy operations
        """
        output_path = Path(output_path)

        ds.write_dataset(
            self._process_file_batches(),
            output_path,
            **self.config.get_dataset_write_options(),
        )

        return ds.dataset(output_path, format="parquet")

    def _process_file_batches(self) -> Iterator[pa.RecordBatch]:
        """Generate RecordBatches from files."""
        if self.config.processing.processing_mode == "parallel":
            yield from self.parallel_process()
        else:
            yield from self._sequential_process()

    def _sequential_process(self) -> Iterator[pa.RecordBatch]:
        """Process files sequentially."""
        accumulated_rows = []
        current_row_count = 0

        for file_path in self.files:
            try:
                table = self.parser.parse(str(file_path))
                if table.num_rows == 0:
                    continue

                source_array = pa.array([str(file_path)] * len(table))
                table = table.append_column("source_file", source_array)

                for batch in table.to_batches(max_chunksize=self.config.processing.batch_row_size):
                    yield batch

            except Exception as e:
                logger.error(f"Error parsing {file_path}: {e}")

    def _process_file_chunk(self, files: List[Path]) -> Optional[pa.Table]:
        """Process a chunk of files and return combined table."""
        tables = []
        for file_path in files:
            try:
                table = self.parser.parse(str(file_path))
                if table.num_rows > 0:
                    source_array = pa.array([str(file_path)] * len(table))
                    table = table.append_column("source_file", source_array)
                    tables.append(table)
            except Exception as e:
                logger.error(f"Error parsing {file_path}: {e}")

        return pa.concat_tables(tables) if tables else None

    def _chunk_files(self) -> Iterator[List[Path]]:
        """Chunk files for parallel processing."""
        batch_size = self.config.processing.batch_file_size
        for i in range(0, len(self.files), batch_size):
            yield self.files[i : i + batch_size]

    def _get_parquet_options(self) -> dict:
        """Get Parquet writer options from config."""
        return {
            k: v
            for k, v in self.config.get_parquet_write_options().items()
            if v is not None and k != "compression_level"
        }


class HybridProcessor(StreamingBatchProcessor):
    """Hybrid processor combining parallel and streaming approaches."""

    def process(self) -> Iterator[pa.RecordBatch]:
        """
        Process with optimal strategy based on config and resources.

        Yields:
            PyArrow RecordBatches
        """
        mode = self.config.processing.processing_mode

        if mode == "streaming":
            yield from self._sequential_process()
        elif mode == "parallel":
            yield from self.parallel_process()
        elif mode == "hybrid":
            yield from self._hybrid_process()
        else:
            yield from self._sequential_process()

    def _hybrid_process(self) -> Iterator[pa.RecordBatch]:
        """
        Hybrid approach: parallel processing with streaming write.

        Yields:
            PyArrow RecordBatches
        """
        with ThreadPoolExecutor(max_workers=self.config.processing.max_workers) as executor:
            for chunk in self._chunk_files():
                futures = [executor.submit(self.parser.parse, str(f)) for f in chunk]

                for future in as_completed(futures):
                    try:
                        table = future.result()
                        if table and table.num_rows > 0:
                            for batch in table.to_batches(max_chunksize=self.config.processing.batch_row_size):
                                if self.config.streaming.enable_backpressure:
                                    import time
                                    time.sleep(0.001)
                                yield batch
                    except Exception as e:
                        logger.error(f"Error in hybrid processing: {e}")


class AdaptiveProcessor(HybridProcessor):
    """Processor that adapts to system resources."""

    def __init__(
        self,
        files: List[Union[str, Path]],
        parser: BaseParser,
        config: Optional[BatchProcessorConfig] = None,
    ):
        super().__init__(files, parser, config)
        self._adapt_config()

    def _adapt_config(self):
        """Adapt configuration based on system resources."""
        try:
            import psutil

            available_memory_mb = psutil.virtual_memory().available // (1024 * 1024)
            cpu_count = psutil.cpu_count()

            if available_memory_mb < 1024:
                self.config.processing.batch_file_size = max(
                    100, self.config.processing.batch_file_size // 2
                )
                self.config.processing.batch_row_size = max(
                    1000, self.config.processing.batch_row_size // 2
                )
                logger.info(f"Adapted batch sizes for low memory: {available_memory_mb}MB")

            if self.config.processing.max_workers is None:
                self.config.processing.max_workers = min(
                    cpu_count, self.config.processing.batch_file_size // 10
                )

        except ImportError:
            logger.warning("psutil not available, using default configuration")