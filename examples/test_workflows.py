#!/usr/bin/env python3
"""Test all three processing workflows with Meleon."""

import sys
from pathlib import Path
import shutil
import tempfile

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as ds
from meleon import (
    ALTOParser,
    StreamingBatchProcessor,
    AdaptiveProcessor,
    BatchProcessorConfig,
    schemas,
)
from meleon.converters.narwhals_adapter import filter_by_confidence


def create_test_files(num_files: int = 10) -> list[Path]:
    """Create multiple test XML files for batch processing."""
    temp_dir = Path(tempfile.mkdtemp(prefix="meleon_test_"))
    files = []

    template = Path(__file__).parent / "sample_alto.xml"
    template_content = template.read_text()

    for i in range(num_files):
        file_path = temp_dir / f"document_{i:04d}.xml"
        content = template_content.replace("page001", f"page{i:03d}")
        content = content.replace("0.95", f"0.{80 + i % 20}")
        file_path.write_text(content)
        files.append(file_path)

    return files, temp_dir


def workflow_1_batch_to_parquet_shards():
    """Workflow 1: Million XML files → Batch process → Write Parquet shards."""
    print("\n=== Workflow 1: Batch → Parquet Shards ===")

    files, temp_dir = create_test_files(25)
    output_dir = temp_dir / "shards"

    config = BatchProcessorConfig()
    config.processing.batch_file_size = 5
    config.processing.batch_row_size = 10
    config.processing.shard_size = 20
    config.processing.processing_mode = "parallel"
    config.processing.max_workers = 2

    parser = ALTOParser(schemas.ALTO_SCHEMA, level="word")
    processor = StreamingBatchProcessor(files, parser, config)

    print(f"Processing {len(files)} files into shards...")
    total_rows = processor.stream_to_parquet(output_dir)
    print(f"✓ Wrote {total_rows} rows to shards")

    shard_files = list(output_dir.glob("*.parquet"))
    print(f"✓ Created {len(shard_files)} shard files")

    for shard in shard_files[:3]:
        table = pq.read_table(shard)
        print(f"  - {shard.name}: {table.num_rows} rows")

    dataset = ds.dataset(output_dir, format="parquet")
    print(f"\n✓ Created lazy dataset with {dataset.count_rows()} total rows")

    scanner = dataset.scanner(
        columns=["text", "confidence"],
        filter=pc.field("confidence") > 0.85
    )
    filtered_table = scanner.to_table()
    print(f"✓ Lazy query: {filtered_table.num_rows} rows with confidence > 0.85")

    shutil.rmtree(temp_dir)
    return True


def workflow_2_streaming_transform():
    """Workflow 2: Stream process → Transform → Serialize on the fly."""
    print("\n=== Workflow 2: Streaming Transform ===")

    files, temp_dir = create_test_files(10)
    output_parquet = temp_dir / "streamed.parquet"

    config = BatchProcessorConfig()
    config.processing.processing_mode = "streaming"
    config.streaming.incremental_write = True
    config.processing.batch_row_size = 5

    parser = ALTOParser(schemas.ALTO_SCHEMA, level="word")
    processor = StreamingBatchProcessor(files, parser, config)

    print(f"Stream processing {len(files)} files...")

    writer = None
    rows_processed = 0

    for batch in processor._process_file_batches():
        if writer is None:
            writer = pq.ParquetWriter(str(output_parquet), batch.schema)

        if "confidence" in batch.schema.names:
            mask = pc.greater(batch.column("confidence"), pa.scalar(0.9))
            filtered_batch = batch.filter(mask)
        else:
            filtered_batch = batch

        writer.write_batch(filtered_batch)
        rows_processed += filtered_batch.num_rows
        print(f"  Streamed batch: {filtered_batch.num_rows} rows (filtered)")

    if writer:
        writer.close()

    print(f"✓ Stream processed and transformed {rows_processed} rows")

    table = pq.read_table(output_parquet)
    print(f"✓ Final parquet has {table.num_rows} rows after filtering")

    shutil.rmtree(temp_dir)
    return True


def workflow_3_lazy_operations():
    """Workflow 3: Lazy dataset operations without loading all data."""
    print("\n=== Workflow 3: Lazy Dataset Operations ===")

    files, temp_dir = create_test_files(50)
    dataset_dir = temp_dir / "dataset"

    config = BatchProcessorConfig()
    config.dataset.partitioning = ["page_id"]
    config.dataset.max_rows_per_file = 20
    config.processing.batch_row_size = 10

    parser = ALTOParser(schemas.ALTO_SCHEMA, level="word")
    processor = AdaptiveProcessor(files, parser, config)

    print(f"Creating partitioned dataset from {len(files)} files...")
    lazy_dataset = processor.to_lazy_dataset(dataset_dir)

    print(f"✓ Created lazy dataset (not loaded in memory)")
    print(f"  Total rows: {lazy_dataset.count_rows()}")

    print("\nLazy operations (data not loaded):")

    high_confidence = lazy_dataset.scanner(
        filter=pc.field("confidence") > 0.95
    ).count_rows()
    print(f"  - High confidence words: {high_confidence}")

    projection = lazy_dataset.scanner(
        columns=["text", "confidence"]
    )
    print(f"  - Column projection defined (text, confidence)")

    aggregated = projection.to_table().group_by(["text"]).aggregate([
        ("confidence", "mean")
    ])
    print(f"  - Aggregated to {len(aggregated)} unique words")

    print("\nMemory-efficient batch iteration:")
    total_batches = 0
    total_rows = 0
    for batch in lazy_dataset.scanner(batch_size=10).to_batches():
        total_batches += 1
        total_rows += len(batch)
        if total_batches <= 3:
            print(f"  - Batch {total_batches}: {len(batch)} rows (only this in memory)")

    print(f"✓ Processed {total_rows} rows in {total_batches} batches")

    shutil.rmtree(temp_dir)
    return True


def workflow_4_parallel_with_narwhals():
    """Workflow 4: Parallel processing with Narwhals transformations."""
    print("\n=== Workflow 4: Parallel + Narwhals Transforms ===")

    files, temp_dir = create_test_files(20)
    output_file = temp_dir / "transformed.parquet"

    config = BatchProcessorConfig()
    config.processing.processing_mode = "hybrid"
    config.processing.max_workers = 4
    config.processing.batch_file_size = 5

    parser = ALTOParser(schemas.ALTO_SCHEMA, level="word")
    processor = StreamingBatchProcessor(files, parser, config)

    print(f"Parallel processing {len(files)} files with transformations...")

    import narwhals as nw

    writer = None
    for batch_table in processor.process_with_memory_limit():
        nw_df = nw.from_native(batch_table)

        filtered_df = nw_df.filter(nw.col("confidence") >= 0.85)

        transformed_df = filtered_df.with_columns(
            text_length=nw.col("text").str.len_chars()
        )

        result_table = nw.to_native(transformed_df)

        if writer is None:
            writer = pq.ParquetWriter(str(output_file), result_table.schema)
        writer.write_table(result_table)

        print(f"  Processed batch: {result_table.num_rows} rows after filter")

    if writer:
        writer.close()

    final_table = pq.read_table(output_file)
    print(f"✓ Transformed {final_table.num_rows} rows with Narwhals")
    print(f"✓ Added text_length column: {final_table.column_names}")

    shutil.rmtree(temp_dir)
    return True


def main():
    """Run all workflow tests."""
    print("🦎 Meleon Workflow Tests")
    print("=" * 50)

    workflows = [
        ("Batch → Parquet Shards", workflow_1_batch_to_parquet_shards),
        ("Stream Transform", workflow_2_streaming_transform),
        ("Lazy Dataset", workflow_3_lazy_operations),
        ("Parallel + Narwhals", workflow_4_parallel_with_narwhals),
    ]

    results = []
    for name, workflow_func in workflows:
        try:
            success = workflow_func()
            results.append((name, "✅ Success" if success else "❌ Failed"))
        except Exception as e:
            print(f"❌ Error in {name}: {e}")
            results.append((name, f"❌ Error: {str(e)[:50]}"))

    print("\n" + "=" * 50)
    print("SUMMARY:")
    for name, status in results:
        print(f"  {name}: {status}")

    all_success = all("✅" in status for _, status in results)
    print("\n🎉 All workflows working!" if all_success else "\n⚠️ Some workflows failed")

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())