# 🦎 Meleon

<div align="center">
  <img width="300" alt="melo2" src="https://github.com/user-attachments/assets/43ad718d-4507-4575-96b7-cb28c2b2dab0" />
</div>

**Adaptive OCR data extraction library for ALTO and PageXML formats**

## Overview

Meleon transforms OCR/HTR data from XML formats (ALTO, PageXML) into PyArrow tables and Parquet files. It provides streaming processing for large-scale document collections with configurable memory usage.

## Features

- **Streaming Processing**: Process large collections with configurable batch sizes
- **Bidirectional Conversion**: Parse XML to PyArrow and serialize back to XML
- **Multiple Extraction Levels**: Extract at word, line, or region level
- **Parallel Processing**: Multi-threaded batch processing
- **Adaptive Processing**: Automatically adjusts to system resources
- **Cross-Library Support**: Narwhals integration for Pandas/Polars compatibility
- **CLI Interface**: Command-line tools for common operations

## Installation

```bash
pip install meleon

# With CLI support
pip install meleon[cli]

# For development
pip install meleon[dev]
```

## Usage

### Python API

```python
import meleon
from meleon import ALTOParser, PageXMLParser
from meleon.schemas import ALTO_SCHEMA, PAGEXML_SCHEMA

# Parse single file
parser = ALTOParser(ALTO_SCHEMA, level="word")
table = meleon.parse("document.xml", parser)

# Batch process to Parquet
from meleon import batch_process

files = ["doc1.xml", "doc2.xml", "doc3.xml"]
batch_process(
    files,
    parser,
    output_path="output.parquet",
    batch_size=10000,
    streaming=True
)

# Serialize back to XML
from meleon import ALTOSerializer

serializer = ALTOSerializer(source_xml="original.xml")
xml_output = meleon.serialize(table, serializer)
```

### CLI

```bash
# Parse single file
meleon parse document.xml --output data.parquet --format alto --level word

# Batch process directory
meleon batch ./xml_files/ ./output.parquet --pattern "*.xml" --workers 8

# Stream process with memory limit
meleon stream ./xml_files/ ./output_shards/ --memory-limit 512 --shard-size 100000

# Transform with filters
meleon transform input.parquet output.parquet --min-confidence 0.9

# Show statistics
meleon stats data.parquet
```

## Processing Modes

### Streaming Mode
```python
from meleon import StreamingBatchProcessor, BatchProcessorConfig

config = BatchProcessorConfig()
config.processing.memory_limit_mb = 512
config.processing.shard_size = 100000

processor = StreamingBatchProcessor(files, parser, config)
processor.stream_to_parquet("output_dir/")
```

### Parallel Mode
```python
config.processing.processing_mode = "parallel"
config.processing.max_workers = 8
processor = StreamingBatchProcessor(files, parser, config)
```

### Adaptive Mode
```python
from meleon import AdaptiveProcessor

processor = AdaptiveProcessor(files, parser)
processor.stream_to_parquet("output.parquet")
```

## Data Schemas

### ALTO Schema
- `page_id`, `region_id`, `line_id`, `word_id`: Hierarchical identifiers
- `text`: Extracted text content
- `x`, `y`, `width`, `height`: Bounding box coordinates
- `confidence`: OCR confidence score
- `style_refs`: Style references

### PageXML Schema
- `page_id`, `region_id`, `line_id`, `word_id`: Hierarchical identifiers
- `text`: Extracted text content
- `coords`: Polygon coordinates
- `baseline`: Text baseline
- `confidence`: OCR confidence score

## Architecture

```
XML Files (ALTO/PageXML)
    ↓
Parsers (Schema-driven extraction)
    ↓
Processing (Stream/Batch/Parallel)
    ↓
Output (PyArrow Table/Parquet/Dataset)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical design.

## License

Apache License 2.0
