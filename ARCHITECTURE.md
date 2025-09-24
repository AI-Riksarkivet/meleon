# XML Parser Architecture & Technical Design

## Overview

The XML Parser is a high-performance, schema-driven system for parsing ALTO and PageXML formats into PyArrow tables, optimized for large-scale batch processing with minimal memory footprint. It provides bidirectional XML processing (parsing and serialization) with cross-library compatibility through Narwhals integration.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          INPUT LAYER                            │
├─────────────────────────┬───────────────────────────────────────┤
│   XML Files             │        PyArrow Schema                 │
│   • ALTO                │        • Field definitions            │
│   • PageXML             │        • Data types                  │
│   • Auto-detection      │        • Metadata preservation       │
└────────────┬────────────┴──────────────┬────────────────────────┘
             │                           │
             ▼                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CORE PROCESSING                            │
├─────────────────────────────────────────────────────────────────┤
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    │
│   │ Format       │───▶│  Main API    │───▶│   Batch      │    │
│   │ Parsers      │    │  (main.py)   │    │  Processor   │    │
│   │ •ALTOParser  │    │              │    │  (batch.py)  │    │
│   │ •PageXMLParser    │  •parse()    │    │              │    │
│   └──────────────┘    │  •serialize()│    └──────────────┘    │
│                       └──────────────┘                         │
│   ┌──────────────┐    ┌──────────────┐                        │
│   │ Serializers  │    │  Converters  │                        │
│   │ •ALTOSerial. │    │ •Narwhals    │                        │
│   │ •PageXMLSer. │    │  Adapter     │                        │
│   └──────────────┘    └──────────────┘                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        OUTPUT LAYER                             │
├──────────────────┬────────────────────┬────────────────────────┤
│  PyArrow Table   │   Parquet Files    │   RecordBatch Stream   │
│  (In-Memory)     │   (Disk Storage)   │   (Streaming)          │
│                  │   •Single File     │                        │
│                  │   •Partitioned DS  │                        │
└──────────────────┴────────────────────┴────────────────────────┘
```

## Module Organization

```
src/meleon/
├── __init__.py              # Public API exports
├── main.py                  # Core parsing/serialization functions
├── batch.py                 # Batch processing with streaming support
├── config.py                # Pydantic configuration models
├── processors.py            # Streaming, parallel, and hybrid processors
├── cli.py                   # Typer CLI implementation
├── schemas.py               # PyArrow schema definitions (ALTO, PageXML, Metadata)
├── utils.py                 # Utility functions & helpers
├── parsers/                 # Parsing implementations
│   ├── __init__.py
│   ├── base.py             # Abstract base parser (BaseParser)
│   ├── alto.py             # ALTO XML parser implementation
│   └── pagexml.py          # PageXML parser implementation
├── serializers/             # Serialization implementations
│   ├── __init__.py
│   ├── base.py             # Abstract base serializer (BaseSerializer)
│   ├── alto.py             # ALTO XML serializer
│   └── pagexml.py          # PageXML serializer
└── converters/              # Data transformation utilities
    ├── __init__.py
    └── narwhals_adapter.py  # Cross-library compatibility (Polars/Pandas/PyArrow)
```

## Component Architecture

```
Parser Components                   Serializer Components
┌─────────────────┐                ┌─────────────────┐
│   BaseParser    │                │ BaseSerializer  │
│   (Abstract)    │                │   (Abstract)    │
│ •detect_format()│                │ •serialize()    │
│ •parse()        │                │ (template-based)│
└────────┬────────┘                └────────┬────────┘
         │                                  │
    ┌────┴────┐                        ┌───┴───┐
    │         │                        │       │
┌───▼──┐  ┌──▼────┐              ┌────▼──┐ ┌─▼──────┐
│ ALTO │  │PageXML│              │ ALTO  │ │PageXML │
│Parser│  │Parser │              │Serial.│ │Serial. │
└──────┘  └───────┘              └───────┘ └────────┘

Batch Processing Components         Data Converters
┌────────────────────────┐         ┌────────────────────┐
│    BatchProcessor      │         │  NarwhalsAdapter   │
│ •to_reader()          │         │ •to_narwhals()     │
│ •to_table()           │         │ •from_narwhals()   │
│ •to_parquet()         │         │ •transform()       │
│ •to_dataset()         │         └────────────────────┘
│ •process_batches()    │
└────────────────────────┘
```

## Data Flow Architecture

```
Single File Processing:
───────────────────────────────────────────────────────────
XML File ──▶ Parser ──▶ PyArrow Table ──▶ [Optional: Narwhals] ──▶ Serializer ──▶ XML
                           │
                           └─ Auto-tracks source_file

Batch Processing (Memory-Efficient):
───────────────────────────────────────────────────────────
                    ┌─────────────────────┐
File List ──▶ Iterator ──▶ Accumulate ──▶│ 10K rows? │──▶ Yield Batch
    ▲                         │          └─────────────┘        │
    │                         │                                 ▼
    │                         └──── No ◀────────────────  Stream/Table/
    │                                                       Parquet
    └──────────── Next File ◀──────────────────────────────────┘

Cross-Library Processing:
───────────────────────────────────────────────────────────
PyArrow Table ──▶ Narwhals ──▶ [Polars/Pandas/PyArrow] ──▶ Transform ──▶ PyArrow Table
```

## Memory Management Strategy

```
┌──────────────────────────────────────────────────────────┐
│              MEMORY-EFFICIENT PROCESSING                  │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  1M XML Files                          Memory Usage       │
│       │                                    < 1GB          │
│       ▼                                      ▲            │
│  ┌──────────┐                               │            │
│  │Generator │ ◀─────────────┐               │            │
│  │  (Lazy)  │              │                │            │
│  └─────┬────┘              │                │            │
│        │                   │                │            │
│        ▼                   │                │            │
│  ┌───────────┐             │          ┌─────┴────┐       │
│  │   10K     │─── Process ─┴───────▶  │  Fixed   │       │
│  │   Rows    │                        │  Buffer  │       │
│  │   Batch   │                        └──────────┘       │
│  └─────┬─────┘                                           │
│        │                                                  │
│        ▼                                                  │
│  ┌────────────┐                                          │
│  │ Incremental│ (Stream write to Parquet)                │
│  │   Write    │ (No accumulation in memory)              │
│  └────────────┘                                          │
│                                                           │
└───────────────────────────────────────────────────────────┘

Parallel Processing Architecture:
┌──────────────────────────────────────────────────────────┐
│              PARALLEL FILE PROCESSING                     │
├────────────────────────────────────────────────────────────┤
│                                                           │
│  File Chunks ──▶ ThreadPoolExecutor                      │
│      │               │                                    │
│  [F1-F1000] ────▶ Worker 1 ──▶ Parse ──▶ Table          │
│  [F1001-F2000] ─▶ Worker 2 ──▶ Parse ──▶ Table          │
│  [F2001-F3000] ─▶ Worker 3 ──▶ Parse ──▶ Table          │
│      ...            ...         ...       │              │
│                                           ▼              │
│                                    Stream Write          │
│                                    (Incremental)         │
└──────────────────────────────────────────────────────────┘
```

## Class Hierarchy & Responsibilities

```
                    BaseParser (Abstract)
                         │
                         ├─ Attributes:
                         │   • schema (PyArrow)
                         │   • level (word/line/region)
                         │
                         ├─ Methods:
                         │   • detect_format() → str
                         │   • parse(file) → pa.Table
                         │   • _extract_elements() (abstract)
                         │
            ┌────────────┴────────────┐
            │                         │
      ALTOParser                PageXMLParser
            │                         │
      Implements:              Implements:
      • ALTO XML parsing       • PageXML parsing
      • HPOS/VPOS/WIDTH/HEIGHT • Coordinate points
      • Style references       • Baseline extraction
      • Namespace handling     • Bounding box calc


                    BaseSerializer (Abstract)
                         │
                         ├─ Attributes:
                         │   • template_path
                         │   • level
                         │
                         ├─ Methods:
                         │   • serialize(table, template) → str
                         │   • _update_elements() (abstract)
                         │
            ┌────────────┴────────────┐
            │                         │
      ALTOSerializer           PageXMLSerializer
            │                         │
      Template-based:          Template-based:
      • Updates existing XML   • Updates existing XML
      • Preserves metadata     • Preserves structure
      • ID-based matching      • ID-based matching


                    BatchProcessor
                         │
                         ├─ Attributes:
                         │   • file_paths: List[Path]
                         │   • schema: pa.Schema
                         │   • level: str
                         │
                         ├─ Methods:
                         │   • to_reader(batch_size) → RecordBatchReader
                         │   • to_table() → pa.Table
                         │   • to_parquet(path, compression)
                         │   • to_dataset(path, partitioning)
                         │   • process_batches() → Iterator[pa.RecordBatch]
```

## Schema Definitions

```
┌────────────────────────────────────────────────────────────┐
│                     ALTO Schema                           │
├────────────────────────────────────────────────────────────┤
│  Hierarchical IDs:          │  Geometry & Attributes:     │
│  • page_id      : string    │  • x       : int32         │
│  • region_id    : string    │  • y       : int32         │
│  • line_id      : string    │  • width   : int32         │
│  • word_id      : string    │  • height  : int32         │
│  • text         : string    │  • confidence : float32    │
│                             │  • style_refs : string     │
└────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────┐
│                    PageXML Schema                         │
├────────────────────────────────────────────────────────────┤
│  Hierarchical IDs:          │  Coordinates:              │
│  • page_id      : string    │  • coords   : string       │
│  • region_id    : string    │  • baseline : string       │
│  • line_id      : string    │  • x, y, width, height    │
│  • word_id      : string    │  • confidence : float32    │
│  • text         : string    │  • region_type : string    │
└────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────┐
│                  Metadata Schema                          │
├────────────────────────────────────────────────────────────┤
│  Document Metadata:                                       │
│  • xml_declaration : struct<version, encoding, standalone>│
│  • namespaces     : list<struct<prefix, uri>>           │
│  • metadata       : string (JSON)                        │
│  • custom_elements: list<struct<name, attrs, text>>      │
│                                                           │
│  Processing Info (Auto-Added):                           │
│  • source_file    : string                               │
│  • processing_time: timestamp                            │
└────────────────────────────────────────────────────────────┘
```

## API Design

### Main API Functions (main.py)

```python
# Core parsing with dependency injection
def parse(file_path: str, parser: BaseParser) -> pa.Table:
    """Parse XML file using configured parser instance"""

# Template-based serialization
def serialize(data_table: pa.Table, serializer: BaseSerializer) -> str:
    """Serialize PyArrow table back to XML using template"""

# Convenience functions with auto-detection
def parse_alto(file_path: str, level: str = "word") -> pa.Table
def parse_pagexml(file_path: str, level: str = "word") -> pa.Table
```

### Batch Processing API (batch.py)

```python
# Memory-efficient batch processing with streaming
def batch_process(files, parser, output_path=None, batch_size=10000,
                 streaming=True, config=None):
    """Process files with streaming or batch mode"""

# Parquet I/O with PyArrow integration
def read_parquet_batches(path, batch_size, columns, filters):
    """Read Parquet with column projection and filter pushdown"""
```

### Streaming Processors API (processors.py)

```python
class StreamingBatchProcessor:
    """Memory-efficient streaming batch processor"""
    def stream_to_parquet(output_path) -> int
    def parallel_process() -> Iterator[RecordBatch]
    def process_with_memory_limit() -> Iterator[Table]
    def stream_transform_serialize(transformer, serializer, output_dir) -> int
    def to_lazy_dataset(output_path) -> Dataset

class HybridProcessor(StreamingBatchProcessor):
    """Hybrid processor combining parallel and streaming"""
    def process() -> Iterator[RecordBatch]

class AdaptiveProcessor(HybridProcessor):
    """Processor that adapts to system resources"""
```

### Configuration API (config.py)

```python
class ProcessingConfig(BaseModel):
    batch_file_size: int  # Files to process in parallel
    batch_row_size: int   # Rows per batch
    shard_size: int       # Rows per Parquet shard
    max_workers: int      # Parallel workers
    processing_mode: Literal["sequential", "parallel", "streaming", "hybrid"]

class BatchProcessorConfig(BaseModel):
    processing: ProcessingConfig
    streaming: StreamingConfig
    parquet: ParquetConfig
    dataset: DatasetConfig
```

### Cross-Library Support (converters/narwhals_adapter.py)

```python
# Narwhals integration for dataframe interoperability
def to_narwhals(table: pa.Table) -> nw.DataFrame
def from_narwhals(df: nw.DataFrame) -> pa.Table
def transform(table: pa.Table, func: Callable) -> pa.Table
```

## Performance Characteristics

```
┌────────────────────┬──────────────┬─────────────────┐
│    Operation       │ Memory Usage │   Throughput    │
├────────────────────┼──────────────┼─────────────────┤
│ Stream 1M files    │    < 1GB     │  ~10K files/min │
│ Parallel Process   │  O(workers)  │  ~50K files/min │
│ Parse to Table     │     O(n)     │  ~50K rows/sec  │
│ Stream to Parquet  │  O(batch)    │  ~100K rows/sec │
│ Incremental Write  │  O(1)        │  ~200K rows/sec │
│ Read Parquet       │  O(batch)    │  ~500K rows/sec │
│ Dataset Filter     │     O(1)     │     Instant     │
│ Narwhals Transform │   O(batch)   │  Library-dependent│
└────────────────────┴──────────────┴─────────────────┘

Processing Modes:
─────────────────────────────────────────────────────
         Mode              Memory         Use Case
─────────────────────────────────────────────────────
    ┌──────────┐
    │Streaming │          O(1)         Millions of files
    │(Iterator)│                       Real-time processing
    └──────────┘

    ┌──────────┐
    │  Batch   │        O(batch)       Medium datasets
    │(10K rows)│                       Controlled memory
    └──────────┘

    ┌──────────┐
    │  Table   │          O(n)         Small datasets
    │(In-memory)│                      Fast access
    └──────────┘

    ┌──────────┐
    │ Dataset  │        O(1)          Partitioned data
    │(Lazy eval)│                      Query optimization
    └──────────┘
```

## Technical Design Patterns

### 1. Dependency Injection Pattern
```python
# Parser configured once, used multiple times
parser = ALTOParser(schema=ALTO_SCHEMA, level="word")
for file in files:
    table = parse(file, parser)
```

### 2. Generator Pattern
```python
# Memory-efficient streaming
def parse_batch_iterator():
    for file in files:
        batch = parse(file)
        if accumulated >= batch_size:
            yield batch  # Memory released after yield
```

### 3. Template Method Pattern
```python
# Base class defines algorithm, subclasses implement steps
class BaseParser:
    def parse(self, file):
        root = self._parse_xml(file)
        elements = self._extract_elements(root)  # Abstract
        return self._create_table(elements)
```

### 4. Adapter Pattern
```python
# Narwhals adapter for cross-library compatibility
def transform_with_polars(arrow_table):
    nw_df = to_narwhals(arrow_table)
    # Use Polars/Pandas/PyArrow transparently
    result = nw_df.filter(nw.col("confidence") > 0.9)
    return from_narwhals(result)
```

### 5. Factory Pattern
```python
# Auto-detect and create appropriate parser
def detect_format(file_path) -> BaseParser:
    if "alto" in str(file_path).lower():
        return ALTOParser(schema, level)
    elif "page" in str(file_path).lower():
        return PageXMLParser(schema, level)
```

## Key Design Decisions

```
┌──────────────────────┬────────────────────────────────────┐
│     Decision         │           Rationale                │
├──────────────────────┼────────────────────────────────────┤
│ PyArrow as core      │ • Native Parquet support           │
│ data structure       │ • Zero-copy operations             │
│                      │ • C++ performance                  │
├──────────────────────┼────────────────────────────────────┤
│ Schema-driven        │ • Type safety                      │
│ extraction           │ • Selective field extraction       │
│                      │ • Predictable output               │
├──────────────────────┼────────────────────────────────────┤
│ Template-based       │ • Preserves document metadata      │
│ serialization        │ • Maintains XML structure          │
│                      │ • Simpler implementation           │
├──────────────────────┼────────────────────────────────────┤
│ Narwhals            │ • Cross-library compatibility      │
│ integration          │ • User choice of dataframe lib    │
│                      │ • Future-proof design              │
├──────────────────────┼────────────────────────────────────┤
│ Batch by rows,       │ • Consistent memory usage          │
│ not files           │ • Handles varying file sizes       │
│                      │ • Predictable performance          │
├──────────────────────┼────────────────────────────────────┤
│ 10K row default      │ • Balance memory vs I/O            │
│ batch size          │ • Optimized for typical workloads  │
│                      │ • User configurable                │
├──────────────────────┼────────────────────────────────────┤
│ Dependency          │ • Clear separation of concerns     │
│ injection           │ • Testable components              │
│                      │ • Flexible configuration           │
└──────────────────────┴────────────────────────────────────┘
```

## Error Handling Strategy

```
                   ┌─────────┐
    XML File ──▶   │  Parse  │
                   └────┬────┘
                        │
                   ┌────▼────┐
                   │Validate │ (Format detection)
                   └────┬────┘
                        │
           ┌────────────┼────────────┐
           ▼                         ▼
       ┌───────┐                ┌────────┐
       │  Valid│                │ Invalid│
       └───┬───┘                └────┬───┘
           │                         │
           ▼                         ▼
     ┌──────────┐             ┌──────────┐
     │Extract   │             │Log Error │
     │Elements  │             │Return    │
     └─────┬────┘             │Empty     │
           │                  └─────┬────┘
           ▼                         │
     ┌──────────┐                    │
     │Create    │                    │
     │PyArrow   │                    │
     │Table     │                    │
     └─────┬────┘                    │
           │                         │
           └──────────┬──────────────┘
                      ▼
                ┌──────────┐
                │ Continue │──▶ Next File
                │Processing│
                └──────────┘

Batch Processing Error Resilience:
─────────────────────────────────────
• Individual file failures don't stop batch
• Failed files logged with details
• Empty batches handled gracefully
• Partial results preserved
```

## Scalability Architecture

```
Vertical Scaling:
─────────────────────────────────────────
    PyArrow Threading ──▶ Parallel I/O
                           │
                      ┌────┴────┐
                      │Thread 1 │ (Parquet write)
                      │Thread 2 │ (Compression)
                      │Thread N │ (Column encoding)
                      └─────────┘

    More Memory ──▶ Larger Batches ──▶ Better Throughput


Horizontal Scaling (via Partitioned Datasets):
─────────────────────────────────────────
                 ┌──────────────┐
    Dataset ──▶  │  Partitioned │
                 │   by page_id  │
                 └───────┬──────┘
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
        ┌──────┐    ┌──────┐    ┌──────┐
        │Part 1│    │Part 2│    │Part N│
        └──────┘    └──────┘    └──────┘
         Process     Process     Process
         Parallel    Parallel    Parallel


Storage Optimization:
─────────────────────────────────────────
    Partitioned ──▶ Fast Queries
     Parquet        (Predicate Pushdown)
                    (Column Projection)

    Compression ──▶ Reduced Storage
     (Snappy)       (~70% reduction)

    Columnar ──▶ Efficient Analytics
     Format      (Only read needed columns)
```

## API Usage Examples

```python
# 1. Simple parsing with auto-detection
from xml_parser import parse_alto, parse_pagexml
table = parse_alto("document.xml", level="word")

# 2. Batch processing with streaming
from xml_parser import BatchProcessor, schemas
processor = BatchProcessor(files, schemas.ALTO_SCHEMA, level="line")
for batch in processor.to_reader(batch_size=5000):
    process(batch)  # Process 5000 rows at a time

# 3. Cross-library processing with Narwhals
from xml_parser.converters import NarwhalsAdapter
adapter = NarwhalsAdapter()
polars_df = adapter.to_narwhals(arrow_table)
# Use Polars operations...
result = adapter.from_narwhals(polars_df)

# 4. Serialization with template
from xml_parser import ALTOSerializer, serialize
serializer = ALTOSerializer(template_path="original.xml")
xml_output = serialize(modified_table, serializer)

# 5. Partitioned dataset creation
processor.to_dataset(
    "output_dataset/",
    partitioning=["page_id"],
    max_rows_per_file=100000
)

# 6. Lazy dataset operations
dataset = read_parquet_lazy("dataset/")
scanner = dataset.scanner(
    columns=["text", "confidence"],
    filter=pc.field("confidence") > 0.9
)
for batch in scanner.to_batches():
    # Only matching data is loaded
```

## Current Limitations & Future Improvements

### Implementation Features
1. **Streaming Write**: Incremental Parquet writing without memory accumulation
2. **Parallel Processing**: Concurrent file processing with ThreadPoolExecutor
3. **Adaptive Processing**: Automatic adjustment to system resources
4. **Configuration**: Comprehensive Pydantic-based configuration system
5. **CLI**: Full-featured Typer CLI for all operations
6. **Memory Efficient**: True streaming with configurable memory limits

### Current Limitations
1. **Serialization**: Cannot generate XML from scratch (requires template)
2. **Format Support**: Limited to ALTO and PageXML formats
3. **Validation**: Basic format detection, no XSD schema validation
4. **Error Granularity**: Limited per-element error reporting in batch mode

### Architectural Strengths
1. **Clean Separation**: Well-defined module boundaries and responsibilities
2. **Type Safety**: Schema-driven approach with PyArrow strong typing
3. **Memory Efficiency**: Streaming architecture for unlimited scale
4. **Extensibility**: Abstract base classes for new format support
5. **Interoperability**: Narwhals integration for ecosystem compatibility
6. **Performance**: Direct PyArrow operations avoid Python overhead

### Extension Points

```
┌─────────────────────────────────────────────────────┐
│              Extension Points                       │
├─────────────────────────────────────────────────────┤
│                                                     │
│  New Formats:  BaseParser ◀─── METSParser          │
│                BaseSerializer ◀─── TEISerializer    │
│                                                     │
│  Custom Schema: user_schema = pa.schema([          │
│                    ("custom_field", pa.string()),  │
│                    ("custom_score", pa.float32())  │
│                ])                                  │
│                                                     │
│  Cloud Storage: dataset = ds.dataset(              │
│                    "s3://bucket/path",             │
│                    format="parquet"                │
│                )                                   │
│                                                     │
│  Distributed:  ray.data.read_parquet(              │
│                    processor.to_parquet()          │
│                ).map_batches(transform)            │
│                                                     │
└─────────────────────────────────────────────────────┘
```

## Performance Optimization Techniques

1. **Schema Projection**: Only extract required fields from XML
2. **Batch Accumulation**: Process multiple small files per batch
3. **Filter Pushdown**: Apply predicates at Parquet read time
4. **Column Pruning**: Read only necessary columns from storage
5. **Compression**: Use Snappy for fast compression/decompression
6. **Partitioning**: Organize data by frequently filtered columns
7. **Direct Construction**: Build PyArrow tables without intermediate Python objects