# Meleon Architecture & Technical Design

## Overview

Meleon is a schema-driven system for parsing ALTO and PageXML formats into PyArrow tables, optimized for batch processing with configurable memory usage. It provides bidirectional XML processing (parsing and serialization) with cross-library compatibility through Narwhals integration.

## System Architecture

```mermaid
flowchart TB
    subgraph Input["Input Layer"]
        XML["XML Files<br/>• ALTO<br/>• PageXML<br/>• Auto-detection"]
        Schema["PyArrow Schema<br/>• Field definitions<br/>• Data types<br/>• Metadata preservation"]
    end

    subgraph Core["Core Processing"]
        Parsers["Format Parsers<br/>• ALTOParser<br/>• PageXMLParser"]
        MainAPI["Main API<br/>• parse()<br/>• serialize()"]
        Batch["Batch Processor"]
        Serializers["Serializers<br/>• ALTOSerializer<br/>• PageXMLSerializer"]
        Converters["Converters<br/>• Narwhals Adapter"]
    end

    subgraph Output["Output Layer"]
        Table["PyArrow Table<br/>(In-Memory)"]
        Parquet["Parquet Files<br/>• Single File<br/>• Partitioned Dataset"]
        Stream["RecordBatch Stream<br/>(Streaming)"]
    end

    XML --> Parsers
    Schema --> Parsers
    Parsers --> MainAPI
    MainAPI --> Batch
    MainAPI --> Serializers
    Batch --> Converters

    Core --> Table
    Core --> Parquet
    Core --> Stream
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
├── services.py              # Service layer for business logic
├── utils.py                 # Utility functions & helpers
├── parsers/                 # Parsing implementations
│   ├── __init__.py
│   ├── base.py             # Abstract base parser (BaseParser)
│   ├── base_xml.py         # Base XML parser with common utilities
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

### Parser and Serializer Hierarchy

```mermaid
classDiagram
    class BaseParser {
        <<abstract>>
        +schema: pa.Schema
        +level: str
        +detect_format() bool
        +parse(file) pa.Table
    }

    class BaseXMLParser {
        <<abstract>>
        +_get_namespace(root) str
        +_get_text_from_element() str
        +_get_attribute_safe() str
        +_get_int_attribute() int
        +_get_float_attribute() float
        +_parse_coords() tuple
        +_find_element_safe() Element
        +_findall_safe() list
    }

    class ALTOParser {
        +parse(file) pa.Table
        +_extract_word_data() dict
        +_aggregate_strings() dict
        +_aggregate_line_data() dict
        +_aggregate_region_data() dict
    }

    class PageXMLParser {
        +parse(file) pa.Table
        +_extract_element_data() dict
        +_extract_words()
        +_extract_lines()
        +_extract_regions()
    }

    class BaseSerializer {
        <<abstract>>
        +source_xml: str
        +serialize(table) str
    }

    class ALTOSerializer {
        +serialize(table) str
    }

    class PageXMLSerializer {
        +serialize(table) str
    }

    BaseParser <|-- BaseXMLParser
    BaseXMLParser <|-- ALTOParser
    BaseXMLParser <|-- PageXMLParser
    BaseSerializer <|-- ALTOSerializer
    BaseSerializer <|-- PageXMLSerializer
```

### Processing Components and Service Layer

```mermaid
classDiagram
    class StreamingBatchProcessor {
        +files: List[Path]
        +parser: BaseParser
        +config: BatchProcessorConfig
        +stream_to_parquet() int
        +parallel_process() Iterator
        +process_with_memory_limit() Iterator
    }

    class AdaptiveProcessor {
        +_adjust_batch_size()
        +_monitor_memory()
    }

    class ParserFactory {
        <<service>>
        +create_parser(format, schema, level) BaseParser
        +auto_detect_format(file) str
    }

    class SerializerFactory {
        <<service>>
        +create_serializer(format, source_xml) BaseSerializer
    }

    class ProcessingService {
        <<service>>
        +config: BatchProcessorConfig
        +parse_single_file() pa.Table
        +batch_process_files() int
        +stream_process_with_memory_limit() int
    }

    class TransformationService {
        <<service>>
        +transform_parquet() int
    }

    class StatsService {
        <<service>>
        +get_parquet_stats() dict
    }

    StreamingBatchProcessor <|-- AdaptiveProcessor
    ProcessingService --> ParserFactory
    ProcessingService --> StreamingBatchProcessor
```

## Data Flow Architecture

### Single File Processing

```mermaid
flowchart LR
    XML[XML File] --> Parser
    Parser --> Table[PyArrow Table]
    Table --> Narwhals[Optional: Narwhals]
    Narwhals --> Serializer
    Table --> Serializer
    Serializer --> OutputXML[XML Output]

    Table -.-> Track[Auto-tracks source_file]
```

### Roundtrip Conversion Flow

```mermaid
flowchart TB
    subgraph Parse["Parsing Phase"]
        XML1[Original ALTO/PageXML]
        XML1 --> ExtractMeta[Extract Metadata]
        XML1 --> ExtractData[Extract Text Data]
        ExtractMeta --> MetaTable[Metadata Table<br/>• XML declaration<br/>• Namespaces<br/>• Processing info<br/>• Custom elements]
        ExtractData --> DataTable[Data Table<br/>• Text content<br/>• Coordinates<br/>• IDs<br/>• Confidence]
    end

    subgraph Store["Storage Phase"]
        DataTable --> Parquet[Parquet File]
        MetaTable --> ParquetMeta[Metadata Sidecar]
        Parquet --> |source_file tracked| Dataset[Partitioned Dataset]
    end

    subgraph Reconstruct["Reconstruction Phase"]
        Dataset --> LoadTable[Load PyArrow Table]
        ParquetMeta --> LoadMeta[Load Metadata]
        LoadTable --> FilterSource{Filter by<br/>source_file}
        FilterSource --> SourceData[Data for specific file]
        SourceData --> Serializer[Template-based<br/>Serializer]
        LoadMeta --> Serializer
        XML1 -.->|Original as template| Serializer
        Serializer --> XML2[Reconstructed ALTO/PageXML<br/>with preserved structure]
    end
```

### Metadata Preservation Strategy

```mermaid
flowchart LR
    subgraph Input["XML Input"]
        OrigXML[Original XML]
        Meta["Metadata Elements<br/>• Description<br/>• Processing Software<br/>• Styles<br/>• Layout"]
        Data["Data Elements<br/>• TextBlocks<br/>• TextLines<br/>• Words"]
    end

    subgraph Processing["Processing"]
        MetaSchema["metadata_schema()<br/>Preserves ALL metadata"]
        DataSchema["ALTO/PageXML Schema<br/>Structured extraction"]
    end

    subgraph Output["PyArrow Output"]
        MainTable["Main Table<br/>page_id, text, coords, ..."]
        MetaTable["Metadata Table<br/>page_id, format_type, namespaces, ..."]
        MainTable -.->|linked by page_id| MetaTable
    end

    OrigXML --> Meta
    OrigXML --> Data
    Meta --> MetaSchema
    Data --> DataSchema
    MetaSchema --> MetaTable
    DataSchema --> MainTable
```

### Batch Processing Flow

```mermaid
flowchart TD
    Files[File List] --> Iterator
    Iterator --> Accumulate
    Accumulate --> Check{10K rows?}
    Check -->|Yes| Yield[Yield Batch]
    Check -->|No| Next[Next File]
    Next --> Iterator
    Yield --> Output[Stream/Table/Parquet]
    Output --> Next
```

### Batch to Individual XML Reconstruction

```mermaid
flowchart TB
    subgraph BatchProcess["Batch Processing"]
        Files[1000 XML Files] --> Parser[Batch Parser]
        Parser --> Combined[Combined PyArrow Table<br/>with source_file column]
        Combined --> Parquet[Single Parquet File<br/>or Partitioned Dataset]
    end

    subgraph Reconstruction["Individual Reconstruction"]
        Parquet --> Query[Query by source_file]
        Query --> |"WHERE source_file='doc_001.xml'"| Filtered[Filtered Table]

        Original[Original doc_001.xml<br/>as template] --> Serializer
        Filtered --> Serializer[ALTOSerializer/<br/>PageXMLSerializer]
        Serializer --> Recreated[Recreated doc_001.xml<br/>with original structure]
    end

    subgraph MultiReconstruction["Bulk Reconstruction"]
        Parquet --> GroupBy[Group by source_file]
        GroupBy --> Iterate[For each group]
        Iterate --> Template[Load original template]
        Template --> BulkSerialize[Serialize with template]
        BulkSerialize --> Multiple[Multiple XML files<br/>with preserved structure]
    end
```

### Cross-Library Processing

```mermaid
flowchart LR
    Table[PyArrow Table] --> Narwhals
    Narwhals --> Libraries[Polars/Pandas/PyArrow]
    Libraries --> Transform
    Transform --> Result[PyArrow Table]
```

## Memory Management Strategy

### Memory-Efficient Processing

```mermaid
flowchart TD
    Files[1M XML Files] --> Generator[Generator<br/>Lazy Loading]
    Generator --> Batch[10K Row Batch]
    Batch --> Process
    Process --> Buffer[Fixed Buffer<br/>< 1GB Memory]
    Buffer --> Write[Incremental Write<br/>to Parquet]
    Write --> Next[Next Batch]
    Next --> Generator
```

### Parallel Processing Architecture

```mermaid
flowchart TD
    Chunks[File Chunks] --> Executor[ThreadPoolExecutor]

    Executor --> W1[Worker 1<br/>Files 1-1000]
    Executor --> W2[Worker 2<br/>Files 1001-2000]
    Executor --> W3[Worker 3<br/>Files 2001-3000]

    W1 --> P1[Parse] --> T1[Table]
    W2 --> P2[Parse] --> T2[Table]
    W3 --> P3[Parse] --> T3[Table]

    T1 --> Stream[Stream Write<br/>Incremental]
    T2 --> Stream
    T3 --> Stream
```

## Recent Refactoring Improvements

### Code Simplification (2024)

#### Before Refactoring
- PageXML parser: 338 lines with massive DRY violations
- ALTO parser: 289 lines with duplicated extraction logic
- Complex type hints throughout
- Unnecessary Pydantic models for internal data

#### After Refactoring
- PageXML parser: 245 lines (27.5% reduction)
- ALTO parser: 211 lines (27% reduction)
- BaseXMLParser: 176 lines of reusable utilities
- Removed unnecessary ExtractedText Pydantic model
- Simplified to use plain dictionaries
- Cleaner code without complex type hints

### Key Improvements

1. **DRY Principle Applied**
   - Single `_extract_element_data()` method replaces 4 duplicate blocks in PageXML
   - Common XML utilities extracted to `BaseXMLParser`
   - Aggregation logic unified in ALTO parser

2. **Service Layer Pattern**
   - `ParserFactory`: Dynamic parser creation with auto-detection
   - `ProcessingService`: Business logic separated from CLI
   - `TransformationService`: Data transformation operations
   - CLI now only handles user interaction

3. **Simplified Data Flow**
   - Direct dictionary usage instead of intermediate Pydantic models
   - PyArrow expects dicts anyway - no conversion needed
   - Cleaner, more maintainable code

## Schema Definitions

### PyArrow Schema Structure

```mermaid
erDiagram
    ALTO_SCHEMA {
        string page_id
        string region_id
        string line_id
        string word_id
        string text
        int32 x
        int32 y
        int32 width
        int32 height
        float32 confidence
        string style_refs
    }

    PAGEXML_SCHEMA {
        string page_id
        string region_id
        string line_id
        string word_id
        string text
        string coords
        string baseline
        float32 confidence
    }

    METADATA_SCHEMA {
        string page_id
        string format_type
        string format_version
        string xml_declaration
        string namespaces
        string schema_location
        int32 page_width
        int32 page_height
        string page_filename
        string page_attributes
        string reading_order
        string alto_measurement_unit
        string alto_processing_info
        string pagexml_creator
        string pagexml_created
        string pagexml_last_change
        string custom_elements
        bool original_schema_valid
        string validation_errors
    }

    ALTO_SCHEMA ||--|| METADATA_SCHEMA : "linked by page_id"
    PAGEXML_SCHEMA ||--|| METADATA_SCHEMA : "linked by page_id"
```

### Schema Usage in Roundtrip

```mermaid
sequenceDiagram
    participant XML as Original XML
    participant Parser
    participant DataTable as Data Table
    participant MetaTable as Metadata Table
    participant Parquet
    participant Serializer
    participant Output as Output XML

    XML->>Parser: Parse file
    Parser->>DataTable: Extract text data<br/>(ALTO_SCHEMA)
    Parser->>MetaTable: Extract metadata<br/>(METADATA_SCHEMA)
    DataTable->>Parquet: Write data
    MetaTable->>Parquet: Write metadata sidecar

    Note over Parquet: Storage phase<br/>Can process millions of files

    Parquet->>Serializer: Read data by source_file
    Parquet->>Serializer: Read metadata
    XML->>Serializer: Use as template
    Serializer->>Output: Generate XML with<br/>preserved structure
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

## Processing Modes

### Streaming Mode
- Memory usage: O(batch_size)
- Use case: Large file collections
- Incremental writing to Parquet

### Parallel Mode
- Memory usage: O(workers * batch_size)
- Use case: Medium datasets with CPU resources
- ThreadPoolExecutor-based processing

### Hybrid Mode
- Combines parallel and streaming
- Adapts batch size to available memory

### Adaptive Mode
- Automatically adjusts to system resources
- Monitors memory usage and adjusts batch sizes


## Key Design Decisions

| Decision | Rationale |
|----------|----------|
| **PyArrow as core data structure** | • Native Parquet support<br/>• Zero-copy operations<br/>• C++ performance |
| **Schema-driven extraction** | • Type safety<br/>• Selective field extraction<br/>• Predictable output |
| **Template-based serialization** | • Preserves document metadata<br/>• Maintains XML structure<br/>• Simpler implementation |
| **Narwhals integration** | • Cross-library compatibility<br/>• User choice of dataframe lib<br/>• Future-proof design |
| **Batch by rows, not files** | • Consistent memory usage<br/>• Handles varying file sizes<br/>• Predictable performance |
| **10K row default batch size** | • Balance memory vs I/O<br/>• Optimized for typical workloads<br/>• User configurable |
| **Dependency injection** | • Clear separation of concerns<br/>• Testable components<br/>• Flexible configuration |

## Error Handling Strategy

```mermaid
flowchart TD
    File[XML File] --> Parse
    Parse --> Validate[Validate<br/>Format detection]
    Validate --> Valid{Valid?}

    Valid -->|Yes| Extract[Extract Elements]
    Valid -->|No| LogError[Log Error]

    Extract --> CreateTable[Create PyArrow Table]
    LogError --> ReturnEmpty[Return Empty]

    CreateTable --> Continue[Continue Processing]
    ReturnEmpty --> Continue
    Continue --> NextFile[Next File]
```

### Batch Processing Error Resilience

- Individual file failures don't stop batch processing
- Failed files are logged with detailed error messages
- Empty batches are handled gracefully
- Partial results are preserved and written
- Configurable error thresholds and retry logic

## Scalability Architecture

### Vertical Scaling

```mermaid
flowchart LR
    PyArrow[PyArrow Threading] --> ParallelIO[Parallel I/O]
    ParallelIO --> T1[Thread 1<br/>Parquet Write]
    ParallelIO --> T2[Thread 2<br/>Compression]
    ParallelIO --> TN[Thread N<br/>Column Encoding]

    Memory[More Memory] --> Batches[Larger Batches]
    Batches --> Throughput[Better Throughput]
```

### Horizontal Scaling

```mermaid
flowchart TD
    Dataset --> Partitioned[Partitioned Dataset<br/>by page_id]
    Partitioned --> P1[Partition 1]
    Partitioned --> P2[Partition 2]
    Partitioned --> PN[Partition N]

    P1 --> Process1[Process Parallel]
    P2 --> Process2[Process Parallel]
    PN --> ProcessN[Process Parallel]
```

### Storage Optimization

```mermaid
flowchart LR
    Parquet[Partitioned Parquet] --> Fast[Fast Queries<br/>• Predicate Pushdown<br/>• Column Projection]
    Compression[Compression<br/>Snappy] --> Storage[Reduced Storage]
    Columnar[Columnar Format] --> Analytics[Efficient Analytics<br/>Only read needed columns]
```

## API Usage Examples

### Using the Service Layer (Recommended)

```python
# New simplified approach with service layer
from meleon.services import ProcessingService, ParserFactory

# Auto-detect format and process
service = ProcessingService()
table = service.parse_single_file(
    input_file="document.xml",
    format_type="auto",  # Auto-detects ALTO or PageXML
    level="word"
)

# Batch process with filtering
files = ["file1.xml", "file2.xml", "file3.xml"]
total_rows = service.batch_process_files(
    files=files,
    output_path="output.parquet",
    format_type="auto",
    level="word",
    mode="streaming"
)
```

### Basic Roundtrip Example

```python
# 1. Parse ALTO to PyArrow
from meleon import parse, serialize, ALTOParser, ALTOSerializer
from meleon.schemas import ALTO_SCHEMA

parser = ALTOParser(ALTO_SCHEMA, level="word")
table = parse("original.xml", parser)

# 2. Process/transform the data
import pyarrow.compute as pc
filtered = table.filter(pc.field("confidence") > 0.9)

# 3. Serialize back to ALTO using original as template
serializer = ALTOSerializer(source_xml="original.xml")
xml_output = serialize(filtered, serializer)

# xml_output preserves all original metadata and structure
```

### Batch Processing with Reconstruction

```python
# 1. Batch process many files to Parquet
from meleon import batch_process, ALTOParser
from meleon.schemas import ALTO_SCHEMA
import glob

files = glob.glob("xmls/*.xml")
parser = ALTOParser(ALTO_SCHEMA, level="word")

batch_process(
    files,
    parser,
    output_path="combined.parquet",
    streaming=True
)

# 2. Later: Reconstruct specific file from Parquet
import pyarrow.parquet as pq
import pyarrow.compute as pc
from meleon import serialize, ALTOSerializer

# Load and filter by source file
table = pq.read_table("combined.parquet")
file_data = table.filter(
    pc.field("source_file") == "xmls/document_001.xml"
)

# Serialize using original as template
serializer = ALTOSerializer(source_xml="xmls/document_001.xml")
reconstructed_xml = serialize(file_data, serializer)
```

### Metadata Preservation Example

```python
# 1. Simple parsing
from meleon import parse, ALTOParser
from meleon.schemas import ALTO_SCHEMA, metadata_schema

parser = ALTOParser(ALTO_SCHEMA, level="word")
table = parse("document.xml", parser)

# 2. Batch processing with streaming
from meleon import batch_process

batch_process(
    files,
    parser,
    output_path="output.parquet",
    batch_size=10000,
    streaming=True
)

# 3. Cross-library processing with Narwhals
from meleon.converters.narwhals_adapter import NarwhalsAdapter

adapter = NarwhalsAdapter()
df = adapter.to_narwhals(arrow_table)
# Transform with Polars/Pandas...
result = adapter.from_narwhals(df)

# 4. Serialization with template
from meleon import serialize, ALTOSerializer

serializer = ALTOSerializer(source_xml="original.xml")
xml_output = serialize(modified_table, serializer)

# 5. Streaming with memory limit
from meleon import StreamingBatchProcessor, BatchProcessorConfig

config = BatchProcessorConfig()
config.processing.memory_limit_mb = 512

processor = StreamingBatchProcessor(files, parser, config)
processor.stream_to_parquet("output_dir/")

# 6. Lazy dataset operations
import pyarrow.dataset as ds
import pyarrow.compute as pc

dataset = ds.dataset("output_dir/", format="parquet")
scanner = dataset.scanner(
    columns=["text", "confidence"],
    filter=pc.field("confidence") > 0.9
)
for batch in scanner.to_batches():
    # Process filtered batches
    pass
```


## Configuration System

```python
class BatchProcessorConfig:
    processing: ProcessingConfig
    streaming: StreamingConfig
    parquet: ParquetConfig
    dataset: DatasetConfig

class ProcessingConfig:
    batch_file_size: int = 1000
    batch_row_size: int = 10000
    shard_size: int = 100000
    max_workers: Optional[int] = None
    memory_limit_mb: int = 1024
    processing_mode: Literal["sequential", "parallel", "streaming", "hybrid"]
    compression: str = "snappy"
```

## Roundtrip Capabilities Summary

### Supported Workflows

1. **Single File Roundtrip**
   - XML → PyArrow → Transform → XML (preserves structure)

2. **Batch Processing with Individual Reconstruction**
   - 1000s of XMLs → Single Parquet → Query by source_file → Individual XMLs

3. **Metadata Preservation**
   - All non-text elements preserved in metadata table
   - Linked to data via page_id
   - Used during serialization to reconstruct original structure

4. **Template-Based Serialization**
   - Original XML serves as template
   - Data is injected into template preserving all formatting
   - Ensures perfect roundtrip fidelity

### Tested Pipeline Capabilities

Successfully tested complete pipeline for:
1. **Loading multiple ALTO XML files**
2. **Data transformation** (filter by confidence > 0.95)
3. **Serialization to both ALTO and PageXML formats**

Example test results:
- Input: 2 ALTO files, 16 total words
- Filter: WC > 0.95
- Output: 10 high-confidence words (62.5%)
- Generated both filtered ALTO and PageXML outputs

### Current Limitations
- Template required for XML serialization (by design - ensures structure preservation)
- Limited to ALTO and PageXML formats
- No XSD schema validation
- Basic error reporting in batch mode

### Extension Points
- Add new format support by extending BaseParser/BaseSerializer
- Custom schemas through PyArrow schema definitions
- Cloud storage support via PyArrow dataset API
- Custom processors by extending StreamingBatchProcessor
