"""
Demo: How the new Service Layer Architecture works
===================================================

This demonstrates the refactoring that separates business logic from the CLI
into dedicated service classes, following SOLID principles.
"""

from pathlib import Path
from meleon.services import (
    ParserFactory,
    SerializerFactory,
    ProcessingService,
)
from meleon.schemas import ALTO_SCHEMA, PAGEXML_SCHEMA
from meleon.config import BatchProcessorConfig


def demo_parser_factory():
    """
    BEFORE: CLI had hardcoded parser creation logic
    ----------------------------------------
    # In CLI (old way - violates Open/Closed Principle):
    if format == "alto":
        parser = ALTOParser(ALTO_SCHEMA, level)
    else:
        parser = PageXMLParser(PAGEXML_SCHEMA, level)

    AFTER: Factory pattern for parser creation
    ----------------------------------------
    """
    print("=" * 60)
    print("DEMO 1: ParserFactory - Creating parsers dynamically")
    print("=" * 60)

    # Create parsers using factory - follows Open/Closed Principle
    # Easy to extend with new formats without modifying existing code

    alto_parser = ParserFactory.create_parser(format_type="alto", schema=ALTO_SCHEMA, level="word")
    print(f"✓ Created ALTO parser: {alto_parser.__class__.__name__}")

    pagexml_parser = ParserFactory.create_parser(
        format_type="pagexml", schema=PAGEXML_SCHEMA, level="line"
    )
    print(f"✓ Created PageXML parser: {pagexml_parser.__class__.__name__}")

    # Auto-detect format from file
    test_file = Path("sample.xml")  # Would check actual file content
    detected_format = ParserFactory.auto_detect_format(test_file)
    print(f"✓ Auto-detected format for {test_file}: {detected_format}")

    print("\nBenefits:")
    print("• Centralized parser creation logic")
    print("• Easy to add new parser types")
    print("• Follows Factory pattern")
    print("• No if/else chains in CLI code")


def demo_serializer_factory():
    """
    Demonstrates SerializerFactory pattern
    """
    print("\n" + "=" * 60)
    print("DEMO 2: SerializerFactory - Creating serializers")
    print("=" * 60)

    # Create serializers using factory
    alto_serializer = SerializerFactory.create_serializer(
        format_type="alto",
        source_xml="<original>...</original>",  # Optional template
    )
    print(f"✓ Created ALTO serializer: {alto_serializer.__class__.__name__}")

    pagexml_serializer = SerializerFactory.create_serializer(format_type="pagexml")
    print(f"✓ Created PageXML serializer: {pagexml_serializer.__class__.__name__}")

    print("\nBenefits:")
    print("• Consistent serializer creation")
    print("• Structure preservation support")
    print("• Extensible for new formats")


def demo_processing_service():
    """
    BEFORE: Business logic mixed with CLI presentation
    ------------------------------------------------
    # In CLI command (old way - violates SRP):
    @app.command()
    def parse(...):
        # Presentation logic (progress bars)
        with Progress() as progress:
            # Business logic mixed in
            if format == "auto":
                format = detect_format()
            parser = create_parser()
            table = parser.parse()
            # More presentation (console output)
            console.print("Done")

    AFTER: Clean separation of concerns
    -----------------------------------
    """
    print("\n" + "=" * 60)
    print("DEMO 3: ProcessingService - Separated business logic")
    print("=" * 60)

    # Create service with configuration
    config = BatchProcessorConfig()
    config.processing.batch_row_size = 10000
    config.processing.memory_limit_mb = 512

    ProcessingService(config)
    print("✓ Created ProcessingService with config")

    # Example: Parse single file (business logic only)
    # The CLI just calls this and handles presentation
    print("\nService methods (business logic only):")
    print("• service.parse_single_file()")
    print("• service.batch_process_files()")
    print("• service.stream_process_with_memory_limit()")

    print("\nHow CLI uses it now:")
    print("""
    # In CLI (clean separation):
    @app.command()
    def parse(input_file, format, level):
        # Presentation layer (progress bar)
        with Progress() as progress:
            # Call service for business logic
            service = ProcessingService()
            table = service.parse_single_file(input_file, format, level)

            # Handle presentation
            console.print(f"Done: {table.num_rows} rows")
    """)

    print("\nBenefits:")
    print("• CLI focuses on user interaction")
    print("• Service layer handles business logic")
    print("• Easier to test (can test service without CLI)")
    print("• Reusable in non-CLI contexts (web API, scripts)")


def demo_transformation_service():
    """
    Demonstrates data transformation service
    """
    print("\n" + "=" * 60)
    print("DEMO 4: TransformationService - Data operations")
    print("=" * 60)

    print("Static service for data transformations:")
    print("""
    # Transform with filters and projections
    num_rows = TransformationService.transform_parquet(
        input_parquet=Path("input.parquet"),
        output_path=Path("output.parquet"),
        min_confidence=0.8,
        columns=["text", "confidence"]
    )
    """)

    print("\nBenefits:")
    print("• Encapsulates PyArrow operations")
    print("• Reusable transformation logic")
    print("• No PyArrow imports in CLI")


def demo_stats_service():
    """
    Demonstrates statistics service
    """
    print("\n" + "=" * 60)
    print("DEMO 5: StatsService - Analytics operations")
    print("=" * 60)

    print("Statistics extraction as a service:")
    print("""
    # Get Parquet file statistics
    stats = StatsService.get_parquet_stats(parquet_file)

    # Returns clean dictionary:
    {
        'num_rows': 150000,
        'num_columns': 12,
        'total_size_mb': 45.2,
        'compressed_size_mb': 12.3,
        'compression_ratio': 72.8
    }
    """)

    print("\nBenefits:")
    print("• Complex calculations hidden from CLI")
    print("• Returns structured data")
    print("• CLI just formats for display")


def show_architecture_comparison():
    """
    Shows the overall architecture improvement
    """
    print("\n" + "=" * 60)
    print("ARCHITECTURE COMPARISON")
    print("=" * 60)

    print("\nBEFORE (Violates SOLID principles):")
    print("""
    CLI Commands
    ├── parse()
    │   ├── Format detection logic
    │   ├── Parser creation logic
    │   ├── File I/O operations
    │   ├── Progress display
    │   └── Error handling
    └── batch()
        ├── File discovery
        ├── Config building
        ├── Processing logic
        └── Result formatting

    Problems:
    • CLI has too many responsibilities (violates SRP)
    • Hard to test business logic
    • Can't reuse logic outside CLI
    • Difficult to extend
    """)

    print("\nAFTER (Follows SOLID principles):")
    print("""
    Service Layer                    CLI Layer
    ├── ParserFactory               ├── parse command
    │   ├── create_parser()         │   ├── Get user input
    │   └── auto_detect()           │   ├── Call service
    │                               │   └── Display results
    ├── ProcessingService           │
    │   ├── parse_single_file()    ├── batch command
    │   ├── batch_process()         │   ├── Get user input
    │   └── stream_process()        │   ├── Call service
    │                               │   └── Show progress
    ├── TransformationService       │
    │   └── transform_parquet()     └── transform command
    │                                   ├── Get user input
    └── StatsService                    ├── Call service
        └── get_parquet_stats()         └── Format output

    Benefits:
    • Each class has single responsibility
    • Easy to test services independently
    • Can use services in web API, scripts, notebooks
    • Open for extension (add new services/factories)
    • Closed for modification (existing code stable)
    """)


if __name__ == "__main__":
    print("\n🎯 SERVICE LAYER ARCHITECTURE DEMO\n")

    # Run all demos
    demo_parser_factory()
    demo_serializer_factory()
    demo_processing_service()
    demo_transformation_service()
    demo_stats_service()
    show_architecture_comparison()

    print("\n" + "=" * 60)
    print("KEY IMPROVEMENTS")
    print("=" * 60)
    print("""
    1. SINGLE RESPONSIBILITY (SRP)
       - CLI: User interaction only
       - Services: Business logic only
       - Factories: Object creation only

    2. OPEN/CLOSED (OCP)
       - Add new formats without changing factories
       - Extend services without modifying CLI

    3. DEPENDENCY INVERSION (DIP)
       - CLI depends on service abstractions
       - Not on concrete implementations

    4. BETTER TESTING
       - Test services without CLI
       - Mock services in CLI tests
       - Unit test each component

    5. REUSABILITY
       - Use services in Jupyter notebooks
       - Build web API with same services
       - Create scripts without CLI overhead
    """)
