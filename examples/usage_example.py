"""
Simple Usage Example: Before vs After Refactoring
==================================================
"""

print("=" * 70)
print("BEFORE: How you had to use the library (tightly coupled)")
print("=" * 70)

print("""
# OLD WAY - Everything hardcoded in your script:

from meleon.parsers import ALTOParser, PageXMLParser
from meleon.schemas import ALTO_SCHEMA, PAGEXML_SCHEMA

# You had to know which parser to use
if "alto" in filename.lower():
    parser = ALTOParser(ALTO_SCHEMA, "word")
else:
    parser = PageXMLParser(PAGEXML_SCHEMA, "word")

# Parse the file
table = parser.parse("document.xml")

# If you wanted to process multiple files, you had to write all the logic:
for file in files:
    if "alto" in file:
        parser = ALTOParser(...)
    else:
        parser = PageXMLParser(...)
    # ... lots of processing code
""")

print("\n" + "=" * 70)
print("AFTER: How you use it now (loosely coupled with services)")
print("=" * 70)

print("""
# NEW WAY - Using service layer:

from meleon.services import ProcessingService

# Just use the service - it handles everything
service = ProcessingService()

# Parse single file - auto-detects format!
table = service.parse_single_file(
    input_file="document.xml",
    format_type="auto",  # Auto-detect!
    level="word"
)

# Process batch of files - all complexity hidden
total_rows = service.batch_process_files(
    files=["file1.xml", "file2.xml"],
    output_path="output.parquet",
    format_type="auto",
    level="word"
)
""")

print("\n" + "=" * 70)
print("PRACTICAL EXAMPLE: Processing OCR files")
print("=" * 70)

# Here's actual working code


# Simulate the imports (these would be real in practice)
class ProcessingService:
    def parse_single_file(self, input_file, format_type="auto", level="word"):
        return f"Parsed {input_file} as {format_type} at {level} level"

    def batch_process_files(self, files, output_path, **kwargs):
        return f"Processed {len(files)} files to {output_path}"


class ParserFactory:
    @staticmethod
    def auto_detect_format(file_path):
        if "alto" in str(file_path).lower():
            return "alto"
        return "pagexml"


# Example 1: Parse a single file
print("\n1. PARSING SINGLE FILE:")
service = ProcessingService()
result = service.parse_single_file(input_file="sample_alto.xml", format_type="auto")
print(f"   {result}")

# Example 2: Batch processing
print("\n2. BATCH PROCESSING:")
files = ["doc1.xml", "doc2.xml", "doc3.xml"]
result = service.batch_process_files(
    files=files, output_path="output.parquet", format_type="auto", level="line"
)
print(f"   {result}")

# Example 3: Using the factory directly (if you need more control)
print("\n3. USING FACTORY DIRECTLY:")
format_type = ParserFactory.auto_detect_format("test_alto.xml")
print(f"   Detected format: {format_type}")

print("\n" + "=" * 70)
print("WHY THIS IS BETTER:")
print("=" * 70)
print("""
1. SIMPLER TO USE
   - Don't need to know about ALTOParser vs PageXMLParser
   - Auto-detection of format
   - Sensible defaults

2. EASIER TO TEST
   - Can mock the service in tests
   - Test business logic separately from CLI

3. MORE FLEXIBLE
   - Use in scripts, notebooks, web apps
   - Same code everywhere

4. EASIER TO MAINTAIN
   - Changes to parsing logic don't affect your code
   - Can add new formats without breaking existing code

5. FOLLOWS BEST PRACTICES
   - Single Responsibility: Each class does one thing
   - Open/Closed: Can extend without modifying
   - Dependency Inversion: Depend on abstractions
""")
