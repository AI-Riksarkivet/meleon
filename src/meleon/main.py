"""Main API for schema-driven XML parsing."""

import logging
from pathlib import Path
import pyarrow as pa

from .parsers import BaseParser
from .serializers import BaseSerializer

logger = logging.getLogger(__name__)


def parse(file_path: str, parser: BaseParser) -> pa.Table:
    """
    Parse XML file to PyArrow table using provided parser instance.

    Args:
        file_path: Path to XML file
        parser: Configured parser instance (e.g., ALTOParser(schema, level))

    Returns:
        PyArrow table with extracted data
    """
    file_path = str(Path(file_path).resolve())

    if not parser.detect_format(file_path):
        logger.warning(f"Parser {parser.get_format_name()} may not support file: {file_path}")

    return parser.parse(file_path)


def serialize(data_table: pa.Table, serializer: BaseSerializer) -> str:
    """
    Serialize PyArrow table back to XML using provided serializer instance.

    Args:
        data_table: PyArrow table with data
        serializer: Configured serializer instance (e.g., ALTOSerializer(source_xml))

    Returns:
        XML string
    """
    return serializer.serialize(data_table)
