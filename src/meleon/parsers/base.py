"""Base parser interface for schema-driven XML parsing."""

from abc import ABC, abstractmethod
import pyarrow as pa


class BaseParser(ABC):
    """Abstract base class for XML parsers that output PyArrow tables directly."""

    def __init__(self, schema: pa.Schema, level: str = "word"):
        """
        Initialize parser with schema and extraction level.

        Args:
            schema: PyArrow schema defining fields to extract
            level: Extraction level ("word", "line", "region")
        """
        self.schema = schema
        self.level = level

    @abstractmethod
    def parse(self, file_path: str) -> pa.Table:
        """
        Parse XML file directly to PyArrow table using configured schema and level.

        Args:
            file_path: Path to XML file

        Returns:
            PyArrow table with schema columns
        """
        pass

    @abstractmethod
    def detect_format(self, file_path: str) -> bool:
        """
        Check if this parser can handle the given file.

        Args:
            file_path: Path to XML file

        Returns:
            True if this parser can handle the format
        """
        pass

    def get_format_name(self) -> str:
        """Get the name of the format this parser handles."""
        return self.__class__.__name__.replace("Parser", "").lower()
