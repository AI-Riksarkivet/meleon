"""Base serializer interface."""

from abc import ABC, abstractmethod
from typing import Optional
import pyarrow as pa


class BaseSerializer(ABC):
    """Abstract base class for XML serializers."""

    def __init__(self, source_xml: Optional[str] = None):
        """
        Initialize serializer with optional source XML.

        Args:
            source_xml: Original XML to use as template for structure preservation.
                       If None, serializer will generate XML from scratch.
        """
        self.source_xml = source_xml

    @abstractmethod
    def serialize(self, data_table: pa.Table) -> str:
        """
        Serialize PyArrow table to XML format.

        Args:
            data_table: PyArrow table with data

        Returns:
            XML string
        """
        pass

    def get_format_name(self) -> str:
        """Get the name of the format this serializer handles."""
        return self.__class__.__name__.replace("Serializer", "").lower()
