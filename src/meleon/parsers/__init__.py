"""XML parsers with direct PyArrow output."""

from .alto import ALTOParser
from .pagexml import PageXMLParser
from .base import BaseParser

__all__ = [
    "ALTOParser",
    "PageXMLParser",
    "BaseParser",
]
