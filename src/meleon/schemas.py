"""Format-specific PyArrow schemas for ALTO and PageXML."""

import json
from typing import Any, Dict, List, Optional

import pyarrow as pa
from pydantic import BaseModel, Field


ALTO_SCHEMA = pa.schema(
    [
        pa.field("page_id", pa.string()),
        pa.field("region_id", pa.string()),
        pa.field("line_id", pa.string()),
        pa.field("word_id", pa.string()),
        pa.field("text", pa.string()),
        pa.field("x", pa.int32()),
        pa.field("y", pa.int32()),
        pa.field("width", pa.int32()),
        pa.field("height", pa.int32()),
        pa.field("confidence", pa.float32()),
        pa.field("style_refs", pa.string()),
    ]
)

PAGEXML_SCHEMA = pa.schema(
    [
        pa.field("page_id", pa.string()),
        pa.field("region_id", pa.string()),
        pa.field("line_id", pa.string()),
        pa.field("word_id", pa.string()),
        pa.field("text", pa.string()),
        pa.field("coords", pa.string()),
        pa.field("baseline", pa.string()),
        pa.field("confidence", pa.float32()),
    ]
)


def metadata_schema() -> pa.Schema:
    """Schema for document metadata sidecar table."""
    return pa.schema(
        [
            pa.field("page_id", pa.string(), nullable=False),
            pa.field("format_type", pa.string(), nullable=False),
            pa.field("format_version", pa.string(), nullable=True),
            pa.field("xml_declaration", pa.string(), nullable=False),
            pa.field("namespaces", pa.string(), nullable=True),
            pa.field("schema_location", pa.string(), nullable=True),
            pa.field("page_width", pa.int32(), nullable=False),
            pa.field("page_height", pa.int32(), nullable=False),
            pa.field("page_filename", pa.string(), nullable=True),
            pa.field("page_attributes", pa.string(), nullable=True),
            pa.field("reading_order", pa.string(), nullable=True),
            pa.field("alto_measurement_unit", pa.string(), nullable=True),
            pa.field("alto_processing_info", pa.string(), nullable=True),
            pa.field("pagexml_creator", pa.string(), nullable=True),
            pa.field("pagexml_created", pa.string(), nullable=True),
            pa.field("pagexml_last_change", pa.string(), nullable=True),
            pa.field("custom_elements", pa.string(), nullable=True),
            pa.field("description_section", pa.string(), nullable=True),
            pa.field("metadata_section", pa.string(), nullable=True),
            pa.field("original_schema_valid", pa.bool_(), nullable=False),
            pa.field("validation_errors", pa.string(), nullable=True),
        ]
    )


class DocumentMetadataFlat(BaseModel):
    """Pydantic model for document metadata before JSON serialization."""

    page_id: str
    format_type: str
    format_version: Optional[str] = None

    xml_declaration: str = '<?xml version="1.0" encoding="UTF-8"?>'
    namespaces: Dict[str, str] = Field(default_factory=dict)
    schema_location: Optional[str] = None

    page_width: int
    page_height: int
    page_filename: Optional[str] = None
    page_attributes: Dict[str, str] = Field(default_factory=dict)

    reading_order: Optional[Dict[str, Any]] = None

    alto_measurement_unit: Optional[str] = None
    alto_processing_info: Optional[Dict[str, Any]] = None
    pagexml_creator: Optional[str] = None
    pagexml_created: Optional[str] = None
    pagexml_last_change: Optional[str] = None

    custom_elements: List[Dict[str, Any]] = Field(default_factory=list)
    description_section: Optional[Dict[str, Any]] = None
    metadata_section: Optional[Dict[str, Any]] = None

    original_schema_valid: bool = False
    validation_errors: Optional[List[str]] = None

    def to_arrow_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for PyArrow table creation."""
        data = self.model_dump()

        for key in [
            "namespaces",
            "page_attributes",
            "reading_order",
            "alto_processing_info",
            "custom_elements",
            "description_section",
            "metadata_section",
            "validation_errors",
        ]:
            if key in data:
                data[key] = json.dumps(data[key]) if data[key] else None

        return data

    @classmethod
    def from_arrow_row(cls, row: Dict[str, Any]) -> "DocumentMetadataFlat":
        """Create from PyArrow table row."""
        for key in [
            "namespaces",
            "page_attributes",
            "reading_order",
            "alto_processing_info",
            "custom_elements",
            "description_section",
            "metadata_section",
            "validation_errors",
        ]:
            if key in row and row[key]:
                row[key] = json.loads(row[key])

        return cls(**row)
