"""Direct Narwhals operations on PyArrow tables for cross-library dataframe compatibility."""

import narwhals as nw
import pyarrow as pa
from typing import Any, Callable, Tuple


def process_with_narwhals(
    data_table: pa.Table,
    metadata_table: pa.Table,
    transform_func: Callable[[Any, Any], Any],
    validate_schema: bool = True,
) -> Tuple[pa.Table, pa.Table]:
    """
    Process PyArrow tables with Narwhals for cross-library compatibility.

    Args:
        data_table: PyArrow data table
        metadata_table: PyArrow metadata table
        transform_func: Function that transforms Narwhals dataframe
        validate_schema: Whether to validate output matches input schema

    Returns:
        Tuple of (transformed_data_table, metadata_table)
    """
    data_df = nw.from_native(data_table)
    metadata_df = nw.from_native(metadata_table)

    transformed_df = transform_func(data_df, metadata_df)

    transformed_table = nw.to_native(transformed_df)

    if validate_schema and data_table.schema != transformed_table.schema:
        try:
            transformed_table = transformed_table.cast(data_table.schema)
        except Exception as e:
            raise ValueError(f"Schema validation failed: {e}")

    return transformed_table, metadata_table



def filter_by_confidence(df: Any, min_confidence: float = 0.8) -> Any:
    """Filter words by confidence threshold."""
    return df.filter(nw.col("confidence") >= min_confidence)


def filter_by_region_type(df: Any, region_types: list) -> Any:
    """Filter by region types."""
    return df.filter(nw.col("region_type").is_in(region_types))


def add_text_length(df: Any) -> Any:
    """Add text length column."""
    return df.with_columns(text_length=nw.col("text").str.len_chars())


def transform_coordinates(df: Any, x_offset: int = 0, y_offset: int = 0) -> Any:
    """Transform coordinates by adding offsets."""
    return df.with_columns(x=nw.col("x") + x_offset, y=nw.col("y") + y_offset)
