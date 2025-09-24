"""Utility functions."""

from pathlib import Path
from typing import Union


def detect_format(filepath: Union[str, Path]) -> str:
    """Detect XML format from file content."""
    filepath = Path(filepath)

    if "alto" in filepath.name.lower():
        return "alto"
    elif "page" in filepath.name.lower():
        return "pagexml"

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read(2000).lower()

        if "alto" in content or "<alto" in content:
            return "alto"
        elif "pcgts" in content or "<page" in content:
            return "pagexml"
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        pass

    return "unknown"
