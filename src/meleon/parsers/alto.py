"""ALTO XML parser with Pydantic models and BaseXMLParser."""

import logging
import xml.etree.ElementTree as ET

import pyarrow as pa

from .base_xml import BaseXMLParser

logger = logging.getLogger(__name__)

INFINITY = float("inf")
DEFAULT_COORDINATE = 0
DEFAULT_SIZE = 0


class ALTOParser(BaseXMLParser):
    def parse(self, file_path: str) -> pa.Table:
        """Parse ALTO XML file using pre-configured schema and level."""
        tree = ET.parse(file_path)
        root = tree.getroot()
        namespace = self._get_namespace(root)

        data = {field.name: [] for field in self.schema}
        field_names = set(self.schema.names)

        if self.level == "word":
            self._extract_words(root, namespace, data, field_names)
        elif self.level == "line":
            self._extract_lines(root, namespace, data, field_names)
        elif self.level == "region":
            self._extract_regions(root, namespace, data, field_names)
        else:
            raise ValueError(f"Invalid level: {self.level}")

        return pa.table(data, schema=self.schema)

    def detect_format(self, file_path: str) -> bool:
        """Check if file is ALTO format."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            if "alto" in root.tag.lower():
                return True

            namespace = self._get_namespace(root)
            if root.find(f".//{namespace}TextBlock") is not None:
                return True

            return False
        except (ET.ParseError, FileNotFoundError, OSError):
            return False

    def _iter_page_elements(self, root, namespace):
        """Iterator for page elements with their metadata."""
        for page in root.findall(f".//{namespace}Page"):
            page_data = {
                "page_id": page.get("ID", ""),
                "page_width": int(page.get("WIDTH", DEFAULT_SIZE)),
                "page_height": int(page.get("HEIGHT", DEFAULT_SIZE)),
            }
            yield page, page_data

    def _iter_block_elements(self, page, namespace):
        """Iterator for text block elements."""
        for block in page.findall(f".//{namespace}TextBlock"):
            block_data = {
                "region_id": block.get("ID", ""),
                "region_type": "TextBlock",
            }
            yield block, block_data

    def _iter_line_elements(self, block, namespace):
        """Iterator for text line elements."""
        for line in block.findall(f".//{namespace}TextLine"):
            line_data = {"line_id": line.get("ID", "")}
            yield line, line_data

    def _iter_string_elements(self, line, namespace):
        """Iterator for string elements."""
        for string in line.findall(f".//{namespace}String"):
            yield string

    def _extract_words(self, root, namespace, data, field_names):
        """Extract word-level data using iterators for cleaner structure."""
        for page, page_data in self._iter_page_elements(root, namespace):
            for block, block_data in self._iter_block_elements(page, namespace):
                for line, line_data in self._iter_line_elements(block, namespace):
                    for string in self._iter_string_elements(line, namespace):
                        context = {
                            "page_id": page_data["page_id"],
                            "region_id": block_data["region_id"],
                            "line_id": line_data["line_id"],
                            "page_width": page_data["page_width"],
                            "page_height": page_data["page_height"],
                            "region_type": block_data["region_type"],
                        }
                        element_data = self._extract_word_data(string, context)
                        for field_name in field_names:
                            data[field_name].append(element_data.get(field_name))

    def _extract_word_data(self, string, context):
        """Extract word-level data into dictionary."""
        return {
            "text": self._get_attribute_safe(string, "CONTENT", ""),
            "page_id": context["page_id"],
            "region_id": context["region_id"],
            "line_id": context["line_id"],
            "word_id": self._get_attribute_safe(string, "ID", ""),
            "x": self._get_int_attribute(string, "HPOS", DEFAULT_COORDINATE),
            "y": self._get_int_attribute(string, "VPOS", DEFAULT_COORDINATE),
            "width": self._get_int_attribute(string, "WIDTH", DEFAULT_SIZE),
            "height": self._get_int_attribute(string, "HEIGHT", DEFAULT_SIZE),
            "confidence": self._get_float_attribute(string, "WC", None),
            "style_refs": self._get_attribute_safe(string, "STYLEREFS", None),
            "page_width": context["page_width"],
            "page_height": context["page_height"],
            "region_type": context["region_type"],
        }

    def _extract_lines(self, root, namespace, data, field_names):
        for page in self._findall_safe(root, ".//Page", namespace):
            page_context = {
                "page_id": self._get_attribute_safe(page, "ID", ""),
                "page_width": self._get_int_attribute(page, "WIDTH", 0),
                "page_height": self._get_int_attribute(page, "HEIGHT", 0),
            }

            for block in self._findall_safe(page, ".//TextBlock", namespace):
                region_context = {
                    **page_context,
                    "region_id": self._get_attribute_safe(block, "ID", ""),
                    "region_type": "TextBlock",
                }

                for line in self._findall_safe(block, ".//TextLine", namespace):
                    element_data = self._aggregate_line_data(line, namespace, region_context)
                    for field_name in field_names:
                        data[field_name].append(element_data.get(field_name))

    def _extract_regions(self, root, namespace, data, field_names):
        for page in self._findall_safe(root, ".//Page", namespace):
            page_context = {
                "page_id": self._get_attribute_safe(page, "ID", ""),
                "page_width": self._get_int_attribute(page, "WIDTH", 0),
                "page_height": self._get_int_attribute(page, "HEIGHT", 0),
            }

            for block in self._findall_safe(page, ".//TextBlock", namespace):
                element_data = self._aggregate_region_data(block, namespace, page_context)
                for field_name in field_names:
                    data[field_name].append(element_data.get(field_name))

    def _aggregate_strings(self, strings, context):
        """Common aggregation logic for lines and regions."""
        texts = []
        min_x, min_y = INFINITY, INFINITY
        max_x, max_y = 0, 0
        confidences = []

        for string in strings:
            texts.append(self._get_attribute_safe(string, "CONTENT", ""))
            x = self._get_int_attribute(string, "HPOS", DEFAULT_COORDINATE)
            y = self._get_int_attribute(string, "VPOS", DEFAULT_COORDINATE)
            w = self._get_int_attribute(string, "WIDTH", DEFAULT_SIZE)
            h = self._get_int_attribute(string, "HEIGHT", DEFAULT_SIZE)

            min_x = min(min_x, x)
            min_y = min(min_y, y)
            max_x = max(max_x, x + w)
            max_y = max(max_y, y + h)

            wc_value = self._get_float_attribute(string, "WC", None)
            if wc_value is not None:
                confidences.append(wc_value)

        return {
            "text": " ".join(texts),
            "page_id": context.get("page_id", ""),
            "region_id": context.get("region_id", ""),
            "line_id": context.get("line_id", ""),
            "x": int(min_x) if min_x != INFINITY else DEFAULT_COORDINATE,
            "y": int(min_y) if min_y != INFINITY else DEFAULT_COORDINATE,
            "width": int(max_x - min_x) if min_x != INFINITY else DEFAULT_SIZE,
            "height": int(max_y - min_y) if min_y != INFINITY else DEFAULT_SIZE,
            "confidence": sum(confidences) / len(confidences) if confidences else None,
            "page_width": context.get("page_width", 0),
            "page_height": context.get("page_height", 0),
            "region_type": context.get("region_type", ""),
        }

    def _aggregate_line_data(self, line, namespace, context):
        """Aggregate data at line level."""
        line_id = self._get_attribute_safe(line, "ID", "")
        strings = self._findall_safe(line, ".//String", namespace)
        line_context = {**context, "line_id": line_id}
        return self._aggregate_strings(strings, line_context)

    def _aggregate_region_data(self, block, namespace, context):
        """Aggregate data at region level."""
        region_id = self._get_attribute_safe(block, "ID", "")
        region_type = "TextBlock"

        # Collect all strings from all lines in the block
        strings = []
        for line in self._findall_safe(block, ".//TextLine", namespace):
            strings.extend(self._findall_safe(line, ".//String", namespace))

        region_context = {**context, "region_id": region_id, "region_type": region_type}
        return self._aggregate_strings(strings, region_context)
