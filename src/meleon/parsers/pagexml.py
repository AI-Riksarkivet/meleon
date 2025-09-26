"""PageXML parser with DRY improvements using Pydantic models."""

import logging
import xml.etree.ElementTree as ET

import pyarrow as pa

from .base_xml import BaseXMLParser

logger = logging.getLogger(__name__)


class PageXMLParser(BaseXMLParser):
    """Parser for PageXML format files."""

    def parse(self, file_path: str) -> pa.Table:
        """Parse PageXML file using pre-configured schema and level."""
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
        """Check if file is PageXML format."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            if "PcGts" in root.tag or "pagexml" in root.tag.lower():
                return True

            namespace = self._get_namespace(root)
            if root.find(f".//{namespace}TextRegion") is not None:
                return True

            return False
        except (ET.ParseError, FileNotFoundError, OSError):
            return False

    def _extract_element_data(self, element, element_type, context, namespace):
        """
        Single method to extract data from any PageXML element.
        This eliminates the massive duplication that was in the original code.

        Args:
            element: XML element to extract from
            element_type: Type of element ("word", "line", "region")
            context: Dictionary with page/region/line context
            namespace: XML namespace

        Returns:
            Dictionary with all extracted data
        """
        # Get text content
        text = self._get_text_from_element(element, namespace, ["TextEquiv", "Unicode"])

        # Get coordinates
        coords_elem = element.find(f"{namespace}Coords")
        coords_str = ""
        x, y, width, height = 0, 0, 0, 0

        if coords_elem is not None:
            coords_str = self._get_attribute_safe(coords_elem, "points", "")
            x, y, width, height = self._parse_coords(coords_str)

        # Get baseline for lines
        baseline = None
        if element_type == "line":
            baseline_elem = element.find(f"{namespace}Baseline")
            if baseline_elem is not None:
                baseline = self._get_attribute_safe(baseline_elem, "points", "")

        # Get confidence
        confidence = self._get_float_attribute(element, "conf", None)

        # Return dictionary
        return {
            "text": text,
            "page_id": context.get("page_id", ""),
            "region_id": context.get("region_id", ""),
            "line_id": context.get("line_id", ""),
            "word_id": context.get("word_id", "") or self._get_attribute_safe(element, "id", ""),
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "confidence": confidence,
            "coords": coords_str,
            "baseline": baseline,
            "page_width": context.get("page_width", 0),
            "page_height": context.get("page_height", 0),
            "region_type": context.get("region_type", ""),
            "custom": self._get_attribute_safe(element, "custom", context.get("custom", "")),
        }

    def _extract_words(self, root, namespace, data, field_names):
        """Extract word-level data from PageXML."""
        for page in self._findall_safe(root, ".//Page", namespace):
            page_context = {
                "page_id": self._get_attribute_safe(page, "imageFilename", ""),
                "page_width": self._get_int_attribute(page, "imageWidth", 0),
                "page_height": self._get_int_attribute(page, "imageHeight", 0),
            }

            for region in self._findall_safe(page, ".//TextRegion", namespace):
                region_context = {
                    **page_context,
                    "region_id": self._get_attribute_safe(region, "id", ""),
                    "region_type": self._get_attribute_safe(region, "type", "TextRegion"),
                    "custom": self._get_attribute_safe(region, "custom", ""),
                }

                for line in self._findall_safe(region, ".//TextLine", namespace):
                    line_context = {
                        **region_context,
                        "line_id": self._get_attribute_safe(line, "id", ""),
                    }

                    # Handle Word elements
                    words = self._findall_safe(line, ".//Word", namespace)
                    if words:
                        for word in words:
                            element_data = self._extract_element_data(
                                word, "word", line_context, namespace
                            )
                            for field_name in field_names:
                                data[field_name].append(element_data.get(field_name))
                    else:
                        # Handle lines without Word elements (whole line as single word)
                        text_equiv = line.find(f"{namespace}TextEquiv")
                        if text_equiv is not None:
                            unicode_elem = text_equiv.find(f"{namespace}Unicode")
                            if unicode_elem is not None and unicode_elem.text:
                                word_context = {
                                    **line_context,
                                    "word_id": f"{line_context['line_id']}_w0",
                                }
                                element_data = self._extract_element_data(
                                    line, "line", word_context, namespace
                                )
                                for field_name in field_names:
                                    data[field_name].append(element_data.get(field_name))

    def _extract_lines(self, root, namespace, data, field_names):
        """Extract line-level data from PageXML."""
        for page in self._findall_safe(root, ".//Page", namespace):
            page_context = {
                "page_id": self._get_attribute_safe(page, "imageFilename", ""),
                "page_width": self._get_int_attribute(page, "imageWidth", 0),
                "page_height": self._get_int_attribute(page, "imageHeight", 0),
            }

            for region in self._findall_safe(page, ".//TextRegion", namespace):
                region_context = {
                    **page_context,
                    "region_id": self._get_attribute_safe(region, "id", ""),
                    "region_type": self._get_attribute_safe(region, "type", "TextRegion"),
                }

                for line in self._findall_safe(region, ".//TextLine", namespace):
                    line_context = {
                        **region_context,
                        "line_id": self._get_attribute_safe(line, "id", ""),
                    }
                    element_data = self._extract_element_data(line, "line", line_context, namespace)
                    for field_name in field_names:
                        data[field_name].append(element_data.get(field_name))

    def _extract_regions(self, root, namespace, data, field_names):
        """Extract region-level data from PageXML."""
        for page in self._findall_safe(root, ".//Page", namespace):
            page_context = {
                "page_id": self._get_attribute_safe(page, "imageFilename", ""),
                "page_width": self._get_int_attribute(page, "imageWidth", 0),
                "page_height": self._get_int_attribute(page, "imageHeight", 0),
            }

            for region in self._findall_safe(page, ".//TextRegion", namespace):
                region_context = {
                    **page_context,
                    "region_id": self._get_attribute_safe(region, "id", ""),
                    "region_type": self._get_attribute_safe(region, "type", "TextRegion"),
                }

                # Check if region has its own text
                text_equiv = region.find(f"{namespace}TextEquiv")
                if text_equiv is not None:
                    unicode_elem = text_equiv.find(f"{namespace}Unicode")
                    if unicode_elem is not None and unicode_elem.text:
                        # Region has direct text
                        element_data = self._extract_element_data(
                            region, "region", region_context, namespace
                        )
                        for field_name in field_names:
                            data[field_name].append(element_data.get(field_name))
                    else:
                        # Aggregate text from lines
                        texts = []
                        for line in self._findall_safe(region, ".//TextLine", namespace):
                            line_text = self._get_text_from_element(
                                line, namespace, ["TextEquiv", "Unicode"]
                            )
                            if line_text:
                                texts.append(line_text)

                        if texts:
                            element_data = self._extract_element_data(
                                region, "region", region_context, namespace
                            )
                            element_data["text"] = " ".join(texts)
                            for field_name in field_names:
                                data[field_name].append(element_data.get(field_name))
                else:
                    # No direct text equiv, aggregate from lines
                    texts = []
                    for line in self._findall_safe(region, ".//TextLine", namespace):
                        line_text = self._get_text_from_element(
                            line, namespace, ["TextEquiv", "Unicode"]
                        )
                        if line_text:
                            texts.append(line_text)

                    if texts:
                        element_data = self._extract_element_data(
                            region, "region", region_context, namespace
                        )
                        element_data["text"] = " ".join(texts)
                        for field_name in field_names:
                            data[field_name].append(element_data.get(field_name))
