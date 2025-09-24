"""ALTO XML parser."""

import logging
import xml.etree.ElementTree as ET
from typing import Dict, List
import pyarrow as pa

from .base import BaseParser

logger = logging.getLogger(__name__)


class ALTOParser(BaseParser):
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

    def _get_namespace(self, root: ET.Element) -> str:
        if root.tag.startswith("{"):
            return root.tag.split("}")[0] + "}"
        return ""

    def _extract_words(
        self, root: ET.Element, namespace: str, data: Dict[str, List], field_names: set
    ):
        for page in root.findall(f".//{namespace}Page"):
            page_id = page.get("ID", "")
            page_width = int(page.get("WIDTH", 0))
            page_height = int(page.get("HEIGHT", 0))

            for block in page.findall(f".//{namespace}TextBlock"):
                region_id = block.get("ID", "")
                region_type = "TextBlock"

                for line in block.findall(f".//{namespace}TextLine"):
                    line_id = line.get("ID", "")

                    for string in line.findall(f".//{namespace}String"):
                        row_data = self._extract_word_data(
                            string,
                            page_id,
                            region_id,
                            line_id,
                            page_width,
                            page_height,
                            region_type,
                            field_names,
                        )
                        for field_name in field_names:
                            data[field_name].append(row_data.get(field_name))

    def _extract_word_data(
        self,
        string: ET.Element,
        page_id: str,
        region_id: str,
        line_id: str,
        page_width: int,
        page_height: int,
        region_type: str,
        field_names: set,
    ) -> Dict:
        row_data = {}

        if "text" in field_names:
            row_data["text"] = string.get("CONTENT", "")
        if "page_id" in field_names:
            row_data["page_id"] = page_id
        if "region_id" in field_names:
            row_data["region_id"] = region_id
        if "line_id" in field_names:
            row_data["line_id"] = line_id
        if "word_id" in field_names:
            row_data["word_id"] = string.get("ID", "")
        if "x" in field_names:
            row_data["x"] = int(string.get("HPOS", 0))
        if "y" in field_names:
            row_data["y"] = int(string.get("VPOS", 0))
        if "width" in field_names:
            row_data["width"] = int(string.get("WIDTH", 0))
        if "height" in field_names:
            row_data["height"] = int(string.get("HEIGHT", 0))
        if "confidence" in field_names:
            wc = string.get("WC")
            row_data["confidence"] = float(wc) if wc else None
        if "style_refs" in field_names:
            row_data["style_refs"] = string.get("STYLEREFS")
        if "page_width" in field_names:
            row_data["page_width"] = page_width
        if "page_height" in field_names:
            row_data["page_height"] = page_height
        if "region_type" in field_names:
            row_data["region_type"] = region_type

        return row_data

    def _extract_lines(
        self, root: ET.Element, namespace: str, data: Dict[str, List], field_names: set
    ):
        for page in root.findall(f".//{namespace}Page"):
            page_id = page.get("ID", "")
            page_width = int(page.get("WIDTH", 0))
            page_height = int(page.get("HEIGHT", 0))

            for block in page.findall(f".//{namespace}TextBlock"):
                region_id = block.get("ID", "")
                region_type = "TextBlock"

                for line in block.findall(f".//{namespace}TextLine"):
                    line_data = self._aggregate_line_data(
                        line,
                        namespace,
                        page_id,
                        region_id,
                        page_width,
                        page_height,
                        region_type,
                        field_names,
                    )
                    for field_name in field_names:
                        data[field_name].append(line_data.get(field_name))

    def _extract_regions(
        self, root: ET.Element, namespace: str, data: Dict[str, List], field_names: set
    ):
        for page in root.findall(f".//{namespace}Page"):
            page_id = page.get("ID", "")
            page_width = int(page.get("WIDTH", 0))
            page_height = int(page.get("HEIGHT", 0))

            for block in page.findall(f".//{namespace}TextBlock"):
                region_data = self._aggregate_region_data(
                    block, namespace, page_id, page_width, page_height, field_names
                )
                for field_name in field_names:
                    data[field_name].append(region_data.get(field_name))

    def _aggregate_line_data(
        self,
        line: ET.Element,
        namespace: str,
        page_id: str,
        region_id: str,
        page_width: int,
        page_height: int,
        region_type: str,
        field_names: set,
    ) -> Dict:
        line_id = line.get("ID", "")
        line_texts = []
        min_x, min_y = float("inf"), float("inf")
        max_x, max_y = 0, 0
        confidences = []

        for string in line.findall(f".//{namespace}String"):
            line_texts.append(string.get("CONTENT", ""))
            x = int(string.get("HPOS", 0))
            y = int(string.get("VPOS", 0))
            w = int(string.get("WIDTH", 0))
            h = int(string.get("HEIGHT", 0))

            min_x = min(min_x, x)
            min_y = min(min_y, y)
            max_x = max(max_x, x + w)
            max_y = max(max_y, y + h)

            wc = string.get("WC")
            if wc:
                confidences.append(float(wc))

        row_data = {}
        if "text" in field_names:
            row_data["text"] = " ".join(line_texts)
        if "page_id" in field_names:
            row_data["page_id"] = page_id
        if "region_id" in field_names:
            row_data["region_id"] = region_id
        if "line_id" in field_names:
            row_data["line_id"] = line_id
        if "x" in field_names:
            row_data["x"] = int(min_x) if min_x != float("inf") else 0
        if "y" in field_names:
            row_data["y"] = int(min_y) if min_y != float("inf") else 0
        if "width" in field_names:
            row_data["width"] = int(max_x - min_x) if min_x != float("inf") else 0
        if "height" in field_names:
            row_data["height"] = int(max_y - min_y) if min_y != float("inf") else 0
        if "confidence" in field_names:
            row_data["confidence"] = sum(confidences) / len(confidences) if confidences else None
        if "page_width" in field_names:
            row_data["page_width"] = page_width
        if "page_height" in field_names:
            row_data["page_height"] = page_height
        if "region_type" in field_names:
            row_data["region_type"] = region_type

        return row_data

    def _aggregate_region_data(
        self,
        block: ET.Element,
        namespace: str,
        page_id: str,
        page_width: int,
        page_height: int,
        field_names: set,
    ) -> Dict:
        region_id = block.get("ID", "")
        region_type = "TextBlock"
        region_texts = []
        min_x, min_y = float("inf"), float("inf")
        max_x, max_y = 0, 0
        confidences = []

        for line in block.findall(f".//{namespace}TextLine"):
            for string in line.findall(f".//{namespace}String"):
                region_texts.append(string.get("CONTENT", ""))
                x = int(string.get("HPOS", 0))
                y = int(string.get("VPOS", 0))
                w = int(string.get("WIDTH", 0))
                h = int(string.get("HEIGHT", 0))

                min_x = min(min_x, x)
                min_y = min(min_y, y)
                max_x = max(max_x, x + w)
                max_y = max(max_y, y + h)

                wc = string.get("WC")
                if wc:
                    confidences.append(float(wc))

        row_data = {}
        if "text" in field_names:
            row_data["text"] = " ".join(region_texts)
        if "page_id" in field_names:
            row_data["page_id"] = page_id
        if "region_id" in field_names:
            row_data["region_id"] = region_id
        if "x" in field_names:
            row_data["x"] = int(min_x) if min_x != float("inf") else 0
        if "y" in field_names:
            row_data["y"] = int(min_y) if min_y != float("inf") else 0
        if "width" in field_names:
            row_data["width"] = int(max_x - min_x) if min_x != float("inf") else 0
        if "height" in field_names:
            row_data["height"] = int(max_y - min_y) if min_y != float("inf") else 0
        if "confidence" in field_names:
            row_data["confidence"] = sum(confidences) / len(confidences) if confidences else None
        if "page_width" in field_names:
            row_data["page_width"] = page_width
        if "page_height" in field_names:
            row_data["page_height"] = page_height
        if "region_type" in field_names:
            row_data["region_type"] = region_type

        return row_data
