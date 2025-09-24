"""PageXML parser."""

import logging
import xml.etree.ElementTree as ET
from typing import Tuple, Dict, List
import pyarrow as pa

from .base import BaseParser

logger = logging.getLogger(__name__)


class PageXMLParser(BaseParser):
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

    def _get_namespace(self, root: ET.Element) -> str:
        if root.tag.startswith("{"):
            return root.tag.split("}")[0] + "}"
        return ""

    def _parse_coords(self, coords_str: str) -> Tuple[int, int, int, int]:
        if not coords_str:
            return 0, 0, 0, 0

        points = []
        for pair in coords_str.split():
            x, y = pair.split(",")
            points.append((int(x), int(y)))

        if not points:
            return 0, 0, 0, 0

        xs = [p[0] for p in points]
        ys = [p[1] for p in points]

        min_x = min(xs)
        min_y = min(ys)
        max_x = max(xs)
        max_y = max(ys)

        return min_x, min_y, max_x - min_x, max_y - min_y

    def _extract_words(
        self, root: ET.Element, namespace: str, data: Dict[str, List], field_names: set
    ):
        for page in root.findall(f".//{namespace}Page"):
            page_id = page.get("imageFilename", "")
            page_width = int(page.get("imageWidth", 0))
            page_height = int(page.get("imageHeight", 0))

            for region in page.findall(f".//{namespace}TextRegion"):
                region_id = region.get("id", "")
                region_type = region.get("type", "TextRegion")
                custom = region.get("custom", "")

                for line in region.findall(f".//{namespace}TextLine"):
                    line_id = line.get("id", "")

                    for word in line.findall(f".//{namespace}Word"):
                        row_data = {}

                        if "text" in field_names:
                            text_equiv = word.find(f"{namespace}TextEquiv")
                            if text_equiv is not None:
                                unicode_elem = text_equiv.find(f"{namespace}Unicode")
                                if unicode_elem is not None:
                                    row_data["text"] = unicode_elem.text or ""
                                else:
                                    row_data["text"] = ""
                            else:
                                row_data["text"] = ""

                        if "page_id" in field_names:
                            row_data["page_id"] = page_id
                        if "region_id" in field_names:
                            row_data["region_id"] = region_id
                        if "line_id" in field_names:
                            row_data["line_id"] = line_id
                        if "word_id" in field_names:
                            row_data["word_id"] = word.get("id", "")

                        if any(f in field_names for f in ["x", "y", "width", "height", "coords"]):
                            coords_elem = word.find(f"{namespace}Coords")
                            if coords_elem is not None:
                                coords_str = coords_elem.get("points", "")

                                if "coords" in field_names:
                                    row_data["coords"] = coords_str

                                x, y, width, height = self._parse_coords(coords_str)
                                if "x" in field_names:
                                    row_data["x"] = x
                                if "y" in field_names:
                                    row_data["y"] = y
                                if "width" in field_names:
                                    row_data["width"] = width
                                if "height" in field_names:
                                    row_data["height"] = height

                        if "confidence" in field_names:
                            conf = word.get("conf")
                            row_data["confidence"] = float(conf) if conf else None

                        if "custom" in field_names:
                            row_data["custom"] = word.get("custom", custom)

                        if "page_width" in field_names:
                            row_data["page_width"] = page_width
                        if "page_height" in field_names:
                            row_data["page_height"] = page_height

                        if "region_type" in field_names:
                            row_data["region_type"] = region_type

                        for field_name in field_names:
                            data[field_name].append(row_data.get(field_name))

                    if not line.findall(f".//{namespace}Word"):
                        text_equiv = line.find(f"{namespace}TextEquiv")
                        if text_equiv is not None:
                            unicode_elem = text_equiv.find(f"{namespace}Unicode")
                            if unicode_elem is not None and unicode_elem.text:
                                row_data = {}

                                if "text" in field_names:
                                    row_data["text"] = unicode_elem.text

                                if "page_id" in field_names:
                                    row_data["page_id"] = page_id
                                if "region_id" in field_names:
                                    row_data["region_id"] = region_id
                                if "line_id" in field_names:
                                    row_data["line_id"] = line_id
                                if "word_id" in field_names:
                                    row_data["word_id"] = f"{line_id}_w0"

                                coords_elem = line.find(f"{namespace}Coords")
                                if coords_elem is not None:
                                    coords_str = coords_elem.get("points", "")

                                    if "coords" in field_names:
                                        row_data["coords"] = coords_str

                                    x, y, width, height = self._parse_coords(coords_str)
                                    if "x" in field_names:
                                        row_data["x"] = x
                                    if "y" in field_names:
                                        row_data["y"] = y
                                    if "width" in field_names:
                                        row_data["width"] = width
                                    if "height" in field_names:
                                        row_data["height"] = height

                                if "confidence" in field_names:
                                    conf = line.get("conf")
                                    row_data["confidence"] = float(conf) if conf else None

                                if "page_width" in field_names:
                                    row_data["page_width"] = page_width
                                if "page_height" in field_names:
                                    row_data["page_height"] = page_height
                                if "region_type" in field_names:
                                    row_data["region_type"] = region_type

                                for field_name in field_names:
                                    data[field_name].append(row_data.get(field_name))

    def _extract_lines(
        self, root: ET.Element, namespace: str, data: Dict[str, List], field_names: set
    ):
        for page in root.findall(f".//{namespace}Page"):
            page_id = page.get("imageFilename", "")
            page_width = int(page.get("imageWidth", 0))
            page_height = int(page.get("imageHeight", 0))

            for region in page.findall(f".//{namespace}TextRegion"):
                region_id = region.get("id", "")
                region_type = region.get("type", "TextRegion")

                for line in region.findall(f".//{namespace}TextLine"):
                    row_data = {}

                    if "text" in field_names:
                        text_equiv = line.find(f"{namespace}TextEquiv")
                        if text_equiv is not None:
                            unicode_elem = text_equiv.find(f"{namespace}Unicode")
                            if unicode_elem is not None:
                                row_data["text"] = unicode_elem.text or ""
                            else:
                                row_data["text"] = ""
                        else:
                            row_data["text"] = ""

                    if "page_id" in field_names:
                        row_data["page_id"] = page_id
                    if "region_id" in field_names:
                        row_data["region_id"] = region_id
                    if "line_id" in field_names:
                        row_data["line_id"] = line.get("id", "")

                    coords_elem = line.find(f"{namespace}Coords")
                    if coords_elem is not None:
                        coords_str = coords_elem.get("points", "")

                        if "coords" in field_names:
                            row_data["coords"] = coords_str

                        x, y, width, height = self._parse_coords(coords_str)
                        if "x" in field_names:
                            row_data["x"] = x
                        if "y" in field_names:
                            row_data["y"] = y
                        if "width" in field_names:
                            row_data["width"] = width
                        if "height" in field_names:
                            row_data["height"] = height

                    if "baseline" in field_names:
                        baseline_elem = line.find(f"{namespace}Baseline")
                        if baseline_elem is not None:
                            row_data["baseline"] = baseline_elem.get("points", "")
                        else:
                            row_data["baseline"] = None

                    if "confidence" in field_names:
                        conf = line.get("conf")
                        row_data["confidence"] = float(conf) if conf else None

                    if "page_width" in field_names:
                        row_data["page_width"] = page_width
                    if "page_height" in field_names:
                        row_data["page_height"] = page_height
                    if "region_type" in field_names:
                        row_data["region_type"] = region_type

                    for field_name in field_names:
                        data[field_name].append(row_data.get(field_name))

    def _extract_regions(
        self, root: ET.Element, namespace: str, data: Dict[str, List], field_names: set
    ):
        for page in root.findall(f".//{namespace}Page"):
            page_id = page.get("imageFilename", "")
            page_width = int(page.get("imageWidth", 0))
            page_height = int(page.get("imageHeight", 0))

            for region in page.findall(f".//{namespace}TextRegion"):
                row_data = {}

                if "text" in field_names:
                    text_equiv = region.find(f"{namespace}TextEquiv")
                    if text_equiv is not None:
                        unicode_elem = text_equiv.find(f"{namespace}Unicode")
                        if unicode_elem is not None:
                            row_data["text"] = unicode_elem.text or ""
                        else:
                            texts = []
                            for line in region.findall(f".//{namespace}TextLine"):
                                te = line.find(f"{namespace}TextEquiv")
                                if te is not None:
                                    ue = te.find(f"{namespace}Unicode")
                                    if ue is not None and ue.text:
                                        texts.append(ue.text)
                            row_data["text"] = " ".join(texts)
                    else:
                        row_data["text"] = ""

                if "page_id" in field_names:
                    row_data["page_id"] = page_id
                if "region_id" in field_names:
                    row_data["region_id"] = region.get("id", "")

                if "region_type" in field_names:
                    row_data["region_type"] = region.get("type", "TextRegion")

                coords_elem = region.find(f"{namespace}Coords")
                if coords_elem is not None:
                    coords_str = coords_elem.get("points", "")

                    if "coords" in field_names:
                        row_data["coords"] = coords_str

                    x, y, width, height = self._parse_coords(coords_str)
                    if "x" in field_names:
                        row_data["x"] = x
                    if "y" in field_names:
                        row_data["y"] = y
                    if "width" in field_names:
                        row_data["width"] = width
                    if "height" in field_names:
                        row_data["height"] = height

                if "confidence" in field_names:
                    conf = region.get("conf")
                    row_data["confidence"] = float(conf) if conf else None

                if "page_width" in field_names:
                    row_data["page_width"] = page_width
                if "page_height" in field_names:
                    row_data["page_height"] = page_height

                for field_name in field_names:
                    data[field_name].append(row_data.get(field_name))
