"""ALTO XML serializer."""

import logging
import xml.etree.ElementTree as ET
from typing import Optional
import pyarrow as pa

from .base import BaseSerializer

logger = logging.getLogger(__name__)


class ALTOSerializer(BaseSerializer):
    def __init__(self, source_xml: Optional[str] = None):
        """
        Initialize ALTO serializer.

        Args:
            source_xml: Original ALTO XML to use as template for structure preservation.
                       If None, will attempt to generate XML from scratch (not yet implemented).
        """
        super().__init__(source_xml)

    def serialize(self, data_table: pa.Table) -> str:
        if self.source_xml:
            return self._update_source_xml(data_table, self.source_xml)
        else:
            logger.error("Cannot generate ALTO XML from scratch")
            raise NotImplementedError("Generating ALTO XML from scratch not yet implemented")

    def _update_source_xml(self, data_table: pa.Table, source_xml: str) -> str:
        root = ET.fromstring(source_xml)
        namespace = self._get_namespace(root)

        data = data_table.to_pydict()
        num_rows = len(data_table)

        for i in range(num_rows):
            if "word_id" in data and data["word_id"][i]:
                elem = root.find(f".//{namespace}String[@ID='{data['word_id'][i]}']")
            elif all(k in data for k in ["page_id", "region_id", "line_id"]):
                page_id = data.get("page_id", [None])[i]
                region_id = data.get("region_id", [None])[i]
                line_id = data.get("line_id", [None])[i]

                page = root.find(f".//{namespace}Page[@ID='{page_id}']") if page_id else None
                if not page:
                    continue

                region = (
                    page.find(f".//{namespace}TextBlock[@ID='{region_id}']") if region_id else None
                )
                if not region:
                    continue

                line = region.find(f".//{namespace}TextLine[@ID='{line_id}']") if line_id else None
                if not line:
                    continue

                strings = line.findall(f"{namespace}String")
                elem = strings[0] if strings else None
            else:
                elem = None

            if elem is not None:
                if "text" in data and data["text"][i] is not None:
                    elem.set("CONTENT", str(data["text"][i]))

                if "x" in data and data["x"][i] is not None:
                    elem.set("HPOS", str(data["x"][i]))
                if "y" in data and data["y"][i] is not None:
                    elem.set("VPOS", str(data["y"][i]))
                if "width" in data and data["width"][i] is not None:
                    elem.set("WIDTH", str(data["width"][i]))
                if "height" in data and data["height"][i] is not None:
                    elem.set("HEIGHT", str(data["height"][i]))

                if "confidence" in data and data["confidence"][i] is not None:
                    elem.set("WC", str(data["confidence"][i]))

        return ET.tostring(root, encoding="unicode")

    def _get_namespace(self, root: ET.Element) -> str:
        if root.tag.startswith("{"):
            return root.tag.split("}")[0] + "}"
        return ""
