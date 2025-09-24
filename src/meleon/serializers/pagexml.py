"""PageXML serializer."""

import logging
import xml.etree.ElementTree as ET
from typing import Optional
import pyarrow as pa

from .base import BaseSerializer

logger = logging.getLogger(__name__)


class PageXMLSerializer(BaseSerializer):
    def __init__(self, source_xml: Optional[str] = None):
        """
        Initialize PageXML serializer.

        Args:
            source_xml: Original PageXML to use as template for structure preservation.
                       If None, will attempt to generate XML from scratch (not yet implemented).
        """
        super().__init__(source_xml)

    def serialize(self, data_table: pa.Table) -> str:
        if self.source_xml:
            return self._update_source_xml(data_table, self.source_xml)
        else:
            logger.error("Cannot generate PageXML from scratch")
            raise NotImplementedError("Generating PageXML from scratch not yet implemented")

    def _update_source_xml(self, data_table: pa.Table, source_xml: str) -> str:
        root = ET.fromstring(source_xml)
        namespace = self._get_namespace(root)

        data = data_table.to_pydict()
        num_rows = len(data_table)

        for i in range(num_rows):
            if "word_id" in data and data["word_id"][i]:
                elem = root.find(f".//{namespace}Word[@id='{data['word_id'][i]}']")
            elif "line_id" in data and data["line_id"][i]:
                elem = root.find(f".//{namespace}TextLine[@id='{data['line_id'][i]}']")
            elif "region_id" in data and data["region_id"][i]:
                elem = root.find(f".//{namespace}TextRegion[@id='{data['region_id'][i]}']")
            else:
                elem = None

            if elem is not None:
                if "text" in data and data["text"][i] is not None:
                    text_equiv = elem.find(f"{namespace}TextEquiv")
                    if text_equiv is None:
                        text_equiv = ET.SubElement(elem, f"{namespace}TextEquiv")
                    unicode_elem = text_equiv.find(f"{namespace}Unicode")
                    if unicode_elem is None:
                        unicode_elem = ET.SubElement(text_equiv, f"{namespace}Unicode")
                    unicode_elem.text = str(data["text"][i])

                if "coords" in data and data["coords"][i] is not None:
                    coords_elem = elem.find(f"{namespace}Coords")
                    if coords_elem is None:
                        coords_elem = ET.SubElement(elem, f"{namespace}Coords")
                    coords_elem.set("points", str(data["coords"][i]))

                if "confidence" in data and data["confidence"][i] is not None:
                    elem.set("conf", str(data["confidence"][i]))

        return ET.tostring(root, encoding="unicode")

    def _get_namespace(self, root: ET.Element) -> str:
        if root.tag.startswith("{"):
            return root.tag.split("}")[0] + "}"
        return ""
