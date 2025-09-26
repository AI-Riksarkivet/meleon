"""Base XML parser with common functionality for ALTO and PageXML."""

from .base import BaseParser


class BaseXMLParser(BaseParser):
    """Base class for XML-based parsers (ALTO, PageXML) with common utilities."""

    def _get_namespace(self, root):
        """
        Extract namespace from root element.

        Args:
            root: XML root element

        Returns:
            Namespace string with curly braces, or empty string if no namespace
        """
        if root.tag.startswith("{"):
            return root.tag.split("}")[0] + "}"
        return ""

    def _get_text_from_element(self, element, namespace, text_path):
        """
        Extract text from nested XML structure.

        Args:
            element: Starting XML element
            namespace: XML namespace string
            text_path: List of element names to traverse, e.g., ["TextEquiv", "Unicode"]

        Returns:
            Text content or empty string if not found
        """
        current = element
        for tag in text_path:
            current = current.find(f"{namespace}{tag}")
            if current is None:
                return ""
        return current.text or ""

    def _get_attribute_safe(self, element, attr_name, default=""):
        """
        Safely get attribute from element.

        Args:
            element: XML element
            attr_name: Name of attribute to retrieve
            default: Default value if attribute not found

        Returns:
            Attribute value or default
        """
        return element.get(attr_name, default) if element is not None else default

    def _get_int_attribute(self, element, attr_name, default=0):
        """
        Get integer attribute from element.

        Args:
            element: XML element
            attr_name: Name of attribute to retrieve
            default: Default value if attribute not found or not numeric

        Returns:
            Integer value or default
        """
        value = self._get_attribute_safe(element, attr_name, str(default))
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _get_float_attribute(self, element, attr_name, default=None):
        """
        Get float attribute from element.

        Args:
            element: XML element
            attr_name: Name of attribute to retrieve
            default: Default value if attribute not found or not numeric

        Returns:
            Float value or default
        """
        value = element.get(attr_name) if element is not None else None
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _parse_coords(self, coords_str):
        """
        Parse coordinate string to bounding box.

        Common for PageXML format where coords are specified as point pairs.

        Args:
            coords_str: String with space-separated coordinate pairs "x1,y1 x2,y2 ..."

        Returns:
            Tuple of (x, y, width, height) representing bounding box
        """
        if not coords_str:
            return 0, 0, 0, 0

        points = []
        try:
            for pair in coords_str.split():
                if "," in pair:
                    x, y = pair.split(",", 1)
                    points.append((int(x), int(y)))
        except (ValueError, TypeError):
            return 0, 0, 0, 0

        if not points:
            return 0, 0, 0, 0

        xs = [p[0] for p in points]
        ys = [p[1] for p in points]

        min_x, min_y = min(xs), min(ys)
        max_x, max_y = max(xs), max(ys)

        return min_x, min_y, max_x - min_x, max_y - min_y

    def _find_element_safe(self, parent, path, namespace=""):
        """
        Safely find element in XML tree.

        Args:
            parent: Parent element to search from
            path: XPath-like path to element
            namespace: Optional namespace prefix

        Returns:
            Found element or None
        """
        if parent is None:
            return None

        if namespace:
            full_path = path.replace("/", f"/{namespace}")
            if not full_path.startswith(namespace):
                full_path = f"{namespace}{full_path}"
        else:
            full_path = path

        return parent.find(full_path)

    def _findall_safe(self, parent, path, namespace=""):
        """
        Safely find all elements matching path.

        Args:
            parent: Parent element to search from
            path: XPath-like path to elements
            namespace: Optional namespace prefix

        Returns:
            List of found elements (empty if none found)
        """
        if parent is None:
            return []

        if namespace:
            full_path = path.replace("/", f"/{namespace}")
            if not full_path.startswith(namespace) and not full_path.startswith(".//"):
                full_path = f"{namespace}{full_path}"
        else:
            full_path = path

        return parent.findall(full_path) or []
