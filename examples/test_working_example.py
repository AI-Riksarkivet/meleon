#!/usr/bin/env python3
"""
Working example showing the full pipeline logic
(without requiring PyArrow installation)
"""

import xml.etree.ElementTree as ET

print("=" * 70)
print("WORKING EXAMPLE: ALTO Processing Pipeline")
print("=" * 70)

# Step 1: Parse ALTO files and extract data
print("\n1️⃣ PARSING ALTO FILES:")
print("-" * 40)

test_files = ["test_data/sample1.xml", "test_data/sample2.xml"]
all_words = []

for file_path in test_files:
    print(f"\n📄 Processing: {file_path}")

    tree = ET.parse(file_path)
    root = tree.getroot()
    namespace = "{http://www.loc.gov/standards/alto/ns-v3#}"

    # Extract page info
    page = root.find(f".//{namespace}Page")
    page_id = page.get("ID") if page is not None else None
    page_width = page.get("WIDTH") if page is not None else None
    page_height = page.get("HEIGHT") if page is not None else None

    print(f"  Page ID: {page_id}, Size: {page_width}x{page_height}")

    # Extract all words
    for block in root.findall(f".//{namespace}TextBlock"):
        block_id = block.get("ID")

        for line in block.findall(f".//{namespace}TextLine"):
            line_id = line.get("ID")

            for string in line.findall(f".//{namespace}String"):
                word_data = {
                    "page_id": page_id,
                    "block_id": block_id,
                    "line_id": line_id,
                    "word_id": string.get("ID"),
                    "text": string.get("CONTENT"),
                    "x": int(string.get("HPOS")),
                    "y": int(string.get("VPOS")),
                    "width": int(string.get("WIDTH")),
                    "height": int(string.get("HEIGHT")),
                    "confidence": float(string.get("WC")) if string.get("WC") else None,
                }
                all_words.append(word_data)

print(f"\n✅ Total words extracted: {len(all_words)}")

# Step 2: Filter by confidence > 0.95
print("\n2️⃣ FILTERING DATA (WC > 0.95):")
print("-" * 40)

filtered_words = [w for w in all_words if w["confidence"] and w["confidence"] > 0.95]

print(f"  Original words: {len(all_words)}")
print(f"  Filtered words: {len(filtered_words)}")
print(f"  Removed: {len(all_words) - len(filtered_words)} words")

print("\n  Kept words:")
for w in filtered_words:
    print(f"    • '{w['text']}' (WC={w['confidence']:.2f})")

# Step 3: Create new ALTO XML
print("\n3️⃣ SERIALIZING TO ALTO:")
print("-" * 40)

# Build new ALTO structure
alto_root = ET.Element("alto", xmlns="http://www.loc.gov/standards/alto/ns-v3#")

# Add description
desc = ET.SubElement(alto_root, "Description")
ET.SubElement(desc, "MeasurementUnit").text = "pixel"

# Add layout
layout = ET.SubElement(alto_root, "Layout")

# Group filtered words by page
pages = {}
for word in filtered_words:
    page_id = word["page_id"]
    if page_id not in pages:
        pages[page_id] = []
    pages[page_id].append(word)

# Create page structure
for page_id, page_words in pages.items():
    page_elem = ET.SubElement(layout, "Page", ID=page_id, WIDTH="1000", HEIGHT="1500")
    printspace = ET.SubElement(page_elem, "PrintSpace")

    # Group by block and line
    blocks = {}
    for word in page_words:
        block_id = word["block_id"]
        if block_id not in blocks:
            blocks[block_id] = {}

        line_id = word["line_id"]
        if line_id not in blocks[block_id]:
            blocks[block_id][line_id] = []

        blocks[block_id][line_id].append(word)

    # Create block/line/word structure
    for block_id, lines in blocks.items():
        block_elem = ET.SubElement(printspace, "TextBlock", ID=block_id)

        for line_id, words in lines.items():
            line_elem = ET.SubElement(block_elem, "TextLine", ID=line_id)

            for word in words:
                ET.SubElement(
                    line_elem,
                    "String",
                    ID=word["word_id"],
                    CONTENT=word["text"],
                    HPOS=str(word["x"]),
                    VPOS=str(word["y"]),
                    WIDTH=str(word["width"]),
                    HEIGHT=str(word["height"]),
                    WC=str(word["confidence"]),
                )

# Save ALTO
alto_output = "test_data/output_filtered.alto.xml"
tree = ET.ElementTree(alto_root)
ET.indent(tree, space="  ")
tree.write(alto_output, encoding="UTF-8", xml_declaration=True)

print(f"  ✅ Saved filtered ALTO to: {alto_output}")

# Step 4: Create PageXML
print("\n4️⃣ SERIALIZING TO PAGEXML:")
print("-" * 40)

# Build PageXML structure
pc_ns = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
pagexml_root = ET.Element("PcGts", xmlns=pc_ns)

# Add metadata
metadata = ET.SubElement(pagexml_root, "Metadata")
ET.SubElement(metadata, "Creator").text = "Meleon Converter"

# Create page
for page_id, page_words in pages.items():
    page_elem = ET.SubElement(
        pagexml_root, "Page", imageFilename=f"{page_id}.jpg", imageWidth="1000", imageHeight="1500"
    )

    # Group by regions (blocks)
    for block_id, lines in blocks.items():
        # Calculate region coordinates
        all_coords = []
        for line_words in lines.values():
            for w in line_words:
                all_coords.extend(
                    [
                        (w["x"], w["y"]),
                        (w["x"] + w["width"], w["y"]),
                        (w["x"] + w["width"], w["y"] + w["height"]),
                        (w["x"], w["y"] + w["height"]),
                    ]
                )

        if all_coords:
            min_x = min(c[0] for c in all_coords)
            min_y = min(c[1] for c in all_coords)
            max_x = max(c[0] for c in all_coords)
            max_y = max(c[1] for c in all_coords)

            region_coords = f"{min_x},{min_y} {max_x},{min_y} {max_x},{max_y} {min_x},{max_y}"

            region = ET.SubElement(page_elem, "TextRegion", id=block_id, type="paragraph")
            ET.SubElement(region, "Coords", points=region_coords)

            # Add text lines
            for line_id, words in lines.items():
                if words:
                    # Calculate line coords
                    min_x = min(w["x"] for w in words)
                    min_y = min(w["y"] for w in words)
                    max_x = max(w["x"] + w["width"] for w in words)
                    max_y = max(w["y"] + w["height"] for w in words)

                    line_coords = f"{min_x},{min_y} {max_x},{min_y} {max_x},{max_y} {min_x},{max_y}"

                    line_elem = ET.SubElement(
                        region, "TextLine", id=line_id, custom="readingOrder {index:0;}"
                    )
                    ET.SubElement(line_elem, "Coords", points=line_coords)

                    # Add text content
                    text_equiv = ET.SubElement(line_elem, "TextEquiv")
                    ET.SubElement(text_equiv, "Unicode").text = " ".join(w["text"] for w in words)

                    # Add individual words
                    for word in words:
                        word_coords = (
                            f"{word['x']},{word['y']} {word['x'] + word['width']},{word['y']} "
                            f"{word['x'] + word['width']},{word['y'] + word['height']} "
                            f"{word['x']},{word['y'] + word['height']}"
                        )

                        word_elem = ET.SubElement(
                            line_elem, "Word", id=word["word_id"], conf=str(word["confidence"])
                        )
                        ET.SubElement(word_elem, "Coords", points=word_coords)

                        word_text = ET.SubElement(word_elem, "TextEquiv")
                        ET.SubElement(word_text, "Unicode").text = word["text"]

# Save PageXML
pagexml_output = "test_data/output_filtered.page.xml"
tree = ET.ElementTree(pagexml_root)
ET.indent(tree, space="  ")
tree.write(pagexml_output, encoding="UTF-8", xml_declaration=True)

print(f"  ✅ Saved filtered PageXML to: {pagexml_output}")

# Summary
print("\n" + "=" * 70)
print("📊 PIPELINE COMPLETE!")
print("=" * 70)
print(f"""
Input:
  • 2 ALTO files
  • {len(all_words)} total words

Processing:
  • Filtered by WC > 0.95
  • Kept {len(filtered_words)} words ({len(filtered_words) / len(all_words) * 100:.1f}%)

Output:
  • {alto_output}
  • {pagexml_output}

✅ Successfully demonstrated:
  1. Loading multiple ALTO XML files
  2. Data transformation (confidence filtering)
  3. Serialization to ALTO format
  4. Serialization to PageXML format
""")
