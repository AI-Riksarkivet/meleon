# 🦎 Meleon (WIP)

<div align="center">
  <img src="assets/meleon.jpg" alt="Meleon Logo" width="250">
</div>


*Transform OCR/HTR into lightning-fast data formats - Like a chameleon adapts, Meleon transforms*

---

## 🦎 **Why Meleon?** 🦎

**The Problem:** You have thousands (or millions) of OCR XML files (ALTO, PageXML) from digitization projects. They're huge, slow to parse, and hard to analyze at scale.


## 🚀 **Key Features**

### ⚡ **Blazing Fast Performance**
- Stream data without loading everything into memory
- Parallel processing with PyArrow's native threading
- Narwhals for data transformation
- Serialize to preferred format


### 🔄 **Bidirectional Conversion**
```python
# XML → PyArrow
table = meleon.parse("document.xml", parser)

data_transformation stuff... on table --> transformed_table

# PyArrow → XML
xml_string = meleon.serialize(transformed_table, serializer)
```
## 🏗️ **Architecture**

Meleon uses a clean, extensible architecture:

```
Input Layer (XML Files)
    ↓
Parser Layer (ALTO/PageXML → PyArrow)
    ↓
Processing Layer (Batch/Stream/Transform)
    ↓
Output Layer (Parquet/Dataset/Stream)
```

- **Dependency Injection**: Configure once, use everywhere
- **Schema-Driven**: Type-safe, predictable outputs
- **Generator-Based**: Process unlimited data with fixed memory
- **Zero-Copy**: Leverage PyArrow's efficient memory handling


See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed technical design.
