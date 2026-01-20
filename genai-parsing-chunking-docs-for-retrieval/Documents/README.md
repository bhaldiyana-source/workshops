# Sample Documents for Parsing and Chunking Lab

This directory contains sample documents for use in the Document Parsing, Transformation, and Chunking labs.

## Document Inventory

### 1. sample_technical_guide.txt
**Type**: Text (simulated PDF content)  
**Purpose**: Technical product documentation  
**Size**: ~3-4 pages equivalent  
**Content**: Product installation guide, setup instructions, troubleshooting  
**Best for**: Testing basic parsing and chunking workflows

### 2. sample_research_paper.txt
**Type**: Text (simulated PDF content)  
**Purpose**: Academic/research content  
**Size**: ~5-6 pages equivalent  
**Content**: AI research paper with abstract, sections, tables (as text)  
**Best for**: Testing longer documents, semantic chunking

### 3. sample_user_manual.txt
**Type**: Text (simulated DOCX content)  
**Purpose**: End-user documentation  
**Size**: ~4 pages equivalent  
**Content**: Step-by-step instructions, FAQs, safety information  
**Best for**: Testing structured document parsing

### 4. sample_whitepaper.txt
**Type**: Text (simulated PDF content)  
**Purpose**: Business/technical whitepaper  
**Size**: ~7-8 pages equivalent  
**Content**: Industry analysis, trends, recommendations  
**Best for**: Testing chunking optimization for long-form content

### 5. sample_contract.txt
**Type**: Text (simulated DOCX content)  
**Purpose**: Legal document  
**Size**: ~3 pages equivalent  
**Content**: Terms and conditions, clauses, definitions  
**Best for**: Testing precision chunking for legal text

## Usage in Labs

### Uploading to Unity Catalog Volume

```python
# Upload documents to volume
volume_path = "/Volumes/main/document_parsing_demo/sample_documents"

# Copy files using dbutils
for file in ["sample_technical_guide.txt", "sample_research_paper.txt", ...]:
    dbutils.fs.cp(f"file:/Workspace/path/to/Documents/{file}", 
                  f"{volume_path}/{file}")
```

### Parsing Documents

```sql
-- Parse all documents in volume
SELECT
  path as doc_path,
  ai_document_parse(content, 2) as parsed_content
FROM read_files('/Volumes/main/document_parsing_demo/sample_documents/*')
```

### Using with Helper Functions

```python
from utils.helper_functions import batch_parse_documents

# Parse all documents
stats = batch_parse_documents(
    volume_path="/Volumes/main/document_parsing_demo/sample_documents",
    output_table="parsed_documents",
    spark=spark
)
```

## Document Characteristics

| Document | Pages | Words | Characters | Tables | Use Case |
|----------|-------|-------|------------|--------|----------|
| Technical Guide | 4 | ~800 | ~5,000 | 1 | Installation/Setup |
| Research Paper | 6 | ~1,500 | ~10,000 | 2 | Academic Content |
| User Manual | 4 | ~1,000 | ~6,500 | 1 | End-user Docs |
| Whitepaper | 8 | ~2,000 | ~13,000 | 3 | Business Analysis |
| Contract | 3 | ~600 | ~4,000 | 0 | Legal Text |

## Chunking Recommendations

### By Document Type

**Technical Guides**
- Chunk size: 800-1000 characters
- Overlap: 150-200 characters
- Strategy: Section-aware chunking

**Research Papers**
- Chunk size: 1200-1500 characters
- Overlap: 200-300 characters
- Strategy: Paragraph-aware chunking

**User Manuals**
- Chunk size: 600-800 characters
- Overlap: 100-150 characters
- Strategy: Step-aware chunking

**Whitepapers**
- Chunk size: 1000-1500 characters
- Overlap: 200-300 characters
- Strategy: Semantic chunking

**Legal Documents**
- Chunk size: 500-800 characters
- Overlap: 100-200 characters
- Strategy: Clause-aware chunking

## Notes

### File Format
- Sample files are provided as `.txt` files for simplicity
- In production, you would work with actual PDF, DOCX, PPTX files
- The text content simulates the output of `ai_document_parse()`

### Customization
- You can add your own documents to this folder
- Supported formats: PDF, DOCX, PPTX, HTML
- Ensure files are less than 50MB for optimal performance

### Best Practices
1. Test with small documents first
2. Validate parsing output before chunking
3. Experiment with different chunk sizes
4. Monitor token counts for embedding limits
5. Add meaningful metadata for retrieval

## Additional Resources

- [Databricks AI Document Parse Documentation](https://docs.databricks.com/)
- [Unity Catalog Volumes Guide](https://docs.databricks.com/en/catalog/volumes.html)
- [LangChain Text Splitters](https://python.langchain.com/docs/modules/data_connection/document_transformers/)

## Questions or Issues?

If you encounter issues with the sample documents:
1. Check file permissions in the volume
2. Verify Unity Catalog access
3. Ensure AI functions are enabled in your workspace
4. Review the lab notebooks for troubleshooting guidance
