# Images and Supporting Materials

This directory contains diagrams, screenshots, and other visual materials to support the Building Single Agents lab.

## Contents

This folder is intended to hold:

### Architecture Diagrams
- Agent execution flow diagrams
- LangChain vs DSPy comparison visuals
- Mosaic AI Agent Framework architecture
- Tool calling sequence diagrams

### Screenshots
- MLflow trace examples
- Databricks workspace setup screenshots
- Agent execution results
- Unity Catalog function configurations

### Usage in Notebooks

To reference images in your Databricks notebooks, use:

```python
# Display image in a notebook
displayHTML('<img src="files/images/your_image.png" width="800"/>')
```

Or in markdown cells:

```markdown
![Architecture Diagram](files/images/architecture.png)
```

## Adding Images

When adding new images:

1. Use descriptive filenames (e.g., `langchain_agent_execution_flow.png`)
2. Keep file sizes reasonable (< 500KB when possible)
3. Use PNG for screenshots and diagrams
4. Use JPG for photos
5. Document what each image represents

## Image Guidelines

### For Diagrams
- Use clear, high-contrast colors
- Include labels and legends
- Keep text readable at various zoom levels
- Export at appropriate resolution (150-300 DPI)

### For Screenshots
- Crop to relevant content
- Highlight important areas with arrows or boxes
- Use annotations sparingly
- Ensure text is legible

## Tools for Creating Diagrams

Recommended tools:
- **Mermaid**: For flowcharts and sequence diagrams (can be embedded in notebooks)
- **Draw.io**: For architecture diagrams
- **Excalidraw**: For hand-drawn style diagrams
- **Lucidchart**: For professional diagrams

## License

All images in this directory are for educational use as part of the Databricks Academy training materials.
