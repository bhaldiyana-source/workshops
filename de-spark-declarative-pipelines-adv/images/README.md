# Images Directory

This directory contains visual assets and diagrams for the **Advanced Techniques for Lakeflow Declarative Pipelines** workshop.

## Purpose

Store images, diagrams, and screenshots that support the workshop notebooks:
- Pipeline architecture diagrams
- Data flow visualizations
- UI screenshots for demonstrations
- Concept illustrations
- Before/after comparison images

## File Naming Convention

Use descriptive names that indicate which notebook or concept the image supports:

- `multi_flow_architecture.png` - Multi-flow pipeline architecture diagram
- `liquid_clustering_concept.png` - Liquid Clustering illustration
- `scd_type1_vs_type2.png` - SCD Type comparison diagram
- `cdc_flow_diagram.png` - CDC process flow
- `auto_cdc_example.png` - AUTO CDC INTO example

## Supported Formats

- PNG (preferred for screenshots and diagrams)
- JPG/JPEG (for photos)
- SVG (for vector graphics)
- GIF (for animated demonstrations)

## Usage in Notebooks

Reference images in notebook markdown cells using:

```markdown
![Alt text](./images/filename.png)
```

Or with HTML for more control:

```html
<img src="./images/filename.png" alt="Description" width="600"/>
```

## Best Practices

1. **Optimize file sizes**: Compress images before adding them
2. **Use descriptive names**: Make filenames self-explanatory
3. **Add alt text**: Always provide meaningful descriptions
4. **Maintain resolution**: Use appropriate resolution for clarity (typically 1200-1600px wide)
5. **Version control**: Commit images along with related notebook changes

## Contributing

When adding new images:
1. Ensure they are relevant to the workshop content
2. Follow the naming convention
3. Update this README if adding a new category of images
4. Verify images display correctly in Databricks notebooks
