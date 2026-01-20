# Images and Diagrams

This folder contains supporting images and diagrams for the Online Feature Store lab.

## Suggested Screenshots

When delivering this lab, consider adding screenshots for:

1. **Lakebase Instance Creation**
   - Screenshot of Lakebase instance being created in Databricks workspace
   - Screenshot showing instance status as "ACTIVE"

2. **Feature Table Schema**
   - Screenshots of feature table schemas in Unity Catalog
   - Example data from user_features, item_features, and interaction_features tables

3. **Online Table Configuration**
   - Screenshot of online table creation dialog
   - Screenshot showing online table sync status

4. **Feature Engineering Client**
   - Screenshot of training set creation code and output
   - Example of feature lookups being configured

5. **Model Registration**
   - Screenshot of model in Unity Catalog with feature store metadata
   - Screenshot showing feature lineage in model details

6. **Inference with Feature Lookup**
   - Screenshot of inference input (only keys)
   - Screenshot of predictions output with automatically retrieved features
   - Performance benchmark results visualization

7. **Architecture Diagram**
   - High-level architecture showing offline → online flow
   - Data flow from feature tables to model serving

## Creating Diagrams

For architecture diagrams, you can use:
- **Mermaid**: For code-based diagrams (supported in Databricks notebooks)
- **draw.io**: For visual diagrams
- **Lucidchart**: For professional diagrams
- **PowerPoint/Keynote**: For presentation-ready diagrams

## Naming Convention

Use descriptive names for image files:
- `lakebase_instance_creation.png`
- `feature_table_schema_user.png`
- `online_table_sync_status.png`
- `model_feature_lineage.png`
- `inference_performance_benchmark.png`

## Image Formats

- Use **PNG** for screenshots and diagrams with transparency
- Use **JPEG** for photos or complex images
- Keep file sizes reasonable (< 1MB per image when possible)
- Use appropriate resolution (1920px width max for screenshots)
