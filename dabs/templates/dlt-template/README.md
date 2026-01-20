# DLT Pipeline Template

Template for creating Delta Live Tables pipelines with medallion architecture (bronze → silver → gold).

## Setup Instructions

1. **Copy this template**:
   ```bash
   cp -r templates/dlt-template my-dlt-pipeline
   cd my-dlt-pipeline
   ```

2. **Customize databricks.yml**:
   Replace placeholders:
   - `{{project_name}}`: Project name
   - `{{pipeline_name}}`: Pipeline identifier
   - `{{pipeline_display_name}}`: Display name
   - `{{data_engineers_group}}`: Data engineers group name
   - `{{analysts_group}}`: Analysts group name
   - `{{viewers_group}}`: Viewers group name
   - `{{notification_email}}`: Email for alerts

3. **Customize DLT notebooks**:
   - `src/bronze_layer.py`: Add your source tables
   - `src/silver_layer.py`: Add cleansing logic
   - `src/gold_layer.py`: Add aggregations
   
   Replace `{{entity_name}}` and `{{metric_name}}` with your actual names

4. **Deploy**:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   ```

## Template Features

- ✅ Medallion architecture (bronze/silver/gold)
- ✅ Unity Catalog schemas and volumes
- ✅ Auto Loader for streaming ingestion
- ✅ Data quality expectations
- ✅ Automated schema evolution
- ✅ Email notifications
- ✅ Multi-environment support

## Architecture

```
Raw Files → Bronze (DLT) → Silver (DLT) → Gold (DLT)
  (Volume)    (Raw data)     (Cleaned)      (Aggregated)
```

## Customization Tips

- Add more bronze tables by copying the template
- Customize data quality expectations in silver layer
- Add business-specific aggregations in gold layer
- Adjust cluster autoscaling based on data volume
- Switch to continuous mode for real-time processing
