# Lab 3: ML Model Interface

## Overview

This lab demonstrates how to build an interactive machine learning model interface using Gradio and Databricks MLflow. You'll learn to load models from Unity Catalog Model Registry, make predictions, and create a user-friendly interface for ML model consumption.

## Learning Objectives

By completing this lab, you will:
- ✅ Load MLflow models from Unity Catalog Model Registry
- ✅ Create interactive prediction interfaces with Gradio
- ✅ Handle multiple input formats (JSON, CSV)
- ✅ Implement batch prediction capabilities
- ✅ Display model metadata and prediction history
- ✅ Respect OBO permissions for model access

## Prerequisites

- Completion of Lab 1 (Hello World)
- An MLflow model registered in Unity Catalog
- `EXECUTE` permission on the model
- Basic understanding of machine learning concepts

## Project Structure

```
lab3-ml-model-interface/
├── app.py              # Main Gradio application
├── app.yaml            # Databricks app configuration
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Configuration

### Step 1: Register a Model in MLflow

If you don't have a model yet, here's how to register one:

```python
import mlflow
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_iris

# Train a simple model
X, y = load_iris(return_X_y=True)
model = RandomForestClassifier()
model.fit(X, y)

# Log and register the model
with mlflow.start_run():
    mlflow.sklearn.log_model(
        model,
        "model",
        registered_model_name="main.ml_models.iris_classifier"
    )
```

### Step 2: Update app.yaml

Edit `app.yaml` and configure your model:

```yaml
env:
  - name: MODEL_NAME
    value: "main.ml_models.iris_classifier"  # Your model name
  
  - name: MODEL_VERSION
    value: "1"  # Or "Champion", "Staging", etc.
  
  - name: WAREHOUSE_ID
    value: "YOUR_WAREHOUSE_ID_HERE"
```

### Step 3: Grant Permissions

Ensure users have the necessary permissions:

```sql
-- Grant EXECUTE permission on the model
GRANT EXECUTE ON MODEL catalog.schema.model_name TO `user@example.com`;

-- For model aliases
ALTER MODEL catalog.schema.model_name 
SET ALIAS Champion FOR VERSION 1;
```

## Deployment

```bash
# Navigate to lab directory
cd apps/lab3-ml-model-interface

# Deploy the app
databricks apps deploy /Users/<your-email>/lab3-ml-model-interface

# View logs
databricks apps logs /Users/<your-email>/lab3-ml-model-interface
```

## Features

### 1. Model Configuration Tab

**Load MLflow Models:**
- Enter fully qualified model name
- Specify version number or alias
- View model metadata (version, status, tags)
- See model description and details

**Example:**
```
Model Name: main.ml_models.customer_churn
Version: Champion
```

### 2. JSON Prediction Tab

**Single Prediction:**
```json
{
  "age": 35,
  "income": 75000,
  "tenure_months": 24,
  "usage_gb": 45.5
}
```

**Batch Prediction:**
```json
[
  {"age": 35, "income": 75000, "tenure_months": 24},
  {"age": 42, "income": 95000, "tenure_months": 36}
]
```

**Features:**
- Syntax highlighting for JSON
- Demo input generator
- Real-time validation
- Formatted output with metadata

### 3. CSV Prediction Tab

**Upload CSV files for batch predictions:**

Input CSV:
```csv
age,income,tenure_months,usage_gb
35,75000,24,45.5
42,95000,36,60.2
28,55000,12,30.8
```

Output CSV includes predictions:
```csv
age,income,tenure_months,usage_gb,prediction
35,75000,24,45.5,0
42,95000,36,60.2,0
28,55000,12,30.8,1
```

**Features:**
- File upload interface
- Automatic prediction column addition
- Downloadable results
- Progress indication

### 4. Prediction History Tab

**Track recent predictions:**
- Timestamp of each prediction
- Number of input rows
- Preview of outputs
- Last 5 predictions shown

### 5. Help Tab

**Comprehensive documentation:**
- Step-by-step usage guide
- Input format examples
- Troubleshooting tips
- Best practices

## Usage Examples

### Example 1: Single Prediction

1. **Load Model:**
   - Model Name: `main.ml_models.iris_classifier`
   - Version: `1`
   - Click "Load Model"

2. **Make Prediction:**
   - Go to "JSON Prediction" tab
   - Enter:
     ```json
     {
       "sepal_length": 5.1,
       "sepal_width": 3.5,
       "petal_length": 1.4,
       "petal_width": 0.2
     }
     ```
   - Click "Predict"

3. **View Results:**
   ```json
   {
     "predictions": [0],
     "input_rows": 1,
     "model": "main.ml_models.iris_classifier",
     "version": "1"
   }
   ```

### Example 2: Batch Predictions from CSV

1. **Prepare CSV file** (`customers.csv`):
   ```csv
   customer_id,age,income,credit_score
   C001,35,75000,720
   C002,42,95000,780
   C003,28,55000,650
   ```

2. **Upload and Predict:**
   - Go to "CSV Prediction" tab
   - Upload `customers.csv`
   - Click "Predict from CSV"

3. **Download Results:**
   - Click download button
   - Get `customers_with_predictions.csv`

### Example 3: Using Model Aliases

Instead of version numbers, use aliases:

```yaml
# app.yaml
env:
  - name: MODEL_VERSION
    value: "Champion"  # Points to promoted version
```

Benefits:
- Automatic updates when you promote new versions
- No code changes needed for model updates
- Better production deployment practices

## Technical Implementation

### Model Loading

```python
class ModelInterface:
    def load_model(self) -> Tuple[bool, str]:
        model_uri = f"models:/{self.model_name}/{self.model_version}"
        self.model = mlflow.pyfunc.load_model(model_uri)
        
        # Model automatically uses OBO credentials
        # User must have EXECUTE permission
```

### Prediction Pipeline

```python
def predict(self, input_data: pd.DataFrame):
    predictions = self.model.predict(input_data)
    
    # Store in history for tracking
    self.prediction_history.append({
        "input": input_data,
        "output": predictions,
        "timestamp": datetime.now()
    })
    
    return predictions
```

### Security with OBO

All model access respects user permissions:

```python
# User A with EXECUTE permission: ✅ Can load and use model
# User B without permission: ❌ Gets permission denied error

# Audit logs show actual user making predictions
log_user_action("prediction_made", {
    "model": model_name,
    "user": user_email
})
```

## Model Input Requirements

### Understanding Input Schema

Models expect specific input formats. Check your model's signature:

```python
import mlflow

# Load model and check signature
model = mlflow.pyfunc.load_model(model_uri)
print(model.metadata.signature)
```

Example output:
```
inputs: 
  ['feature1': double, 'feature2': string, 'feature3': long]
outputs: 
  [double]
```

### Handling Different Input Types

**Numeric Features:**
```json
{"temperature": 72.5, "humidity": 0.65}
```

**Categorical Features:**
```json
{"category": "electronics", "brand": "acme"}
```

**Mixed Features:**
```json
{
  "age": 35,
  "income": 75000.0,
  "city": "San Francisco",
  "is_member": true
}
```

## Error Handling

### Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| "Model not found" | Wrong name/version | Verify model exists in registry |
| "Permission denied" | Lack of EXECUTE | Grant permission on model |
| "Invalid input schema" | Wrong features | Check model signature |
| "Dimension mismatch" | Wrong feature count | Provide all required features |

### Validation Examples

**Check Required Features:**
```python
expected_features = ["feature1", "feature2", "feature3"]
provided_features = list(input_df.columns)

if set(expected_features) != set(provided_features):
    raise ValueError("Feature mismatch")
```

## Performance Optimization

### Batch Processing

For large datasets, process in batches:

```python
batch_size = 1000
predictions = []

for i in range(0, len(df), batch_size):
    batch = df[i:i+batch_size]
    pred = model.predict(batch)
    predictions.extend(pred)
```

### Caching

Cache model loading for better performance:

```python
@functools.lru_cache(maxsize=1)
def get_model(model_name, version):
    return mlflow.pyfunc.load_model(f"models:/{model_name}/{version}")
```

## Model Versioning Best Practices

### Use Aliases for Stages

```python
# Promote model to Champion
from mlflow.tracking import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(
    name="main.ml_models.churn_predictor",
    alias="Champion",
    version="5"
)
```

### Version Management Strategy

1. **Development**: Test with specific versions (`version="1"`)
2. **Staging**: Use Staging alias (`version="Staging"`)
3. **Production**: Use Champion alias (`version="Champion"`)

### Rollback Strategy

```python
# Rollback to previous version
client.set_registered_model_alias(
    name="main.ml_models.churn_predictor",
    alias="Champion",
    version="4"  # Previous stable version
)
```

## Monitoring and Logging

### Prediction Logging

```python
log_user_action("prediction_made", {
    "model": model_name,
    "version": model_version,
    "input_rows": len(input_data),
    "user": user_email,
    "timestamp": datetime.now().isoformat()
})
```

### Performance Metrics

Track prediction latency:

```python
import time

start_time = time.time()
predictions = model.predict(input_data)
latency = time.time() - start_time

logger.info(f"Prediction latency: {latency:.3f}s for {len(input_data)} rows")
```

## Extension Ideas

Enhance this lab with:

1. **Model Comparison**: Load multiple models and compare predictions
2. **Visualization**: Add charts for prediction distributions
3. **A/B Testing**: Split traffic between model versions
4. **Confidence Scores**: Display prediction probabilities
5. **Feature Importance**: Show which features influenced predictions
6. **Model Monitoring**: Track prediction drift over time

## Troubleshooting

### Issue: "MLflow not available"

**Solution:**
```bash
pip install mlflow>=2.8.0
```

### Issue: Model loading timeout

**Solution:**
- Check network connectivity
- Verify model registry accessibility
- Increase timeout settings

### Issue: Predictions don't match expectations

**Solution:**
- Verify input feature names exactly match training data
- Check data types (int vs float)
- Ensure no missing values
- Review model's preprocessing requirements

## Next Steps

After completing this lab:

1. **Lab 4: Multi-User Dashboard** - Build interactive dashboards with Dash
2. **Lab 5: RESTful API** - Create APIs for model serving

## Additional Resources

- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)
- [Gradio Documentation](https://www.gradio.app/docs/)
- [Unity Catalog Models](https://docs.databricks.com/machine-learning/manage-model-lifecycle/index.html)
- [Model Serving Best Practices](https://docs.databricks.com/machine-learning/model-serving/index.html)

---

**Happy Predicting! 🤖**
