"""
Lab 3: ML Model Interface
A Gradio application for interacting with MLflow models deployed in Databricks.
Demonstrates model loading, inference, and result visualization.
"""

import gradio as gr
import pandas as pd
import numpy as np
import os
import sys
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import logging

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.auth import get_user_context, log_user_action
from utils.logging_config import setup_logging, get_logger

# Configure logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_logger(__name__)

# Try to import MLflow
try:
    import mlflow
    import mlflow.pyfunc
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    logger.warning("MLflow not available. Using mock mode.")


class ModelInterface:
    """Interface for loading and using MLflow models."""
    
    def __init__(self, model_name: Optional[str] = None, model_version: Optional[str] = None):
        """
        Initialize model interface.
        
        Args:
            model_name: Fully qualified model name (catalog.schema.model)
            model_version: Model version or alias (e.g., "1" or "Champion")
        """
        self.model_name = model_name or os.getenv("MODEL_NAME", "")
        self.model_version = model_version or os.getenv("MODEL_VERSION", "1")
        self.model = None
        self.model_info = {}
        self.prediction_history = []
    
    def load_model(self) -> Tuple[bool, str]:
        """
        Load the MLflow model.
        
        Returns:
            Tuple of (success, message)
        """
        if not MLFLOW_AVAILABLE:
            return False, "MLflow is not available. Install mlflow to use this feature."
        
        if not self.model_name:
            return False, "Model name not configured. Set MODEL_NAME environment variable."
        
        try:
            model_uri = f"models:/{self.model_name}/{self.model_version}"
            logger.info(f"Loading model from: {model_uri}")
            
            self.model = mlflow.pyfunc.load_model(model_uri)
            
            # Get model info
            client = mlflow.tracking.MlflowClient()
            try:
                model_details = client.get_model_version(
                    name=self.model_name,
                    version=self.model_version
                )
                self.model_info = {
                    "name": self.model_name,
                    "version": self.model_version,
                    "status": model_details.status,
                    "description": model_details.description or "No description",
                    "tags": model_details.tags
                }
            except Exception as e:
                logger.warning(f"Could not fetch model metadata: {str(e)}")
                self.model_info = {
                    "name": self.model_name,
                    "version": self.model_version,
                    "status": "Unknown",
                    "description": "Model loaded successfully",
                    "tags": {}
                }
            
            log_user_action("model_loaded", {"model": self.model_name, "version": self.model_version})
            return True, f"✅ Model loaded successfully: {self.model_name} (v{self.model_version})"
            
        except Exception as e:
            error_msg = f"❌ Failed to load model: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def predict(self, input_data: pd.DataFrame) -> Tuple[Optional[Any], str]:
        """
        Make predictions using the loaded model.
        
        Args:
            input_data: Input DataFrame
            
        Returns:
            Tuple of (predictions, message)
        """
        if self.model is None:
            return None, "Model not loaded. Please load a model first."
        
        try:
            predictions = self.model.predict(input_data)
            
            # Store in history
            self.prediction_history.append({
                "input": input_data.to_dict('records'),
                "output": predictions.tolist() if hasattr(predictions, 'tolist') else predictions,
                "timestamp": pd.Timestamp.now().isoformat()
            })
            
            log_user_action("prediction_made", {"model": self.model_name, "rows": len(input_data)})
            return predictions, "✅ Prediction completed successfully"
            
        except Exception as e:
            error_msg = f"❌ Prediction failed: {str(e)}"
            logger.error(error_msg)
            return None, error_msg
    
    def get_model_info_display(self) -> str:
        """Get formatted model information for display."""
        if not self.model_info:
            return "No model loaded"
        
        info = f"""
### Model Information

- **Name**: {self.model_info.get('name', 'N/A')}
- **Version**: {self.model_info.get('version', 'N/A')}
- **Status**: {self.model_info.get('status', 'N/A')}
- **Description**: {self.model_info.get('description', 'N/A')}

**Tags**:
"""
        tags = self.model_info.get('tags', {})
        if tags:
            for key, value in tags.items():
                info += f"\n- {key}: {value}"
        else:
            info += "\n- No tags available"
        
        return info


# Global model interface instance
model_interface = ModelInterface()


def load_model_ui(model_name: str, model_version: str) -> str:
    """Load model from UI inputs."""
    user = get_user_context()
    logger.info(f"User {user.user_email} loading model: {model_name} v{model_version}")
    
    if not model_name:
        return "❌ Please provide a model name"
    
    model_interface.model_name = model_name
    model_interface.model_version = model_version or "1"
    
    success, message = model_interface.load_model()
    
    if success:
        return message + "\n\n" + model_interface.get_model_info_display()
    else:
        return message


def predict_from_json(json_input: str) -> Tuple[str, str]:
    """
    Make predictions from JSON input.
    
    Args:
        json_input: JSON string with input features
        
    Returns:
        Tuple of (prediction output, status message)
    """
    user = get_user_context()
    
    if model_interface.model is None:
        return "", "❌ Please load a model first"
    
    try:
        # Parse JSON input
        data = json.loads(json_input)
        
        # Convert to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            return "", "❌ Invalid JSON format. Expected list or dict."
        
        # Make prediction
        predictions, message = model_interface.predict(df)
        
        if predictions is None:
            return "", message
        
        # Format output
        if isinstance(predictions, np.ndarray):
            predictions = predictions.tolist()
        
        output = {
            "predictions": predictions,
            "input_rows": len(df),
            "model": model_interface.model_name,
            "version": model_interface.model_version
        }
        
        output_json = json.dumps(output, indent=2)
        return output_json, message
        
    except json.JSONDecodeError as e:
        return "", f"❌ Invalid JSON: {str(e)}"
    except Exception as e:
        return "", f"❌ Error: {str(e)}"


def predict_from_csv(csv_file) -> Tuple[str, str]:
    """
    Make predictions from CSV file.
    
    Args:
        csv_file: Uploaded CSV file
        
    Returns:
        Tuple of (results CSV, status message)
    """
    if model_interface.model is None:
        return "", "❌ Please load a model first"
    
    try:
        # Read CSV
        df = pd.read_csv(csv_file.name)
        
        # Make prediction
        predictions, message = model_interface.predict(df)
        
        if predictions is None:
            return "", message
        
        # Add predictions to DataFrame
        df['prediction'] = predictions
        
        # Convert to CSV string
        csv_output = df.to_csv(index=False)
        
        return csv_output, message + f"\n\nProcessed {len(df)} rows"
        
    except Exception as e:
        return "", f"❌ Error: {str(e)}"


def get_prediction_history() -> str:
    """Get formatted prediction history."""
    if not model_interface.prediction_history:
        return "No predictions made yet"
    
    history = "### Recent Predictions\n\n"
    for i, pred in enumerate(reversed(model_interface.prediction_history[-5:]), 1):
        history += f"**Prediction {len(model_interface.prediction_history) - i + 1}**\n"
        history += f"- Time: {pred['timestamp']}\n"
        history += f"- Input rows: {len(pred['input'])}\n"
        history += f"- Output: {pred['output'][:3]}{'...' if len(pred['output']) > 3 else ''}\n\n"
    
    return history


def create_demo_input() -> str:
    """Create demo JSON input for testing."""
    demo_data = [
        {
            "feature1": 1.0,
            "feature2": 2.5,
            "feature3": 0.8,
            "feature4": "category_a"
        },
        {
            "feature1": 2.0,
            "feature2": 3.5,
            "feature3": 1.2,
            "feature4": "category_b"
        }
    ]
    return json.dumps(demo_data, indent=2)


def build_interface() -> gr.Blocks:
    """Build the Gradio interface."""
    
    user = get_user_context()
    
    # Custom CSS
    css = """
    .gradio-container {
        font-family: 'IBM Plex Sans', sans-serif;
    }
    .header {
        text-align: center;
        padding: 20px;
        background: linear-gradient(90deg, #FF3621 0%, #FF6B4A 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 20px;
    }
    .info-box {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    """
    
    with gr.Blocks(css=css, title="Lab 3: ML Model Interface") as demo:
        
        # Header
        gr.HTML("""
            <div class="header">
                <h1>🤖 ML Model Interface</h1>
                <p>Lab 3: Interactive MLflow Model Predictions</p>
            </div>
        """)
        
        # User info
        gr.Markdown(f"""
        <div class="info-box">
        
        **Current User**: {user.display_name} ({user.user_email})
        
        **About**: This interface allows you to load MLflow models from Unity Catalog and make predictions interactively.
        All model access respects your On-Behalf-Of (OBO) permissions.
        
        </div>
        """)
        
        # Model Configuration Tab
        with gr.Tab("🔧 Model Configuration"):
            gr.Markdown("## Load MLflow Model")
            gr.Markdown("Provide your model's fully qualified name and version/alias.")
            
            with gr.Row():
                with gr.Column():
                    model_name_input = gr.Textbox(
                        label="Model Name",
                        placeholder="catalog.schema.model_name",
                        value=os.getenv("MODEL_NAME", ""),
                        info="Fully qualified model name in Unity Catalog"
                    )
                    model_version_input = gr.Textbox(
                        label="Model Version or Alias",
                        placeholder="1 or Champion",
                        value=os.getenv("MODEL_VERSION", "1"),
                        info="Version number or alias (e.g., 'Champion', 'Staging')"
                    )
                    load_btn = gr.Button("🔄 Load Model", variant="primary", size="lg")
                
                with gr.Column():
                    model_info_output = gr.Markdown("### Model Information\n\nNo model loaded yet")
            
            load_btn.click(
                fn=load_model_ui,
                inputs=[model_name_input, model_version_input],
                outputs=model_info_output
            )
        
        # JSON Prediction Tab
        with gr.Tab("📝 JSON Prediction"):
            gr.Markdown("## Make Predictions with JSON Input")
            gr.Markdown("Provide input features as JSON. The format should match your model's expected input schema.")
            
            with gr.Row():
                with gr.Column():
                    json_input = gr.Code(
                        label="Input JSON",
                        language="json",
                        value=create_demo_input(),
                        lines=15
                    )
                    
                    with gr.Row():
                        predict_json_btn = gr.Button("🚀 Predict", variant="primary")
                        demo_btn = gr.Button("📋 Load Demo Input")
                    
                    demo_btn.click(
                        fn=create_demo_input,
                        outputs=json_input
                    )
                
                with gr.Column():
                    json_output = gr.Code(
                        label="Prediction Output",
                        language="json",
                        lines=15
                    )
                    json_status = gr.Textbox(label="Status", lines=2)
            
            predict_json_btn.click(
                fn=predict_from_json,
                inputs=json_input,
                outputs=[json_output, json_status]
            )
        
        # CSV Prediction Tab
        with gr.Tab("📊 CSV Prediction"):
            gr.Markdown("## Batch Predictions with CSV")
            gr.Markdown("Upload a CSV file with input features. The output will include predictions for each row.")
            
            with gr.Row():
                with gr.Column():
                    csv_input = gr.File(
                        label="Upload CSV File",
                        file_types=[".csv"],
                        type="filepath"
                    )
                    predict_csv_btn = gr.Button("🚀 Predict from CSV", variant="primary")
                
                with gr.Column():
                    csv_output = gr.Textbox(
                        label="Results (CSV)",
                        lines=15,
                        max_lines=20
                    )
                    csv_status = gr.Textbox(label="Status", lines=2)
                    download_btn = gr.File(label="Download Results")
            
            predict_csv_btn.click(
                fn=predict_from_csv,
                inputs=csv_input,
                outputs=[csv_output, csv_status]
            )
        
        # History Tab
        with gr.Tab("📜 Prediction History"):
            gr.Markdown("## Recent Predictions")
            gr.Markdown("View your recent prediction requests and results.")
            
            history_output = gr.Markdown("No predictions made yet")
            refresh_history_btn = gr.Button("🔄 Refresh History")
            
            refresh_history_btn.click(
                fn=get_prediction_history,
                outputs=history_output
            )
        
        # Help Tab
        with gr.Tab("❓ Help"):
            gr.Markdown("""
            ## How to Use This Interface
            
            ### Step 1: Configure Model
            1. Go to the **Model Configuration** tab
            2. Enter your model's fully qualified name (e.g., `main.ml_models.my_classifier`)
            3. Enter the version or alias (e.g., `1` or `Champion`)
            4. Click **Load Model**
            
            ### Step 2: Make Predictions
            
            **Option A: JSON Input**
            1. Go to the **JSON Prediction** tab
            2. Enter your input features as JSON
            3. Click **Predict**
            
            **Option B: CSV File**
            1. Go to the **CSV Prediction** tab
            2. Upload a CSV file with input features
            3. Click **Predict from CSV**
            4. Download the results with predictions
            
            ### JSON Input Format
            
            Single prediction:
            ```json
            {
                "feature1": 1.0,
                "feature2": "value",
                "feature3": true
            }
            ```
            
            Multiple predictions:
            ```json
            [
                {"feature1": 1.0, "feature2": "value"},
                {"feature1": 2.0, "feature2": "other"}
            ]
            ```
            
            ### CSV Input Format
            
            ```csv
            feature1,feature2,feature3
            1.0,value1,100
            2.0,value2,200
            ```
            
            ### Troubleshooting
            
            **"Model not found"**
            - Verify the model name and version
            - Check Unity Catalog permissions
            - Ensure the model is registered
            
            **"Permission denied"**
            - You need `EXECUTE` permission on the model
            - Contact your workspace administrator
            
            **"Invalid input format"**
            - Check feature names match model expectations
            - Verify data types (numeric, string, etc.)
            - Review model's input schema
            
            ### Best Practices
            
            ✅ Test with small batches first  
            ✅ Validate input format before large batches  
            ✅ Review prediction history regularly  
            ✅ Use model aliases for production deployments  
            
            ### Security Note
            
            All model access uses your Databricks credentials (OBO). You can only access models you have permission to use.
            """)
        
        # Footer
        gr.Markdown("---")
        gr.Markdown("**Databricks Apps Workshop - Lab 3: ML Model Interface** | Built with Gradio")
    
    return demo


def main():
    """Main application entry point."""
    logger.info("Starting ML Model Interface application")
    
    # Build and launch interface
    demo = build_interface()
    
    # Launch on port 8080
    demo.launch(
        server_name="0.0.0.0",
        server_port=8080,
        show_error=True
    )


if __name__ == "__main__":
    main()
