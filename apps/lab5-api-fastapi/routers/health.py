"""Health check endpoints."""

from fastapi import APIRouter, status
from pydantic import BaseModel, Field
from datetime import datetime
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

router = APIRouter()


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str = Field(..., description="Service status", example="healthy")
    timestamp: str = Field(..., description="Current timestamp")
    version: str = Field(..., description="API version", example="1.0.0")
    environment: dict = Field(..., description="Environment information")


@router.get("/", response_model=HealthResponse, status_code=status.HTTP_200_OK)
async def health_check():
    """
    Health check endpoint.
    
    Returns the current status of the API service.
    """
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now().isoformat(),
        version="1.0.0",
        environment={
            "app_name": os.getenv("APP_NAME", "Databricks Data API"),
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            "databricks_host": os.getenv("DATABRICKS_HOST", "Not set")
        }
    )


@router.get("/ready", status_code=status.HTTP_200_OK)
async def readiness_check():
    """
    Readiness check endpoint.
    
    Indicates whether the service is ready to accept requests.
    """
    return {
        "ready": True,
        "timestamp": datetime.now().isoformat()
    }


@router.get("/live", status_code=status.HTTP_200_OK)
async def liveness_check():
    """
    Liveness check endpoint.
    
    Indicates whether the service is alive and running.
    """
    return {
        "alive": True,
        "timestamp": datetime.now().isoformat()
    }
