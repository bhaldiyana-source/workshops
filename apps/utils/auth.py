"""Authentication and authorization utilities."""

import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class UserContext:
    """Manages user context and authentication information."""
    
    def __init__(self):
        """Initialize user context from environment variables."""
        self.user_email = os.getenv("DATABRICKS_USER_EMAIL")
        self.user_id = os.getenv("DATABRICKS_USER_ID")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.host = os.getenv("DATABRICKS_HOST")
        
        logger.info(f"User context initialized for: {self.user_email or 'Unknown user'}")
    
    @property
    def is_authenticated(self) -> bool:
        """Check if user is authenticated."""
        return bool(self.token and self.user_email)
    
    @property
    def display_name(self) -> str:
        """Get display name for the user."""
        if self.user_email:
            # Extract name from email (e.g., "john.doe@company.com" -> "John Doe")
            name_part = self.user_email.split('@')[0]
            return ' '.join(word.capitalize() for word in name_part.split('.'))
        return "Guest"
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Get user context as dictionary.
        
        Returns:
            Dictionary with user information (excluding token)
        """
        return {
            'email': self.user_email,
            'user_id': self.user_id,
            'display_name': self.display_name,
            'host': self.host,
            'is_authenticated': self.is_authenticated
        }
    
    def __repr__(self) -> str:
        """String representation of user context."""
        return f"UserContext(email={self.user_email}, display_name={self.display_name})"


def get_user_context() -> UserContext:
    """
    Get current user context.
    
    Returns:
        UserContext instance
    """
    return UserContext()


def check_permission(required_email: Optional[str] = None) -> bool:
    """
    Check if current user has required permissions.
    
    Args:
        required_email: Required email address (for demo purposes)
        
    Returns:
        True if user has permission, False otherwise
    """
    user = get_user_context()
    
    if not user.is_authenticated:
        logger.warning("User is not authenticated")
        return False
    
    if required_email and user.user_email != required_email:
        logger.warning(f"User {user.user_email} does not match required email {required_email}")
        return False
    
    return True


def log_user_action(action: str, details: Optional[Dict[str, Any]] = None):
    """
    Log user action for audit trail.
    
    Args:
        action: Action description
        details: Additional details dictionary
    """
    user = get_user_context()
    log_data = {
        'user': user.user_email or 'Unknown',
        'action': action,
        'details': details or {}
    }
    logger.info(f"User action: {log_data}")
