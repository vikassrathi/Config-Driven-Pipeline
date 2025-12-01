"""
Configuration management for Self-Service Fabric Pipeline.
Loads configuration from environment variables with secure defaults.
"""
import os
from typing import Optional
from pathlib import Path

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    env_path = Path('.') / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ImportError:
    # python-dotenv not installed, will use system environment variables only
    pass


class Config:
    """Central configuration class for application settings."""

    # Azure AD / Fabric Authentication
    FABRIC_TENANT_ID: str = os.getenv('FABRIC_TENANT_ID', '')
    FABRIC_CLIENT_ID: str = os.getenv('FABRIC_CLIENT_ID', '')
    FABRIC_CLIENT_SECRET: str = os.getenv('FABRIC_CLIENT_SECRET', '')

    # Fabric Workspace and Lakehouse
    FABRIC_WORKSPACE_ID: str = os.getenv('FABRIC_WORKSPACE_ID', '')
    FABRIC_WORKSPACE_NAME: str = os.getenv('FABRIC_WORKSPACE_NAME', 'selfservingpipeline')
    FABRIC_LAKEHOUSE_ID: str = os.getenv('FABRIC_LAKEHOUSE_ID', '')
    FABRIC_LAKEHOUSE_NAME: str = os.getenv('FABRIC_LAKEHOUSE_NAME', 'selfserving')

    # Application Settings
    DEV_MODE_SKIP_LOGIN: bool = os.getenv('DEV_MODE_SKIP_LOGIN', 'False').lower() == 'true'

    # Mock Users for Development (should be removed in production)
    MOCK_USERS: dict = {
        "user1": {
            "password": os.getenv('MOCK_USER1_PASSWORD', ''),
            "token": "jwt_token_for_user1_123"
        },
        "admin": {
            "password": os.getenv('MOCK_ADMIN_PASSWORD', ''),
            "token": "jwt_token_for_admin_789"
        }
    }

    # API Endpoints
    FABRIC_API_BASE_URL: str = "https://api.fabric.microsoft.com"
    ONELAKE_DFS_BASE_URL: str = "https://onelake.dfs.fabric.microsoft.com"

    # Authentication Scopes
    FABRIC_SCOPE: list = ["https://api.fabric.microsoft.com/.default"]
    ONELAKE_SCOPE: list = ["https://storage.azure.com/.default"]

    @classmethod
    def validate_config(cls) -> tuple[bool, list[str]]:
        """
        Validates required configuration parameters.

        Returns:
            tuple: (is_valid, missing_params)
        """
        required_params = {
            'FABRIC_TENANT_ID': cls.FABRIC_TENANT_ID,
            'FABRIC_CLIENT_ID': cls.FABRIC_CLIENT_ID,
        }

        missing = [key for key, value in required_params.items() if not value]
        is_valid = len(missing) == 0

        return is_valid, missing

    @classmethod
    def get_connection_string(cls) -> dict:
        """
        Returns connection configuration as dictionary.

        Returns:
            dict: Connection parameters
        """
        return {
            'tenant_id': cls.FABRIC_TENANT_ID,
            'client_id': cls.FABRIC_CLIENT_ID,
            'client_secret': cls.FABRIC_CLIENT_SECRET,
            'workspace_id': cls.FABRIC_WORKSPACE_ID,
            'workspace_name': cls.FABRIC_WORKSPACE_NAME,
            'lakehouse_id': cls.FABRIC_LAKEHOUSE_ID,
            'lakehouse_name': cls.FABRIC_LAKEHOUSE_NAME,
        }


# Default configuration instance
config = Config()