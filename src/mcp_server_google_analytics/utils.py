"""
Utility functions for Google Analytics MCP Server.

Contains common utilities for authentication, data processing, and 
protocol buffer conversion.
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta

from google.protobuf import message as proto
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient

# Configure logging
logger = logging.getLogger(__name__)

# Global client instances
analytics_client: Optional[BetaAnalyticsDataClient] = None
admin_client: Optional[AnalyticsAdminServiceClient] = None
default_property_id: Optional[str] = None


def proto_to_dict(obj: proto.Message) -> Dict[str, Any]:
    """Converts a proto message to a dictionary."""
    return type(obj).to_dict(
        obj, use_integers_for_enums=False, preserving_proto_field_name=True
    )


def proto_to_json(obj: proto.Message) -> str:
    """Converts a proto message to a JSON string."""
    return type(obj).to_json(obj, indent=None, preserving_proto_field_name=True)


def initialize_client():
    """Initialize Google Analytics client with service account credentials."""
    global analytics_client, admin_client, default_property_id
    
    try:
        # Get environment variables
        client_email = os.getenv("GOOGLE_CLIENT_EMAIL")
        private_key = os.getenv("GOOGLE_PRIVATE_KEY")
        property_id = os.getenv("GA_PROPERTY_ID")
        
        if not all([client_email, private_key, property_id]):
            raise ValueError(
                "Missing required environment variables: "
                "GOOGLE_CLIENT_EMAIL, GOOGLE_PRIVATE_KEY, GA_PROPERTY_ID"
            )
        
        # Parse private key (handle escaped newlines)
        private_key = private_key.replace("\\n", "\n")
        
        # Create service account credentials
        credentials_info = {
            "type": "service_account",
            "client_email": client_email,
            "private_key": private_key,
            "private_key_id": "",
            "client_id": "",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
        }
        
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=[
                "https://www.googleapis.com/auth/analytics.readonly", 
                "https://www.googleapis.com/auth/analytics.edit"
            ]
        )
        
        # Initialize client
        analytics_client = BetaAnalyticsDataClient(credentials=credentials)
        admin_client = AnalyticsAdminServiceClient(credentials=credentials)
        default_property_id = property_id
        
        logger.info("Google Analytics client initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize Google Analytics client: {e}")
        raise


def format_parent_id(parent_id: str) -> str:
    """Format parent account ID for API calls."""
    if parent_id.startswith("accounts/"):
        return parent_id
    return f"accounts/{parent_id}"


def format_property_id(property_id: str) -> str:
    """Format property ID for API calls."""
    if property_id.startswith("properties/"):
        return property_id
    return f"properties/{property_id}"


def parse_date_string(date_str: str) -> str:
    """Parse date string and convert to YYYY-MM-DD format."""
    # Handle relative dates
    if date_str.lower() in ["today", "yesterday"]:
        if date_str.lower() == "today":
            return datetime.now().strftime("%Y-%m-%d")
        else:  # yesterday
            return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Handle relative date patterns like "7daysAgo", "30daysAgo"
    if date_str.lower().endswith("daysago"):
        try:
            days = int(date_str.lower().replace("daysago", ""))
            return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        except ValueError:
            pass
    
    # Handle relative date patterns like "1monthAgo", "2monthsAgo"
    if "month" in date_str.lower() and date_str.lower().endswith("ago"):
        try:
            months = int(date_str.lower().split("month")[0])
            # Approximate months as 30 days each
            return (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")
        except ValueError:
            pass
    
    # Return as-is for absolute dates (assume already in correct format)
    return date_str


def preprocess_list_param(param: Union[List[str], str, None]) -> Optional[List[str]]:
    """
    Preprocess a parameter that should be a list but might come as a JSON string.
    
    Args:
        param: The parameter that might be a list, JSON string, or None
        
    Returns:
        A proper list or None
    """
    if param is None:
        return None
    
    if isinstance(param, list):
        return param
    
    if isinstance(param, str):
        try:
            # Try to parse as JSON
            parsed = json.loads(param)
            if isinstance(parsed, list):
                return parsed
            else:
                # If it's a single string value, wrap it in a list
                return [param]
        except json.JSONDecodeError:
            # If JSON parsing fails, treat it as a single string value
            return [param]
    
    return None


def preprocess_dict_param(param: Union[Dict[str, Any], str, None]) -> Optional[Dict[str, Any]]:
    """
    Preprocess a parameter that should be a dict but might come as a JSON string.
    
    Args:
        param: The parameter that might be a dict, JSON string, or None
        
    Returns:
        A proper dict or None
    """
    if param is None:
        return None
    
    if isinstance(param, dict):
        return param
    
    if isinstance(param, str):
        try:
            # Try to parse as JSON
            parsed = json.loads(param)
            if isinstance(parsed, dict):
                return parsed
            else:
                raise ValueError(f"Expected dict but got {type(parsed)}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string for filter parameter: {e}")
    
    raise ValueError(f"Filter parameter must be dict or JSON string, got {type(param)}")