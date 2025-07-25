"""
Google Analytics metadata tools.

Contains tools for retrieving GA4 property metadata and related information.
"""

import json
import logging
import asyncio
from typing import Optional

from google.analytics.data_v1beta.types import GetMetadataRequest
from google.api_core.exceptions import GoogleAPIError

from .coordinator import mcp
from . import utils

# Configure logging
logger = logging.getLogger(__name__)


async def get_property_metadata(property_id: str) -> str:
    """
    Get metadata for a Google Analytics property including available metrics and dimensions.
    
    Args:
        property_id: GA4 property ID
    
    Returns:
        JSON string containing property metadata
    """
    if not utils.analytics_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        # Create request
        request = GetMetadataRequest(
            name=f"{utils.format_property_id(property_id)}/metadata"
        )
        
        # Get metadata
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, utils.analytics_client.get_metadata, request)
        
        # Convert response to dictionary using proto_to_dict
        result = utils.proto_to_dict(response)
        
        # Add property_id for clarity
        result["property_id"] = property_id
        
        return json.dumps(result, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error getting metadata: {e}")
        return json.dumps({"error": f"Error getting metadata: {str(e)}"})


@mcp.tool()
async def get_metadata_tool(property_id: str) -> str:
    """
    Get metadata for a Google Analytics property including available metrics and dimensions.
    
    Args:
        property_id: GA4 property ID
    
    Returns:
        JSON string containing property metadata
    """
    return await get_property_metadata(property_id)