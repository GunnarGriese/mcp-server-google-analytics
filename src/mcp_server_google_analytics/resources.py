"""
Google Analytics MCP Resources.

Contains MCP resource endpoints for accessing GA4 data including metadata
and property listings.
"""

import json
import logging
import asyncio

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
        from google.analytics.data_v1beta.types import GetMetadataRequest
        
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
        
    except Exception as e:
        logger.error(f"Error getting metadata: {e}")
        return json.dumps({"error": f"Error getting metadata: {str(e)}"})


@mcp.resource("ga4://default/metadata")
async def get_default_metadata() -> str:
    """Get metadata for the default Google Analytics property."""
    if not utils.default_property_id:
        return json.dumps({"error": "No default property ID configured"})
    
    return await get_property_metadata(utils.default_property_id)


# NOTE: RESOURCE TEMPLATES ARE CURRENTLY NOT SUPPORTED ON CLAUDE DESKTOP
@mcp.resource("ga4://{property_id}/metadata")
async def get_specific_metadata(property_id: str) -> str:
    """Get metadata for a specific Google Analytics property."""
    if not property_id:
        return json.dumps({"error": "No property ID configured"})
    
    return await get_property_metadata(property_id)


@mcp.resource("ga4://properties/list")
async def get_properties_list() -> str:
    """List all available GA4 properties (legacy format for backward compatibility)."""
    
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        from google.analytics.admin_v1alpha.types import ListAccountSummariesRequest
        
        # Initialize request for account summaries
        request = ListAccountSummariesRequest()
        
        # Make the request to get account summaries (includes properties)
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, utils.admin_client.list_account_summaries, request)
        
        all_properties = []
        
        # Handle the response - each account summary contains property summaries
        for account_summary in page_result:
            account_id = account_summary.account.split('/')[-1]  # Extract account ID
            account_name = account_summary.display_name
            
            # Iterate through property summaries for this account
            for property_summary in account_summary.property_summaries:
                property_id = property_summary.property.split('/')[-1]  # Extract property ID
                
                property_info = {
                    "id": property_id,
                    "name": property_summary.display_name,
                    "resource_name": property_summary.property,
                    "account_name": account_name,
                    "account_id": account_id,
                    "account_resource_name": account_summary.account,
                    "property_type": property_summary.property_type.name if property_summary.property_type else None,
                    "parent": property_summary.parent if hasattr(property_summary, 'parent') else None
                }
                
                all_properties.append(property_info)
        
        return json.dumps({
            "properties": all_properties,
            "total_count": len(all_properties)
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "error": f"Failed to list GA4 properties: {str(e)}"
        })