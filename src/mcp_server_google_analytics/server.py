"""
Google Analytics MCP Server

A Model Context Protocol server for Google Analytics Data API.
Provides tools and resources for accessing GA4 data including reports,
realtime data, and metadata.
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional, Union

from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest,
    GetMetadataRequest,
    Dimension,
    Metric,
)

from google.api_core.exceptions import GoogleAPIError

# Import coordinator with singleton MCP instance
from .coordinator import mcp

# Import our modular components
from . import utils

# The following imports are necessary to register the tools with the `mcp`
# object, even though they are not directly used in this file.
# The `# noqa: F401` comment tells the linter to ignore the "unused import"
# warning.
from . import reporting  # noqa: F401
from . import admin  # noqa: F401

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import global clients from utils
from .utils import analytics_client, default_property_id, proto_to_dict


##
## REMAINING TOOLS (Not yet modularized)
##

@mcp.tool()
async def get_realtime_data(
    metrics: Union[List[str], str],
    dimensions: Union[List[str], str, None] = None,
    property_id: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """
    Get real-time Google Analytics data.
    
    Args:
        metrics: List of metric names (e.g., ["activeUsers"]) or JSON string
        dimensions: Optional list of dimension names (e.g., ["deviceCategory"]) or JSON string
        property_id: Optional GA4 property ID (uses default if not provided)
        limit: Optional limit on number of rows returned
    
    Returns:
        JSON string containing the real-time data
    """
    if not analytics_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        # Preprocess list parameters
        processed_metrics = utils.preprocess_list_param(metrics)
        processed_dimensions = utils.preprocess_list_param(dimensions)
        
        if not processed_metrics:
            return json.dumps({"error": "Metrics parameter is required and must be a valid list"})
        
        # Use default property ID if not provided
        prop_id = property_id or default_property_id
        if not prop_id:
            return json.dumps({"error": "No property ID provided"})
        
        # Build metrics
        metric_objs = [Metric(name=metric) for metric in processed_metrics]
        
        # Build dimensions
        dimension_objs = []
        if processed_dimensions:
            dimension_objs = [Dimension(name=dim) for dim in processed_dimensions]
        
        # Create request
        request = RunRealtimeReportRequest(
            property=utils.format_property_id(prop_id),
            metrics=metric_objs,
            dimensions=dimension_objs,
            limit=limit
        )
        
        # Run realtime report
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, analytics_client.run_realtime_report, request)
        
        # Convert response to dictionary using proto_to_dict
        result = proto_to_dict(response)
        
        return json.dumps(result, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error getting realtime data: {e}")
        return json.dumps({"error": f"Error getting realtime data: {str(e)}"})


## Access metadata as tool and resource (default property)
async def get_property_metadata(property_id: str) -> str:
    """
    Get metadata for a Google Analytics property including available metrics and dimensions.
    
    Args:
        property_id: GA4 property ID
    
    Returns:
        JSON string containing property metadata
    """
    if not analytics_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        # Create request
        request = GetMetadataRequest(
            name=f"{utils.format_property_id(property_id)}/metadata"
        )
        
        # Get metadata
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, analytics_client.get_metadata, request)
        
        # Convert response to dictionary using proto_to_dict
        result = proto_to_dict(response)
        
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


##
## RESOURCES
##

@mcp.resource("ga4://default/metadata")
async def get_default_metadata() -> str:
    """Get metadata for the default Google Analytics property."""
    if not default_property_id:
        return json.dumps({"error": "No default property ID configured"})
    
    return await get_property_metadata(default_property_id)


# NOTE: RESOURCE TEMPLATES ARE CURRENTLY NOT SUPPORTED ON CLAUDE DESKTOP
@mcp.resource("ga4://{property_id}/metadata")
async def get_specific_metadata(property_id: str) -> str:
    """Get metadata for the default Google Analytics property."""
    if not property_id:
        return json.dumps({"error": "No property ID configured"})
    
    return await get_property_metadata(property_id)


@mcp.resource("ga4://properties/list")
async def get_properties_list() -> str:
    """List all available GA4 properties (legacy format for backward compatibility)."""
    
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        from google.analytics.admin_v1beta.types import ListAccountSummariesRequest
        
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


##
## PROMPTS
##

@mcp.prompt()
async def analyze_ga4_metadata(property_id: str) -> str:
    """
    Generates a comprehensive analysis prompt for Claude to explore the metadata
    of a specific Google Analytics 4 property. This prompt guides Claude to
    utilize the `get_metadata_tool(property_id='{property_id}')` and then synthesize insights about the
    available dimensions and metrics.

    Args:
        property_id: The Google Analytics 4 property ID (e.g., "123456789") for which
                     to analyze metadata.

    Returns:
        A detailed prompt string for Claude, instructing it to fetch, parse, and
        analyze the GA4 property metadata.
    """
    return f"""Analyze the Google Analytics 4 property metadata for property ID '{property_id}'.

1.  First, use the `get_metadata_tool(property_id='{property_id}')` to retrieve the metadata.

2.  For the metadata found, extract and organize the following information for both dimensions and metrics:
    -   API Name
    -   UI Name (if available)
    -   Description
    -   Category (for both dimensions and metrics)
    -   Type (for metrics, e.g., INTEGER, FLOAT)

3.  Provide a comprehensive summary that includes:
    -   An overview of the total number of dimensions and metrics available.
    -   A breakdown of dimensions by category, highlighting the most common categories.
    -   A breakdown of metrics by category, highlighting the most common categories.
    -   Identify any potentially useful or commonly used dimensions (e.g., 'date', 'country', 'deviceCategory').
    -   Identify any potentially useful or commonly used metrics (e.g., 'activeUsers', 'screenPageViews', 'sessions').
    -   Suggest potential combinations of dimensions and metrics that could be used for common GA4 reports.
    -   Discuss any interesting or unusual dimensions/metrics.

4.  Organize your findings in a clear, structured format with headings and bullet points for easy readability.

Please present both detailed information about each dimension and metric and a high-level synthesis of the data capabilities of the '{property_id}' property.
"""


def main():
    """Main entry point for the MCP server."""
    try:
        # Initialize Google Analytics client
        utils.initialize_client()
        
        # Start the MCP server
        logger.info("Starting Google Analytics MCP Server...")
        mcp.run()
        
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise


if __name__ == "__main__":
    main()