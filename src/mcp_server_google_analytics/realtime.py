"""
Google Analytics realtime data tools.

Contains tools for retrieving real-time GA4 data.
"""

import json
import logging
import asyncio
from typing import List, Optional, Union

from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest,
    Dimension,
    Metric,
)
from google.api_core.exceptions import GoogleAPIError

from .coordinator import mcp
from . import utils

# Configure logging
logger = logging.getLogger(__name__)


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
    if not utils.analytics_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        # Preprocess list parameters
        processed_metrics = utils.preprocess_list_param(metrics)
        processed_dimensions = utils.preprocess_list_param(dimensions)
        
        if not processed_metrics:
            return json.dumps({"error": "Metrics parameter is required and must be a valid list"})
        
        # Use default property ID if not provided
        prop_id = property_id or utils.default_property_id
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
        response = await loop.run_in_executor(None, utils.analytics_client.run_realtime_report, request)
        
        # Convert response to dictionary using proto_to_dict
        result = utils.proto_to_dict(response)
        
        return json.dumps(result, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error getting realtime data: {e}")
        return json.dumps({"error": f"Error getting realtime data: {str(e)}"})