"""
Google Analytics MCP Server

A Model Context Protocol server for Google Analytics Data API.
Provides tools and resources for accessing GA4 data including reports,
realtime data, and metadata.
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    RunRealtimeReportRequest,
    GetMetadataRequest,
    DateRange,
    Dimension,
    Metric,
    Filter,
    FilterExpression,
    NumericValue,
    FilterExpressionList,
)

from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
from google.analytics.admin_v1beta.types import (
    ListAccountSummariesRequest,
    Property, 
    CreatePropertyRequest,
    DataStream,
    CreateDataStreamRequest,
)

from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError

#from mcp.server.fastmcp import FastMCP
from fastmcp import FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP("Google Analytics Server")

# Global client instance
analytics_client: Optional[BetaAnalyticsDataClient] = None
admin_client: Optional[AnalyticsAdminServiceClient] = None
default_property_id: Optional[str] = None

##
## AUTHORIZATION
##

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

## 
## TOOLS AND RESOURCES
##

def format_parent_id(parent_id: str) -> str:
    """Format property ID for API calls."""
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

def normalize_filter_keys(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize filter keys to match Google Analytics API expectations.
    Converts camelCase to snake_case for common field names.
    """
    if not isinstance(data, dict):
        return data
    
    # Key mappings from camelCase to snake_case
    key_mappings = {
        'fieldName': 'field_name',
        'matchType': 'match_type',
        'caseSensitive': 'case_sensitive',
        'int64Value': 'int64_value',
        'doubleValue': 'double_value',
        'fromValue': 'from_value',
        'toValue': 'to_value',
        'stringFilter': 'string_filter',
        'numericFilter': 'numeric_filter',
        'betweenFilter': 'between_filter',
        'inListFilter': 'in_list_filter',
        'andGroup': 'and_group',
        'orGroup': 'or_group',
        'notExpression': 'not_expression'
    }
    
    normalized = {}
    for key, value in data.items():
        # Map the key if it exists in our mappings
        new_key = key_mappings.get(key, key)
        
        # Recursively normalize nested dictionaries
        if isinstance(value, dict):
            normalized[new_key] = normalize_filter_keys(value)
        elif isinstance(value, list):
            # Handle lists that might contain dictionaries
            normalized[new_key] = [
                normalize_filter_keys(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            normalized[new_key] = value
    
    return normalized

def build_filter_expression(filter_config: Dict[str, Any]) -> FilterExpression:
    """
    Build a FilterExpression from a configuration dictionary.
    
    Args:
        filter_config: Dictionary containing filter configuration
        
    Returns:
        FilterExpression object
        
    Example filter_config:
    {
        "filter": {
            "field_name": "country",
            "string_filter": {
                "match_type": "EXACT",
                "value": "United States"
            }
        }
    }
    
    Or for complex filters:
    {
        "and_group": {
            "expressions": [
                {
                    "filter": {
                        "field_name": "country",
                        "string_filter": {"match_type": "EXACT", "value": "United States"}
                    }
                },
                {
                    "filter": {
                        "field_name": "activeUsers",
                        "numeric_filter": {"operation": "GREATER_THAN", "value": {"int64_value": "100"}}
                    }
                }
            ]
        }
    }
    """
    if "filter" in filter_config:
        # Single filter
        filter_data = filter_config["filter"]
        filter_obj = Filter(field_name=filter_data["field_name"])
        
        if "string_filter" in filter_data:
            string_filter_data = filter_data["string_filter"]
            filter_obj.string_filter = Filter.StringFilter(
                match_type=getattr(Filter.StringFilter.MatchType, string_filter_data.get("match_type", "EXACT")),
                value=string_filter_data["value"],
                case_sensitive=string_filter_data.get("case_sensitive", False)
            )
        elif "numeric_filter" in filter_data:
            numeric_filter_data = filter_data["numeric_filter"]
            numeric_value = NumericValue()
            
            if "int64_value" in numeric_filter_data["value"]:
                numeric_value.int64_value = int(numeric_filter_data["value"]["int64_value"])
            elif "double_value" in numeric_filter_data["value"]:
                numeric_value.double_value = float(numeric_filter_data["value"]["double_value"])
                
            filter_obj.numeric_filter = Filter.NumericFilter(
                operation=getattr(Filter.NumericFilter.Operation, numeric_filter_data["operation"]),
                value=numeric_value
            )
        elif "between_filter" in filter_data:
            between_filter_data = filter_data["between_filter"]
            
            from_value = NumericValue()
            to_value = NumericValue()
            
            if "int64_value" in between_filter_data["from_value"]:
                from_value.int64_value = int(between_filter_data["from_value"]["int64_value"])
            elif "double_value" in between_filter_data["from_value"]:
                from_value.double_value = float(between_filter_data["from_value"]["double_value"])
                
            if "int64_value" in between_filter_data["to_value"]:
                to_value.int64_value = int(between_filter_data["to_value"]["int64_value"])
            elif "double_value" in between_filter_data["to_value"]:
                to_value.double_value = float(between_filter_data["to_value"]["double_value"])
            
            filter_obj.between_filter = Filter.BetweenFilter(
                from_value=from_value,
                to_value=to_value
            )
        elif "in_list_filter" in filter_data:
            in_list_filter_data = filter_data["in_list_filter"]
            filter_obj.in_list_filter = Filter.InListFilter(
                values=in_list_filter_data["values"],
                case_sensitive=in_list_filter_data.get("case_sensitive", False)
            )
        
        return FilterExpression(filter=filter_obj)
    
    elif "and_group" in filter_config:
        # AND group of filters
        expressions = []
        for expr_config in filter_config["and_group"]["expressions"]:
            expressions.append(build_filter_expression(expr_config))
        
        return FilterExpression(
            and_group=FilterExpressionList(expressions=expressions)
        )
    
    elif "or_group" in filter_config:
        # OR group of filters
        expressions = []
        for expr_config in filter_config["or_group"]["expressions"]:
            expressions.append(build_filter_expression(expr_config))
        
        return FilterExpression(
            or_group=FilterExpressionList(expressions=expressions)
        )
    
    elif "not_expression" in filter_config:
        # NOT expression
        return FilterExpression(
            not_expression=build_filter_expression(filter_config["not_expression"])
        )
    
    else:
        raise ValueError(f"Invalid filter configuration: {filter_config}")

## 
## DATA API TOOLS
##

@mcp.tool()
def get_report(
    start_date: str,
    end_date: str,
    metrics: Union[List[str], str],
    dimensions: Union[List[str], str, None] = None,
    dimension_filter: Union[Dict[str, Any], str, None] = None,  # Allow both types
    metric_filter: Union[Dict[str, Any], str, None] = None,     # Allow both types
    property_id: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> str:
    """
    Get a Google Analytics report for the specified date range, metrics, and dimensions.
    
    Args:
        start_date: Start date in YYYY-MM-DD format or relative format (e.g., "7daysAgo", "today")
        end_date: End date in YYYY-MM-DD format or relative format (e.g., "today", "yesterday")
        metrics: List of metric names (e.g., ["activeUsers", "screenPageViews"]) or JSON string
        dimensions: Optional list of dimension names (e.g., ["date", "country"]) or JSON string
        dimension_filter: Optional dimension filter configuration or JSON string
        metric_filter: Optional metric filter configuration or JSON string
        property_id: Optional GA4 property ID (uses default if not provided)
        limit: Optional limit on number of rows returned
        offset: Optional offset for pagination

    Returns:
        JSON string containing the report data
    """
    if not analytics_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        # Preprocess all parameters
        processed_metrics = preprocess_list_param(metrics)
        processed_dimensions = preprocess_list_param(dimensions)
        processed_dimension_filter = preprocess_dict_param(dimension_filter)
        processed_metric_filter = preprocess_dict_param(metric_filter)
        
        if not processed_metrics:
            return json.dumps({"error": "Metrics parameter is required and must be a valid list"})
        
        # Use default property ID if not provided
        prop_id = property_id or default_property_id
        if not prop_id:
            return json.dumps({"error": "No property ID provided"})
        
        # Parse dates
        parsed_start = parse_date_string(start_date)
        parsed_end = parse_date_string(end_date)
        
        # Build date ranges
        date_ranges = [DateRange(start_date=parsed_start, end_date=parsed_end)]
        
        # Build metrics
        metric_objs = [Metric(name=metric) for metric in processed_metrics]
        
        # Build dimensions
        dimension_objs = []
        if processed_dimensions:
            dimension_objs = [Dimension(name=dim) for dim in processed_dimensions]

        # Build filters
        dimension_filter_obj = None
        if processed_dimension_filter:
            dimension_filter_obj = build_filter_expression(processed_dimension_filter)
            
        metric_filter_obj = None
        if processed_metric_filter:
            metric_filter_obj = build_filter_expression(processed_metric_filter)
        
        # Create request
        request = RunReportRequest(
            property=format_property_id(prop_id),
            date_ranges=date_ranges,
            metrics=metric_objs,
            dimensions=dimension_objs,
            dimension_filter=dimension_filter_obj,
            metric_filter=metric_filter_obj,
            limit=limit,
            offset=offset
        )
        
        # Run report
        response = analytics_client.run_report(request=request)
        
        # Format response
        result = {
            "date_ranges": [{"start_date": parsed_start, "end_date": parsed_end}],
            "metrics_headers": [{"name": metric.name} for metric in response.metric_headers],
            "dimension_headers": [{"name": dim.name} for dim in response.dimension_headers],
            "rows": [],
            "row_count": response.row_count,
            "metadata": {
                "currency_code": response.metadata.currency_code if response.metadata else None,
                "time_zone": response.metadata.time_zone if response.metadata else None
            }
        }
        
        # Process rows
        for row in response.rows:
            row_data = {
                "dimensions": [dim_value.value for dim_value in row.dimension_values],
                "metrics": [metric_value.value for metric_value in row.metric_values]
            }
            result["rows"].append(row_data)
        
        return json.dumps(result, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error getting report: {e}")
        return json.dumps({"error": f"Error getting report: {str(e)}"})


@mcp.tool()
def get_realtime_data(
    metrics: Union[List[str], str],  # Allow both types
    dimensions: Union[List[str], str, None] = None,  # Allow both types
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
        processed_metrics = preprocess_list_param(metrics)
        processed_dimensions = preprocess_list_param(dimensions)
        
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
            property=format_property_id(prop_id),
            metrics=metric_objs,
            dimensions=dimension_objs,
            limit=limit
        )
        
        # Run realtime report
        response = analytics_client.run_realtime_report(request=request)
        
        # Format response
        result = {
            "metrics_headers": [{"name": metric.name} for metric in response.metric_headers],
            "dimension_headers": [{"name": dim.name} for dim in response.dimension_headers],
            "rows": [],
            "row_count": response.row_count
        }
        
        # Process rows
        for row in response.rows:
            row_data = {
                "dimensions": [dim_value.value for dim_value in row.dimension_values],
                "metrics": [metric_value.value for metric_value in row.metric_values]
            }
            result["rows"].append(row_data)
        
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
            name=f"{format_property_id(property_id)}/metadata"
        )
        
        # Get metadata
        response = analytics_client.get_metadata(request=request)
        
        # Format response
        result = {
            "property_id": property_id,
            "metrics": [],
            "dimensions": []
        }
        
        # Process metrics
        for metric in response.metrics:
            metric_data = {
                "api_name": metric.api_name,
                "ui_name": metric.ui_name,
                "description": metric.description,
                "type": metric.type_.name if metric.type_ else None,
                "category": metric.category
            }
            result["metrics"].append(metric_data)
        
        # Process dimensions
        for dimension in response.dimensions:
            dimension_data = {
                "api_name": dimension.api_name,
                "ui_name": dimension.ui_name,
                "description": dimension.description,
                "category": dimension.category
            }
            result["dimensions"].append(dimension_data)
        
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
## DATA API RESOURCES
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
    """List all available GA4 properties."""
    
    if not admin_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        # Initialize request for account summaries
        request = ListAccountSummariesRequest()
        
        # Make the request to get account summaries (includes properties)
        page_result = admin_client.list_account_summaries(request=request)
        
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
## ADMIN API TOOLS
##
@mcp.tool()
def create_property(
    display_name: str,
    time_zone: str,
    parent: str,
    currency_code: Optional[str] = None,
    industry_category: Optional[str] = None,
) -> str:
    """
    Create a new Google Analytics 4 property.
    
    Args:
        display_name: Human-readable display name for the property
        time_zone: Time zone for the property (e.g., "America/Los_Angeles", "UTC")
        currency_code: Optional currency code for the property (e.g., "USD", "EUR")
        industry_category: Optional industry category (e.g., "AUTOMOTIVE", "BUSINESS_AND_INDUSTRIAL_MARKETS")
        parent: Optional parent account in format "accounts/{account_id}" (uses default account if not provided)
    
    Returns:
        JSON string containing the created property information
    """
    if not admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        
        # Create property object
        property_obj = Property()
        property_obj.display_name = display_name
        property_obj.time_zone = time_zone
        property_obj.parent = format_parent_id(parent)
        
        
        if currency_code:
            property_obj.currency_code = currency_code
        
        if industry_category:
            # Convert string to enum if needed
            property_obj.industry_category = getattr(Property.IndustryCategory, industry_category, industry_category)
        
        # Create request
        request = CreatePropertyRequest(
            property=property_obj
        )
        
        # Make the request
        response = admin_client.create_property(request=request)
        
        # Format response
        result = {
            "property_id": response.name.split('/')[-1],
            "resource_name": response.name,
            "display_name": response.display_name,
            "time_zone": response.time_zone,
            "currency_code": response.currency_code,
            "industry_category": response.industry_category.name if response.industry_category else None,
            "create_time": response.create_time.isoformat() if response.create_time else None,
            "update_time": response.update_time.isoformat() if response.update_time else None,
            "parent": response.parent,
            "delete_time": response.delete_time.isoformat() if response.delete_time else None,
            "expire_time": response.expire_time.isoformat() if response.expire_time else None
        }
        
        return json.dumps(result, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error creating property: {e}")
        return json.dumps({"error": f"Error creating property: {str(e)}"})
    
@mcp.tool()
def create_data_stream(
    parent_property_id: str,
    display_name: str,
    default_uri: str
) -> str:
    """
    Create a new web data stream for a Google Analytics 4 property.
    
    Args:
        parent_property_id: GA4 property ID where the data stream will be created
        display_name: Human-readable display name for the data stream
        default_uri: Default URI for the web stream (e.g., "https://example.com")
    
    Returns:
        JSON string containing the created data stream information
    """
    if not admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        from google.analytics import admin_v1beta
        
        # Create data stream object
        data_stream = admin_v1beta.DataStream()
        data_stream.display_name = display_name
        data_stream.type_ = "WEB_DATA_STREAM"
        
        # Set web stream data
        data_stream.web_stream_data = admin_v1beta.DataStream.WebStreamData()
        data_stream.web_stream_data.default_uri = default_uri
        
        # Create request
        request = admin_v1beta.CreateDataStreamRequest(
            parent=format_property_id(parent_property_id),
            data_stream=data_stream
        )
        
        # Make the request
        response = admin_client.create_data_stream(request=request)
        
        # Format response
        result = {
            "data_stream_id": response.name.split('/')[-1],
            "resource_name": response.name,
            "display_name": response.display_name,
            "type": response.type_,
            "create_time": response.create_time.isoformat() if response.create_time else None,
            "update_time": response.update_time.isoformat() if response.update_time else None
        }
        
        # Add web stream data
        if response.web_stream_data:
            result["web_stream_data"] = {
                "measurement_id": response.web_stream_data.measurement_id,
                "firebase_app_id": response.web_stream_data.firebase_app_id,
                "default_uri": response.web_stream_data.default_uri
            }
        
        return json.dumps(result, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error creating data stream: {e}")
        return json.dumps({"error": f"Error creating data stream: {str(e)}"})

def main():
    """Main entry point for the MCP server."""
    try:
        # Initialize Google Analytics client
        initialize_client()
        
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