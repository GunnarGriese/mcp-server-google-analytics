"""
Google Analytics MCP Server

A Model Context Protocol server for Google Analytics Data API.
Provides tools and resources for accessing GA4 data including reports,
realtime data, and metadata.
"""

import os
import json
import logging
import asyncio
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta

from google.protobuf import message as proto

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
    GetPropertyRequest,
    ListGoogleAdsLinksRequest,
    ListDataStreamsRequest,
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
## UTILITY FUNCTIONS
##

def proto_to_dict(obj: proto.Message) -> Dict[str, Any]:
    """Converts a proto message to a dictionary."""
    return type(obj).to_dict(
        obj, use_integers_for_enums=False, preserving_proto_field_name=True
    )

def proto_to_json(obj: proto.Message) -> str:
    """Converts a proto message to a JSON string."""
    return type(obj).to_json(obj, indent=None, preserving_proto_field_name=True)

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

# Common notes to consider when applying dimension and metric filters.
_FILTER_NOTES = """
  Notes:
    The API applies the `dimension_filter` and `metric_filter`
    independently. As a result, some complex combinations of dimension and
    metric filters are not possible in a single report request.

    For example, you can't create a `dimension_filter` and `metric_filter`
    combination for the following condition:

    (
      (eventName = "page_view" AND eventCount > 100)
      OR
      (eventName = "join_group" AND eventCount < 50)
    )

    This isn't possible because there's no way to apply the condition
    "eventCount > 100" only to the data with eventName of "page_view", and
    the condition "eventCount < 50" only to the data with eventName of
    "join_group".

    More generally, you can't define a `dimension_filter` and `metric_filter`
    for:

    (
      ((dimension condition D1) AND (metric condition M1))
      OR
      ((dimension condition D2) AND (metric condition M2))
    )

    If you have complex conditions like this, either:

    a)  Run a single report that applies a subset of the conditions that
        the API supports as well as the data needed to perform filtering of the
        API response on the client side. For example, for the condition:
        (
          (eventName = "page_view" AND eventCount > 100)
          OR
          (eventName = "join_group" AND eventCount < 50)
        )
        You could run a report that filters only on:
        eventName one of "page_view" or "join_group"
        and include the eventCount metric, then filter the API response on the
        client side to apply the different metric filters for the different
        events.

    or

    b)  Run a separate report for each combination of dimension condition and
        metric condition. For the example above, you'd run one report for the
        combination of (D1 AND M1), and another report for the combination of
        (D2 AND M2).

    Try to run fewer reports (option a) if possible. However, if running
    fewer reports results in excessive quota usage for the API, use option
    b. More information on quota usage is at
    https://developers.google.com/analytics/blog/2023/data-api-quota-management.
  """

@mcp.tool()
async def get_dimensions(property_id: str) -> Dict[str, Any]:
    """
    Get core reporting dimensions for a specific property, including custom dimensions.
    
    Args:
        property_id: GA4 property ID
        
    Returns:
        Dictionary containing available dimensions for the property
    """
    if not analytics_client:
        return {"error": "Google Analytics client not initialized"}
    
    try:
        if property_id.startswith("properties/"):
            property_id = property_id.split("/")[-1]
            
        loop = asyncio.get_event_loop()
        metadata = await loop.run_in_executor(
            None, 
            analytics_client.get_metadata, 
            f"properties/{property_id}/metadata"
        )
        
        # Create metadata object with only dimensions
        from google.analytics.data_v1beta.types import Metadata
        dimensions_only = Metadata(
            name=metadata.name, 
            dimensions=metadata.dimensions
        )
        
        return proto_to_dict(dimensions_only)
        
    except Exception as e:
        logger.error(f"Error getting dimensions: {e}")
        return {"error": f"Error getting dimensions: {str(e)}"}

@mcp.tool()
async def get_metrics(property_id: str) -> Dict[str, Any]:
    """
    Get core reporting metrics for a specific property, including custom metrics.
    
    Args:
        property_id: GA4 property ID
        
    Returns:
        Dictionary containing available metrics for the property
    """
    if not analytics_client:
        return {"error": "Google Analytics client not initialized"}
    
    try:
        if property_id.startswith("properties/"):
            property_id = property_id.split("/")[-1]
            
        loop = asyncio.get_event_loop()
        metadata = await loop.run_in_executor(
            None, 
            analytics_client.get_metadata, 
            f"properties/{property_id}/metadata"
        )
        
        # Create metadata object with only metrics
        from google.analytics.data_v1beta.types import Metadata
        metrics_only = Metadata(
            name=metadata.name, 
            metrics=metadata.metrics
        )
        
        return proto_to_dict(metrics_only)
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return {"error": f"Error getting metrics: {str(e)}"}

@mcp.tool()
def get_standard_dimensions() -> str:
    """Get information about standard dimensions available to all properties."""
    return """Standard dimensions defined in the HTML table at
    https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#dimensions
    These dimensions are available to *every* property"""

@mcp.tool()
def get_standard_metrics() -> str:
    """Get information about standard metrics available to all properties."""
    return """Standard metrics defined in the HTML table at
    https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema#metrics
    These metrics are available to *every* property"""

@mcp.tool()
def run_report_date_ranges_hints() -> str:
    """Provide hints about expected date_ranges parameter format for get_report."""
    from google.analytics.data_v1beta.types import DateRange
    
    range_jan = DateRange(
        start_date="2025-01-01", end_date="2025-01-31", name="Jan2025"
    )
    range_feb = DateRange(
        start_date="2025-02-01", end_date="2025-02-28", name="Feb2025"
    )
    range_last_2_days = DateRange(
        start_date="yesterday", end_date="today", name="YesterdayAndToday"
    )
    range_prev_30_days = DateRange(
        start_date="30daysAgo", end_date="yesterday", name="Previous30Days"
    )

    return f"""Example date_range arguments:
      1. A single date range:
        [ {proto_to_json(range_jan)} ]

      2. A relative date range using 'yesterday' and 'today':
        [ {proto_to_json(range_last_2_days)} ]

      3. A relative date range using 'NdaysAgo' and 'today':
        [ {proto_to_json(range_prev_30_days)}]

      4. Multiple date ranges:
        [ {proto_to_json(range_jan)}, {proto_to_json(range_feb)} ]
    """

@mcp.tool()
def run_report_dimension_filter_hints() -> str:
    """Provide hints about expected dimension_filter parameter format for get_report."""
    from google.analytics.data_v1beta.types import FilterExpression, Filter, FilterExpressionList
    
    begins_with = FilterExpression(
        filter=Filter(
            field_name="eventName",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.BEGINS_WITH,
                value="add",
            ),
        )
    )
    not_filter = FilterExpression(not_expression=begins_with)
    empty_filter = FilterExpression(
        filter=Filter(
            field_name="source", 
            empty_filter=Filter.EmptyFilter()
        )
    )
    source_medium_filter = FilterExpression(
        filter=Filter(
            field_name="sourceMedium",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value="google / cpc",
            ),
        )
    )
    event_list_filter = FilterExpression(
        filter=Filter(
            field_name="eventName",
            in_list_filter=Filter.InListFilter(
                case_sensitive=True,
                values=["first_visit", "purchase", "add_to_cart"],
            ),
        )
    )
    and_filter = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[source_medium_filter, event_list_filter]
        )
    )
    or_filter = FilterExpression(
        or_group=FilterExpressionList(
            expressions=[source_medium_filter, event_list_filter]
        )
    )
    
    return (
        f"""Example dimension_filter arguments:
      1. A simple filter:
        {proto_to_json(begins_with)}

      2. A NOT filter:
        {proto_to_json(not_filter)}

      3. An empty value filter:
        {proto_to_json(empty_filter)}

      4. An AND group filter:
        {proto_to_json(and_filter)}

      5. An OR group filter:
        {proto_to_json(or_filter)}

    """
        + _FILTER_NOTES
    )

@mcp.tool()
def run_report_metric_filter_hints() -> str:
    """Provide hints about expected metric_filter parameter format for get_report."""
    from google.analytics.data_v1beta.types import FilterExpression, Filter, FilterExpressionList, NumericValue
    
    event_count_gt_10_filter = FilterExpression(
        filter=Filter(
            field_name="eventCount",
            numeric_filter=Filter.NumericFilter(
                operation=Filter.NumericFilter.Operation.GREATER_THAN,
                value=NumericValue(int64_value=10),
            ),
        )
    )
    not_filter = FilterExpression(
        not_expression=event_count_gt_10_filter
    )
    empty_filter = FilterExpression(
        filter=Filter(
            field_name="purchaseRevenue",
            empty_filter=Filter.EmptyFilter(),
        )
    )
    revenue_between_filter = FilterExpression(
        filter=Filter(
            field_name="purchaseRevenue",
            between_filter=Filter.BetweenFilter(
                from_value=NumericValue(double_value=10.0),
                to_value=NumericValue(double_value=25.0),
            ),
        )
    )
    and_filter = FilterExpression(
        and_group=FilterExpressionList(
            expressions=[event_count_gt_10_filter, revenue_between_filter]
        )
    )
    or_filter = FilterExpression(
        or_group=FilterExpressionList(
            expressions=[event_count_gt_10_filter, revenue_between_filter]
        )
    )
    
    return (
        f"""Example metric_filter arguments:
      1. A simple filter:
        {proto_to_json(event_count_gt_10_filter)}

      2. A NOT filter:
        {proto_to_json(not_filter)}

      3. An empty value filter:
        {proto_to_json(empty_filter)}

      4. An AND group filter:
        {proto_to_json(and_filter)}

      5. An OR group filter:
        {proto_to_json(or_filter)}

    """
        + _FILTER_NOTES
    )

@mcp.tool()
def run_report_order_bys_hints() -> str:
    """Provide hints about expected order_bys parameter format for run_report."""
    from google.analytics.data_v1beta.types import OrderBy
    
    dimension_alphanumeric_ascending = OrderBy(
        dimension=OrderBy.DimensionOrderBy(
            dimension_name="eventName",
            order_type=OrderBy.DimensionOrderBy.OrderType.ALPHANUMERIC,
        ),
        desc=False,
    )
    dimension_alphanumeric_no_case_descending = OrderBy(
        dimension=OrderBy.DimensionOrderBy(
            dimension_name="campaignName",
            order_type=OrderBy.DimensionOrderBy.OrderType.CASE_INSENSITIVE_ALPHANUMERIC,
        ),
        desc=True,
    )
    dimension_numeric_ascending = OrderBy(
        dimension=OrderBy.DimensionOrderBy(
            dimension_name="audienceId",
            order_type=OrderBy.DimensionOrderBy.OrderType.NUMERIC,
        ),
        desc=False,
    )
    metric_ascending = OrderBy(
        metric=OrderBy.MetricOrderBy(
            metric_name="eventCount",
        ),
        desc=False,
    )
    metric_descending = OrderBy(
        metric=OrderBy.MetricOrderBy(
            metric_name="eventValue",
        ),
        desc=True,
    )

    return f"""Example order_bys arguments:

    1.  Order by ascending 'eventName':
        [ {proto_to_json(dimension_alphanumeric_ascending)} ]

    2.  Order by descending 'eventName', ignoring case:
        [ {proto_to_json(dimension_alphanumeric_no_case_descending)} ]

    3.  Order by ascending 'audienceId':
        [ {proto_to_json(dimension_numeric_ascending)} ]

    4.  Order by descending 'eventCount':
        [ {proto_to_json(metric_descending)} ]

    5.  Order by ascending 'eventCount':
        [ {proto_to_json(metric_ascending)} ]

    6.  Combination of dimension and metric order bys:
        [
          {proto_to_json(dimension_alphanumeric_ascending)},
          {proto_to_json(metric_descending)},
        ]

    7.  Order by multiple dimensions and metrics:
        [
          {proto_to_json(dimension_alphanumeric_ascending)},
          {proto_to_json(dimension_numeric_ascending)},
          {proto_to_json(metric_descending)},
        ]

    The dimensions and metrics in order_bys must also be present in the report
    request's "dimensions" and "metrics" arguments, respectively.
    """

@mcp.tool()
async def run_report(
    property_id: str,
    date_ranges: List[Dict[str, str]],
    dimensions: List[str],
    metrics: List[str],
    dimension_filter: Dict[str, Any] = None,
    metric_filter: Dict[str, Any] = None,
    order_bys: List[Dict[str, Any]] = None,
    limit: int = None,
    offset: int = None,
    currency_code: str = None,
    return_property_quota: bool = False,
) -> Dict[str, Any]:
    """
    Run a Google Analytics Data API report.

    Note that the reference docs at
    https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta
    all use camelCase field names, but field names passed to this method should
    be in snake_case since the tool is using the protocol buffers (protobuf)
    format. The protocol buffers for the Data API are available at
    https://github.com/googleapis/googleapis/tree/master/google/analytics/data/v1beta.

    Args:
        property_id: The Google Analytics property ID.
        date_ranges: A list of date ranges to include in the report.
          For more information about the expected format of this argument, see
          the `run_report_date_ranges_hints` tool.
        dimensions: A list of dimensions to include in the report.
        metrics: A list of metrics to include in the report.
        dimension_filter: A Data API FilterExpression to apply to the dimensions.
          Don't use this for filtering metrics. Use metric_filter instead. 
          The `field_name` in a `dimension_filter` must be a dimension, as defined 
          in the `get_standard_dimensions` and `get_dimensions` tools.
          For more information about the expected format of this argument, see
          the `run_report_dimension_filter_hints` tool.
        metric_filter: A Data API FilterExpression to apply to the metrics.
          Don't use this for filtering dimensions. Use dimension_filter instead. 
          The `field_name` in a `metric_filter` must be a metric, as defined 
          in the `get_standard_metrics` and `get_metrics` tools.
          For more information about the expected format of this argument, see
          the `run_report_metric_filter_hints` tool.
        order_bys: A list of Data API OrderBy objects to apply to the dimensions and metrics.
          For more information about the expected format of this argument, see
          the `run_report_order_bys_hints` tool.
        limit: The maximum number of rows to return in each response. Value must
          be a positive integer <= 250,000. Used to paginate through large
          reports, following the guide at
          https://developers.google.com/analytics/devguides/reporting/data/v1/basics#pagination.
        offset: The row count of the start row. The first row is counted as row
          0. Used to paginate through large reports, following the guide at
          https://developers.google.com/analytics/devguides/reporting/data/v1/basics#pagination.
        currency_code: The currency code to use for currency values. Must be in
          ISO4217 format, such as "AED", "USD", "JPY". If the field is empty, the
          report uses the property's default currency.
        return_property_quota: Whether to return property quota in the response.

    Returns:
        Dictionary containing the report data
    """
    if not analytics_client:
        return {"error": "Google Analytics client not initialized"}
    
    try:
        from google.analytics.data_v1beta.types import (
            RunReportRequest, DateRange, Dimension, Metric, FilterExpression, OrderBy
        )
        
        # Build the request
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=dimension) for dimension in dimensions],
            metrics=[Metric(name=metric) for metric in metrics],
            date_ranges=[DateRange(**dr) for dr in date_ranges],
            return_property_quota=return_property_quota,
        )

        if dimension_filter:
            request.dimension_filter = FilterExpression(dimension_filter)

        if metric_filter:
            request.metric_filter = FilterExpression(metric_filter)

        if order_bys:
            request.order_bys = [OrderBy(order_by) for order_by in order_bys]

        if limit:
            request.limit = limit
        if offset:
            request.offset = offset
        if currency_code:
            request.currency_code = currency_code

        # Run report
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, analytics_client.run_report, request)

        return proto_to_dict(response)
        
    except Exception as e:
        logger.error(f"Error running report: {e}")
        return {"error": f"Error running report: {str(e)}"}

@mcp.tool()
async def get_report(
    start_date: str,
    end_date: str,
    metrics: Union[List[str], str],
    dimensions: Union[List[str], str, None] = None,
    dimension_filter: Union[Dict[str, Any], str, None] = None,
    metric_filter: Union[Dict[str, Any], str, None] = None,
    property_id: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
) -> str:
    """
    Legacy get_report function for backward compatibility.
    
    This function maintains the original interface while using the new run_report internally.
    For new implementations, use run_report instead which follows the official Google Analytics MCP format.
    """
    # Preprocess parameters
    processed_metrics = preprocess_list_param(metrics)
    processed_dimensions = preprocess_list_param(dimensions)
    processed_dimension_filter = preprocess_dict_param(dimension_filter) if dimension_filter else None
    processed_metric_filter = preprocess_dict_param(metric_filter) if metric_filter else None
    
    if not processed_metrics:
        return json.dumps({"error": "Metrics parameter is required and must be a valid list"})
    
    # Use default property ID if not provided
    prop_id = property_id or default_property_id
    if not prop_id:
        return json.dumps({"error": "No property ID provided"})
    
    # Parse dates and create date_ranges
    parsed_start = parse_date_string(start_date)
    parsed_end = parse_date_string(end_date)
    date_ranges = [{"start_date": parsed_start, "end_date": parsed_end}]
    
    # Call the new run_report function
    try:
        result = await run_report(
            property_id=prop_id,
            date_ranges=date_ranges,
            dimensions=processed_dimensions or [],
            metrics=processed_metrics,
            dimension_filter=processed_dimension_filter,
            metric_filter=processed_metric_filter,
            limit=limit,
            offset=offset
        )
        
        # Add parsed date information for backward compatibility
        if isinstance(result, dict) and "error" not in result:
            result["parsed_date_ranges"] = [{"start_date": parsed_start, "end_date": parsed_end}]
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        logger.error(f"Error in legacy get_report: {e}")
        return json.dumps({"error": f"Error getting report: {str(e)}"})

@mcp.tool()
async def get_realtime_data(
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
            name=f"{format_property_id(property_id)}/metadata"
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
    """List all available GA4 properties (legacy format for backward compatibility)."""
    
    if not admin_client:
        return json.dumps({"error": "Google Analytics client not initialized"})
    
    try:
        # Initialize request for account summaries
        request = ListAccountSummariesRequest()
        
        # Make the request to get account summaries (includes properties)
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, admin_client.list_account_summaries, request)
        
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

## 
## ADMIN API TOOLS
##

@mcp.tool()
async def get_account_summaries() -> str:
    """
    Get all Google Analytics account summaries including properties.
    
    Returns:
        JSON string containing account summaries with properties
    """
    if not admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        # Initialize request for account summaries
        request = ListAccountSummariesRequest()
        
        # Make the request to get account summaries
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, admin_client.list_account_summaries, request)
        
        account_summaries = []
        
        # Handle the response - convert each account summary
        for account_summary in page_result:
            account_data = {
                "account": account_summary.account,
                "account_id": account_summary.account.split('/')[-1],
                "display_name": account_summary.display_name,
                "property_summaries": []
            }
            
            # Add property summaries for this account
            for property_summary in account_summary.property_summaries:
                property_data = {
                    "property": property_summary.property,
                    "property_id": property_summary.property.split('/')[-1],
                    "display_name": property_summary.display_name,
                    "property_type": property_summary.property_type.name if property_summary.property_type else None,
                    "parent": property_summary.parent if hasattr(property_summary, 'parent') else None
                }
                account_data["property_summaries"].append(property_data)
            
            account_summaries.append(account_data)
        
        return json.dumps({
            "account_summaries": account_summaries,
            "total_accounts": len(account_summaries)
        }, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error getting account summaries: {e}")
        return json.dumps({"error": f"Error getting account summaries: {str(e)}"})

@mcp.tool()
async def get_property_details(property_id: str) -> str:
    """
    Get detailed information about a specific Google Analytics property.
    
    Args:
        property_id: GA4 property ID
        
    Returns:
        JSON string containing detailed property information
    """
    if not admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        # Create request for property details
        request = GetPropertyRequest(
            name=format_property_id(property_id)
        )
        
        # Make the request
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, admin_client.get_property, request)
        
        # Format response manually
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
        logger.error(f"Error getting property details: {e}")
        return json.dumps({"error": f"Error getting property details: {str(e)}"})

@mcp.tool()
async def list_google_ads_links(property_id: str) -> str:
    """
    List Google Ads links for a specific Google Analytics property.
    
    Args:
        property_id: GA4 property ID
        
    Returns:
        JSON string containing Google Ads links information
    """
    if not admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        # Create request for Google Ads links
        request = ListGoogleAdsLinksRequest(
            parent=format_property_id(property_id)
        )
        
        # Make the request
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, admin_client.list_google_ads_links, request)
        
        google_ads_links = []
        
        # Handle the paginated response
        for google_ads_link in page_result:
            link_data = {
                "name": google_ads_link.name,
                "customer_id": google_ads_link.customer_id,
                "can_manage_clients": google_ads_link.can_manage_clients,
                "ads_personalization_enabled": google_ads_link.ads_personalization_enabled,
                "create_time": google_ads_link.create_time.isoformat() if google_ads_link.create_time else None,
                "update_time": google_ads_link.update_time.isoformat() if google_ads_link.update_time else None,
                "creator_email_address": google_ads_link.creator_email_address
            }
            google_ads_links.append(link_data)
        
        return json.dumps({
            "property_id": property_id,
            "google_ads_links": google_ads_links,
            "total_links": len(google_ads_links)
        }, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error listing Google Ads links: {e}")
        return json.dumps({"error": f"Error listing Google Ads links: {str(e)}"})

@mcp.tool()
async def list_data_streams(property_id: str) -> str:
    """
    List all data streams for a specific Google Analytics property.
    
    Args:
        property_id: GA4 property ID
        
    Returns:
        JSON string containing data streams information
    """
    if not admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        # Create request for data streams
        request = ListDataStreamsRequest(
            parent=format_property_id(property_id)
        )
        
        # Make the request
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, admin_client.list_data_streams, request)
        
        data_streams = []
        
        # Handle the paginated response
        for data_stream in page_result:
            stream_data = {
                "name": data_stream.name,
                "data_stream_id": data_stream.name.split('/')[-1],
                "display_name": data_stream.display_name,
                "type": data_stream.type_.name if data_stream.type_ else None,
                "create_time": data_stream.create_time.isoformat() if data_stream.create_time else None,
                "update_time": data_stream.update_time.isoformat() if data_stream.update_time else None
            }
            
            # Add web stream data if present
            if hasattr(data_stream, 'web_stream_data') and data_stream.web_stream_data:
                stream_data["web_stream_data"] = {
                    "measurement_id": data_stream.web_stream_data.measurement_id,
                    "firebase_app_id": data_stream.web_stream_data.firebase_app_id,
                    "default_uri": data_stream.web_stream_data.default_uri
                }
            
            # Add android app stream data if present
            if hasattr(data_stream, 'android_app_stream_data') and data_stream.android_app_stream_data:
                stream_data["android_app_stream_data"] = {
                    "firebase_app_id": data_stream.android_app_stream_data.firebase_app_id,
                    "package_name": data_stream.android_app_stream_data.package_name
                }
            
            # Add iOS app stream data if present
            if hasattr(data_stream, 'ios_app_stream_data') and data_stream.ios_app_stream_data:
                stream_data["ios_app_stream_data"] = {
                    "firebase_app_id": data_stream.ios_app_stream_data.firebase_app_id,
                    "bundle_id": data_stream.ios_app_stream_data.bundle_id
                }
            
            data_streams.append(stream_data)
        
        return json.dumps({
            "property_id": property_id,
            "data_streams": data_streams,
            "total_streams": len(data_streams)
        }, indent=2)
        
    except GoogleAPIError as e:
        logger.error(f"Google API error: {e}")
        return json.dumps({"error": f"Google API error: {str(e)}"})
    except Exception as e:
        logger.error(f"Error listing data streams: {e}")
        return json.dumps({"error": f"Error listing data streams: {str(e)}"})

@mcp.tool()
async def create_property(
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
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, admin_client.create_property, request)
        
        # Format response manually (proto_to_dict doesn't work with admin types)
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
async def create_data_stream(
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
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, admin_client.create_data_stream, request)
        
        # Format response manually (proto_to_dict doesn't work with admin types)
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