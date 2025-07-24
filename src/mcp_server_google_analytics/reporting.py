"""
Google Analytics Data API reporting tools.

Contains tools for running reports, retrieving dimensions/metrics,
and providing parameter hints for the Data API.
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional, Union

from .coordinator import mcp
from . import utils

# Configure logging
logger = logging.getLogger(__name__)

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
    if not utils.analytics_client:
        return {"error": "Google Analytics client not initialized"}
    
    try:
        if property_id.startswith("properties/"):
            property_id = property_id.split("/")[-1]
            
        loop = asyncio.get_event_loop()
        metadata = await loop.run_in_executor(
            None, 
            utils.analytics_client.get_metadata, 
            f"properties/{property_id}/metadata"
        )
        
        # Create metadata object with only dimensions
        from google.analytics.data_v1beta.types import Metadata
        dimensions_only = Metadata(
            name=metadata.name, 
            dimensions=metadata.dimensions
        )
        
        return utils.proto_to_dict(dimensions_only)
        
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
    if not utils.analytics_client:
        return {"error": "Google Analytics client not initialized"}
    
    try:
        if property_id.startswith("properties/"):
            property_id = property_id.split("/")[-1]
            
        loop = asyncio.get_event_loop()
        metadata = await loop.run_in_executor(
            None, 
            utils.analytics_client.get_metadata, 
            f"properties/{property_id}/metadata"
        )
        
        # Create metadata object with only metrics
        from google.analytics.data_v1beta.types import Metadata
        metrics_only = Metadata(
            name=metadata.name, 
            metrics=metadata.metrics
        )
        
        return utils.proto_to_dict(metrics_only)
        
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
    """Provide hints about expected date_ranges parameter format for run_report."""
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
        [ {utils.proto_to_json(range_jan)} ]

      2. A relative date range using 'yesterday' and 'today':
        [ {utils.proto_to_json(range_last_2_days)} ]

      3. A relative date range using 'NdaysAgo' and 'today':
        [ {utils.proto_to_json(range_prev_30_days)}]

      4. Multiple date ranges:
        [ {utils.proto_to_json(range_jan)}, {utils.proto_to_json(range_feb)} ]
    """


@mcp.tool()
def run_report_dimension_filter_hints() -> str:
    """Provide hints about expected dimension_filter parameter format for run_report."""
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
        {utils.proto_to_json(begins_with)}

      2. A NOT filter:
        {utils.proto_to_json(not_filter)}

      3. An empty value filter:
        {utils.proto_to_json(empty_filter)}

      4. An AND group filter:
        {utils.proto_to_json(and_filter)}

      5. An OR group filter:
        {utils.proto_to_json(or_filter)}

    """
        + _FILTER_NOTES
    )


@mcp.tool()
def run_report_metric_filter_hints() -> str:
    """Provide hints about expected metric_filter parameter format for run_report."""
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
        {utils.proto_to_json(event_count_gt_10_filter)}

      2. A NOT filter:
        {utils.proto_to_json(not_filter)}

      3. An empty value filter:
        {utils.proto_to_json(empty_filter)}

      4. An AND group filter:
        {utils.proto_to_json(and_filter)}

      5. An OR group filter:
        {utils.proto_to_json(or_filter)}

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
        [ {utils.proto_to_json(dimension_alphanumeric_ascending)} ]

    2.  Order by descending 'eventName', ignoring case:
        [ {utils.proto_to_json(dimension_alphanumeric_no_case_descending)} ]

    3.  Order by ascending 'audienceId':
        [ {utils.proto_to_json(dimension_numeric_ascending)} ]

    4.  Order by descending 'eventCount':
        [ {utils.proto_to_json(metric_descending)} ]

    5.  Order by ascending 'eventCount':
        [ {utils.proto_to_json(metric_ascending)} ]

    6.  Combination of dimension and metric order bys:
        [
          {utils.proto_to_json(dimension_alphanumeric_ascending)},
          {utils.proto_to_json(metric_descending)},
        ]

    7.  Order by multiple dimensions and metrics:
        [
          {utils.proto_to_json(dimension_alphanumeric_ascending)},
          {utils.proto_to_json(dimension_numeric_ascending)},
          {utils.proto_to_json(metric_descending)},
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
    if not utils.analytics_client:
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
        response = await loop.run_in_executor(None, utils.analytics_client.run_report, request)

        return utils.proto_to_dict(response)
        
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
    processed_metrics = utils.preprocess_list_param(metrics)
    processed_dimensions = utils.preprocess_list_param(dimensions)
    processed_dimension_filter = utils.preprocess_dict_param(dimension_filter) if dimension_filter else None
    processed_metric_filter = utils.preprocess_dict_param(metric_filter) if metric_filter else None
    
    if not processed_metrics:
        return json.dumps({"error": "Metrics parameter is required and must be a valid list"})
    
    # Use default property ID if not provided
    prop_id = property_id or utils.default_property_id
    if not prop_id:
        return json.dumps({"error": "No property ID provided"})
    
    # Parse dates and create date_ranges
    parsed_start = utils.parse_date_string(start_date)
    parsed_end = utils.parse_date_string(end_date)
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