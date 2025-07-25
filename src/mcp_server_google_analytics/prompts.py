
"""
Google Analytics MCP Prompt Templates.

Contains MCP prompt templates for analyzing and working with GA4 data.
"""

from .coordinator import mcp


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