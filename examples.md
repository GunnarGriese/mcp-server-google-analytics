# Google Analytics MCP Server Usage Examples

This document provides detailed examples of how to use the Google Analytics MCP Server with Claude or other MCP clients.

## Basic Report Examples

### 1. Get Active Users for the Past 7 Days

Ask Claude:
> "Get the number of active users for the past 7 days, broken down by date"

This will use the `get_report` tool with these parameters:
```json
{
  "start_date": "7daysAgo",
  "end_date": "today",
  "metrics": ["activeUsers"],
  "dimensions": ["date"]
}
```

### 2. Page Views by Country

Ask Claude:
> "Show me page views by country for the past month"

Parameters:
```json
{
  "start_date": "30daysAgo",
  "end_date": "today",
  "metrics": ["screenPageViews"],
  "dimensions": ["country"],
  "limit": 20