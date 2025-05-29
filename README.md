# Google Analytics MCP Server (Python)

A Model Context Protocol (MCP) server that provides access to Google Analytics Data API. This Python implementation allows LLMs to retrieve reports, real-time data, and metadata from Google Analytics 4 properties.

## Features

- **get_report**: Retrieve reports based on specified date ranges, metrics, and dimensions
- **get_realtime_data**: Fetch real-time analytics data
- **Metadata Resources**: Access metadata for Google Analytics properties including available metrics and dimensions

## Prerequisites

1. **Google Cloud Project**: Create a Google Cloud project and enable the Analytics Data API
2. **Service Account**: Create a service account and download the credentials JSON file
3. **GA4 Access**: Grant the service account appropriate access to your GA4 property

For detailed setup instructions, see the [Google Analytics Data API documentation](https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries).

## Installation

### Option 1: Using pip (recommended)

```bash
pip install mcp-server-google-analytics
```

### Option 2: From source

```bash
# Clone the repository
git clone https://github.com/GunnarGriese/mcp-server-google-analytics.git
cd mcp-server-google-analytics

# Install dependencies
pip install -e .
```

## Configuration

Set the following environment variables:

```bash
export GOOGLE_CLIENT_EMAIL="your-service-account@project.iam.gserviceaccount.com"
export GOOGLE_PRIVATE_KEY="your-private-key"
export GA_PROPERTY_ID="your-ga4-property-id"
```

### Finding Your Configuration Values

1. **GOOGLE_CLIENT_EMAIL**: Found in your service account JSON file as `client_email`
2. **GOOGLE_PRIVATE_KEY**: Found in your service account JSON file as `private_key`
3. **GA_PROPERTY_ID**: Your GA4 property ID (numeric, e.g., "123456789")

## Usage

### Running the Server

```bash
# Using the installed script
mcp-server-google-analytics

# Or directly with Python
python -m mcp_server_google_analytics.server
```

### Claude Desktop Integration

Add the following to your Claude Desktop configuration file:

```json
{
  "mcpServers": {
    "google-analytics": {
      "command": "python",
      "args": ["-m", "mcp_server_google_analytics.server"],
      "env": {
        "GOOGLE_CLIENT_EMAIL": "your-service-account@project.iam.gserviceaccount.com",
        "GOOGLE_PRIVATE_KEY": "your-private-key",
        "GA_PROPERTY_ID": "your-ga4-property-id"
      }
    }
  }
}
```

### Alternative: Using the pip-installed script

```json
{
  "mcpServers": {
    "google-analytics": {
      "command": "mcp-server-google-analytics",
      "env": {
        "GOOGLE_CLIENT_EMAIL": "your-service-account@project.iam.gserviceaccount.com",
        "GOOGLE_PRIVATE_KEY": "your-private-key",
        "GA_PROPERTY_ID": "your-ga4-property-id"
      }
    }
  }
}
```

## Examples

### Get Report

Use the `get_report` tool to retrieve analytics data:

```python
# Example arguments:
{
  "start_date": "7daysAgo",
  "end_date": "today",
  "metrics": ["activeUsers", "screenPageViews"],
  "dimensions": ["date"],
  "limit": 10
}
```

### Get Real-time Data

Use the `get_realtime_data` tool to get current active users:

```python
# Example arguments:
{
  "metrics": ["activeUsers"],
  "dimensions": ["deviceCategory"],
  "limit": 10
}
```

### Access Metadata

Access the `ga4://property/123456789/metadata` or `ga4://default/metadata` resource to see available metrics and dimensions.

## Supported Date Formats

- Absolute dates: `"2024-01-01"`
- Relative dates: `"today"`, `"yesterday"`
- Days ago: `"7daysAgo"`, `"30daysAgo"`
- Months ago: `"1monthAgo"`, `"2monthsAgo"`

## Common Metrics and Dimensions

### Popular Metrics
- `activeUsers` - Number of distinct users
- `screenPageViews` - Number of page/screen views
- `sessions` - Number of sessions
- `bounceRate` - Bounce rate percentage
- `averageSessionDuration` - Average session duration

### Popular Dimensions
- `date` - Date of the session
- `country` - Country of origin
- `city` - City of origin
- `deviceCategory` - Device category (desktop, mobile, tablet)
- `browser` - Browser used
- `operatingSystem` - Operating system

## Error Handling

The server includes comprehensive error handling for:
- Missing environment variables
- Invalid Google API credentials
- API rate limits and quotas
- Invalid property IDs
- Malformed requests

## Development

### Setting up for development

```bash
# Clone the repository
git clone https://github.com/your-username/mcp-server-google-analytics-python.git
cd mcp-server-google-analytics-python

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"
```

## Troubleshooting

### Authentication Issues

1. Verify your service account has the correct permissions
2. Check that the Analytics Data API is enabled in your Google Cloud project
3. Ensure your private key is properly formatted (newlines should be `\n`)

### Property Access Issues

1. Confirm your service account has been added to your GA4 property
2. Verify the property ID is correct (numeric format)
3. Check that the property has data for the requested date range

### Common Error Messages

- `"Google Analytics client not initialized"`: Check environment variables
- `"No property ID provided"`: Set GA_PROPERTY_ID environment variable
- `"Google API error"`: Check API credentials and permissions

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Please feel free to submit a Pull Request.

## Support

If you encounter any issues or have questions, please file an issue on the GitHub repository.