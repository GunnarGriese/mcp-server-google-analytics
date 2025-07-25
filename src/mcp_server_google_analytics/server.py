"""
Google Analytics MCP Server

A Model Context Protocol server for Google Analytics Data API.
Provides tools and resources for accessing GA4 data including reports,
realtime data, and metadata.
"""

import logging

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
from . import resources  # noqa: F401
from . import prompts  # noqa: F401
from . import metadata  # noqa: F401
from . import realtime  # noqa: F401

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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