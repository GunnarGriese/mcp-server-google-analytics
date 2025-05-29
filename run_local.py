#!/usr/bin/env python3
"""
Simple runner for local development.
"""

import os
import sys

# Add debug output
print("Starting Google Analytics MCP Server...", file=sys.stderr)
print(f"Python version: {sys.version}", file=sys.stderr)
print(f"Current working directory: {os.getcwd()}", file=sys.stderr)
print(f"Environment variables set: GOOGLE_CLIENT_EMAIL={bool(os.getenv('GOOGLE_CLIENT_EMAIL'))}", file=sys.stderr)

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from mcp_server_google_analytics.server import main
    print("Successfully imported main function", file=sys.stderr)
except ImportError as e:
    print(f"Import error: {e}", file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    main()