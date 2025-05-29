"""
Google Analytics MCP Server

A Model Context Protocol server for Google Analytics Data API.
"""

__version__ = "0.1.0"
__author__ = "Gunnar Griese"
__email__ = "gunnar.griese.gg@gmail.com"

from .server import main

__all__ = ["main"]