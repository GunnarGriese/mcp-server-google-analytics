"""
Google Analytics Admin API tools.

Contains tools for managing Google Analytics accounts, properties,
data streams, and other administrative functions.
"""

import json
import logging
import asyncio
from typing import Any, Dict, List, Optional

from .coordinator import mcp
from . import utils

# Configure logging
logger = logging.getLogger(__name__)


@mcp.tool()
async def get_account_summaries() -> str:
    """
    Get all Google Analytics account summaries including properties.
    
    Returns:
        JSON string containing account summaries with properties
    """
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        from google.analytics.admin_v1beta.types import ListAccountSummariesRequest
        
        # Initialize request for account summaries
        request = ListAccountSummariesRequest()
        
        # Make the request to get account summaries
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, utils.admin_client.list_account_summaries, request)
        
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
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        from google.analytics.admin_v1beta.types import GetPropertyRequest
        
        # Create request for property details
        request = GetPropertyRequest(
            name=utils.format_property_id(property_id)
        )
        
        # Make the request
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, utils.admin_client.get_property, request)
        
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
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        from google.analytics.admin_v1beta.types import ListGoogleAdsLinksRequest
        
        # Create request for Google Ads links
        request = ListGoogleAdsLinksRequest(
            parent=utils.format_property_id(property_id)
        )
        
        # Make the request
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, utils.admin_client.list_google_ads_links, request)
        
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
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        from google.analytics.admin_v1beta.types import ListDataStreamsRequest
        
        # Create request for data streams
        request = ListDataStreamsRequest(
            parent=utils.format_property_id(property_id)
        )
        
        # Make the request
        loop = asyncio.get_event_loop()
        page_result = await loop.run_in_executor(None, utils.admin_client.list_data_streams, request)
        
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
        parent: Parent account in format "accounts/{account_id}"
    
    Returns:
        JSON string containing the created property information
    """
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        from google.analytics.admin_v1beta.types import Property, CreatePropertyRequest
        
        # Create property object
        property_obj = Property()
        property_obj.display_name = display_name
        property_obj.time_zone = time_zone
        property_obj.parent = utils.format_parent_id(parent)
        
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
        response = await loop.run_in_executor(None, utils.admin_client.create_property, request)
        
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
    if not utils.admin_client:
        return json.dumps({"error": "Google Analytics admin client not initialized"})
    
    try:
        from google.analytics import admin_v1beta
        from google.analytics.admin_v1beta.types import CreateDataStreamRequest
        
        # Create data stream object
        data_stream = admin_v1beta.DataStream()
        data_stream.display_name = display_name
        data_stream.type_ = "WEB_DATA_STREAM"
        
        # Set web stream data
        data_stream.web_stream_data = admin_v1beta.DataStream.WebStreamData()
        data_stream.web_stream_data.default_uri = default_uri
        
        # Create request
        request = CreateDataStreamRequest(
            parent=utils.format_property_id(parent_property_id),
            data_stream=data_stream
        )
        
        # Make the request
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, utils.admin_client.create_data_stream, request)
        
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
        
    except Exception as e:
        logger.error(f"Error creating data stream: {e}")
        return json.dumps({"error": f"Error creating data stream: {str(e)}"})