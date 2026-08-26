import requests
import json
import base64
import os
from dotenv import load_dotenv
import environmentconfig

# Load environment variables from .env file
load_dotenv()


def extract_with_zyte(extraction_request):
    """
    Simple method to extract data using Zyte API
    
    Args:
        extraction_request: Dictionary containing extraction parameters
                           Example: {"url": "https://example.com", "httpResponseBody": True}
    
    Returns:
        Dictionary containing the API response or error information
    """
    try:
        # Get API key and base URL from environment
        api_key = os.getenv('ZYTE_API_KEY')
        base_url = os.getenv('ZYTE_BASE_URL', 'https://api.zyte.com/v1')
        
        if not api_key:
            return {
                "success": False,
                "error": "API_KEY not found in .env file"
            }
        
        # Prepare authentication
        auth_string = f"{api_key}:"
        encoded_auth = base64.b64encode(auth_string.encode('ascii')).decode('ascii')
        
        headers = {
            'Authorization': f'Basic {encoded_auth}',
            'Content-Type': 'application/json'
        }
        
        # Make the API request
        url = f"{base_url.rstrip('/')}/extract"
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(extraction_request),
            timeout=30
        )
        
        # Parse response
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            response_data = {"raw_content": response.text}
        
        return {
            "success": response.status_code < 400,
            "status_code": response.status_code,
            "data": response_data
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Request failed: {str(e)}"
        }


# Example usage
if __name__ == "__main__":
    # Basic extraction - get HTML content
    basic_request = {
        "url": "https://example.com",
        "httpResponseBody": True,
        "httpResponseHeaders": True
    }
    
    # Advanced extraction - get browser-rendered HTML with screenshot
    advanced_request = {
        "url": "https://example.com",
        "browserHtml": True,
        "screenshot": True,
        "screenshotOptions": {
            "format": "png",
            "width": 1200,
            "height": 800
        },
        "javascript": True
    }
    
    # Product extraction example
    product_request = {
        "url": "https://shop.example.com/product/123",
        "product": True,
        "productOptions": {
            "extractFrom": "browserHtml"
        },
        "browserHtml": True,
        "javascript": True
    }
    
    # Article extraction example
    article_request = {
        "url": "https://news.example.com/article/123",
        "article": True,
        "articleOptions": {
            "extractFrom": "browserHtml"
        },
        "browserHtml": True
    }
    
    # Custom headers and geolocation example
    custom_request = {
        "url": "https://example.com",
        "httpResponseBody": True,
        "requestHeaders": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        },
        "geolocation": "US",
        "device": "desktop"
    }
    
    result = extract_with_zyte(basic_request)
    
    if result["success"]:
        print("Extraction successful!")
        print(f"Status: {result['status_code']}")
        print(f"Data keys: {list(result['data'].keys())}")
    else:
        print(f"Error: {result.get('error', 'Unknown error')}")