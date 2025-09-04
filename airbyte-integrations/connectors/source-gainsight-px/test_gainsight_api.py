#!/usr/bin/env python3
"""
Gainsight PX API Test Script
This script ingests account records from the Gainsight PX API with proper rate limiting.
"""

import json
import time
import requests
from typing import Dict, List, Optional, Generator
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GainsightPXClient:
    """Client for interacting with the Gainsight PX API"""
    
    def __init__(self, api_key: str, rate_limit_delay: float = 5.0):
        """
        Initialize the Gainsight PX client
        
        Args:
            api_key: Your Gainsight PX API key
            rate_limit_delay: Delay between requests in seconds (default: 5 seconds)
        """
        self.api_key = api_key
        self.base_url = "https://api.aptrinsic.com/v1"
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update({
            'X-APTRINSIC-API-KEY': api_key,
            'Content-Type': 'application/json',
            'User-Agent': 'Gainsight-PX-Test-Script/1.0'
        })
        self.last_request_time = 0
        
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limits by waiting between requests"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last_request
            logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a rate-limited request to the API
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.RequestException: For API errors
        """
        self._wait_for_rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        
        # Log full URL with parameters
        if params:
            # Create a prepared request to get the full URL with params
            prepared_request = self.session.prepare_request(
                requests.Request('GET', url, params=params)
            )
            full_url = prepared_request.url
            logger.info(f"Making request to: {full_url}")
            logger.info(f"Request params: {json.dumps(params, indent=2)}")
        else:
            logger.info(f"Making request to: {url}")
            logger.info("Request params: None")
        
        # Log request headers
        logger.info("Request headers:")
        for header, value in self.session.headers.items():
            # Mask sensitive headers
            if 'key' in header.lower() or 'auth' in header.lower():
                logger.info(f"  {header}: ***MASKED***")
            else:
                logger.info(f"  {header}: {value}")
        
        try:
            response = self.session.get(url, params=params)
            
            # Log response status and headers
            logger.info(f"Response status: {response.status_code} {response.reason}")
            logger.info("Response headers:")
            for header, value in response.headers.items():
                logger.info(f"  {header}: {value}")
            
            # Log specific rate limit headers with emphasis
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = response.headers.get('X-RateLimit-Remaining')
                reset_time = response.headers.get('X-RateLimit-Reset')
                logger.info(f"🔄 RATE LIMIT INFO - Remaining: {remaining}, Reset: {reset_time}")
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', '60')
                logger.warning(f"⚠️  RATE LIMITED! Waiting {retry_after} seconds before retry")
                logger.warning(f"Response body: {response.text}")
                time.sleep(int(retry_after))
                return self._make_request(endpoint, params)  # Retry once
            
            # Log response body (truncated if too large)
            response_text = response.text
            if len(response_text) > 2000:  # Truncate very large responses
                logger.info(f"Response body (first 2000 chars): {response_text[:2000]}...")
                logger.info(f"Response body length: {len(response_text)} characters")
            else:
                logger.info(f"Response body: {response_text}")
            
            # Handle other errors
            response.raise_for_status()
            
            # Parse and log JSON structure
            json_response = response.json()
            logger.info("Response JSON structure:")
            if isinstance(json_response, dict):
                for key in json_response.keys():
                    if key == 'accounts':
                        logger.info(f"  {key}: array with {len(json_response[key])} items")
                    elif isinstance(json_response[key], (list, dict)):
                        logger.info(f"  {key}: {type(json_response[key]).__name__} with {len(json_response[key])} items")
                    else:
                        logger.info(f"  {key}: {json_response[key]}")
            
            return json_response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ REQUEST FAILED: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response headers: {dict(e.response.headers)}")
                logger.error(f"Response body: {e.response.text}")
            raise
    
    def get_accounts(self, scroll_id: Optional[str] = None, page_size: int = 100) -> Dict:
        """
        Fetch accounts from the Gainsight PX API
        
        Args:
            scroll_id: Pagination token for continuing from previous request
            page_size: Number of records per page (max 1000, default 100)
            
        Returns:
            API response containing accounts and pagination info
        """
        params = {
            'pageSize': min(page_size, 1000)  # Ensure we don't exceed max page size
        }
        if scroll_id:
            params['scrollId'] = scroll_id
        
        return self._make_request('/accounts', params)
    
    def get_all_accounts(self, page_size: int = 100) -> Generator[Dict, None, None]:
        """
        Generator that yields all account records, handling pagination automatically
        
        Args:
            page_size: Number of records per page (max 1000, default 100)
        
        Yields:
            Individual account records
        """
        scroll_id = None
        page_count = 0
        total_records = 0
        requested_page_size = min(page_size, 1000)
        
        logger.info(f"🔢 Using page size: {requested_page_size} records per request")
        
        while True:
            page_count += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"📄 FETCHING PAGE {page_count}")
            logger.info(f"{'='*60}")
            
            try:
                response = self.get_accounts(scroll_id, requested_page_size)
                
                # Extract accounts from response
                accounts = response.get('accounts', [])
                
                if not accounts:
                    logger.info("✅ No more accounts found - pagination complete")
                    break
                
                logger.info(f"✅ Retrieved {len(accounts)} accounts from page {page_count}")
                total_records += len(accounts)
                
                # Yield each account
                for account in accounts:
                    yield account
                
                # Check if we got fewer results than requested (API documentation guidance)
                if len(accounts) < requested_page_size:
                    logger.info(f"✅ Received {len(accounts)} < {requested_page_size} requested - pagination complete")
                    break
                
                # Get scroll ID for next page
                scroll_id = response.get('scrollId')
                if not scroll_id:
                    logger.info("✅ No more pages (no scrollId) - pagination complete")
                    break
                else:
                    logger.info(f"📋 Next page scroll ID: {scroll_id}")
                    
            except Exception as e:
                logger.error(f"❌ Error fetching page {page_count}: {e}")
                break
        
        logger.info(f"📊 Total records retrieved: {total_records} across {page_count} pages")


def save_accounts_to_file(accounts: List[Dict], filename: str):
    """Save accounts to a JSON file"""
    with open(filename, 'w') as f:
        json.dump(accounts, f, indent=2, default=str)
    logger.info(f"Saved {len(accounts)} accounts to {filename}")


def main():
    """Main function to test the Gainsight PX API"""
    
    # Load API key from config file (same format as Airbyte)
    try:
        with open('secrets/config.json', 'r') as f:
            config = json.load(f)
            api_key = config['api_key']
    except FileNotFoundError:
        logger.error("Config file not found. Please create secrets/config.json with your API key")
        logger.error("Format: {\"api_key\": \"your-api-key-here\"}")
        return
    except KeyError:
        logger.error("API key not found in config file")
        return
    
    # Initialize client with 1-second rate limiting (API supports 200 req/sec, so this is very conservative)
    client = GainsightPXClient(api_key, rate_limit_delay=1.0)
    
    logger.info("\n" + "="*80)
    logger.info("🚀 STARTING GAINSIGHT PX ACCOUNT INGESTION")
    logger.info("="*80)
    logger.info(f"🔄 Rate limit: 1 request every {client.rate_limit_delay} seconds")
    logger.info(f"🌐 Base URL: {client.base_url}")
    logger.info(f"📊 Test limit: 1000 records (modify script to change)")
    logger.info("="*80)
    
    # Collect all accounts
    accounts = []
    start_time = datetime.now()
    
    try:
        # Use larger page size for more efficient data retrieval
        for account in client.get_all_accounts(page_size=1000):
            accounts.append(account)
            
            # Log progress every 10 records
            if len(accounts) % 10 == 0:
                elapsed = datetime.now() - start_time
                logger.info(f"Progress: {len(accounts)} accounts retrieved in {elapsed}")
            
            # # Optional: Limit for testing (remove this for full sync)
            # if len(accounts) >= 100:  # Limit to 100 records for testing
            #     logger.info("Reached test limit of 100 records")
            #     break
                
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    # Save results
    if accounts:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"gainsight_accounts_{timestamp}.json"
        save_accounts_to_file(accounts, filename)
        
        # Print summary
        elapsed = datetime.now() - start_time
        logger.info(f"\n=== SUMMARY ===")
        logger.info(f"Total accounts retrieved: {len(accounts)}")
        logger.info(f"Total time: {elapsed}")
        logger.info(f"Average time per record: {elapsed / len(accounts) if accounts else 'N/A'}")
        logger.info(f"Results saved to: {filename}")
        
        # Show sample record
        if accounts:
            logger.info(f"\n=== SAMPLE RECORD ===")
            sample = accounts[0]
            logger.info(f"Account ID: {sample.get('id')}")
            logger.info(f"Account Name: {sample.get('name')}")
            logger.info(f"Number of Users: {sample.get('numberOfUsers')}")
            logger.info(f"Last Seen: {sample.get('lastSeenDate')}")
    else:
        logger.warning("No accounts retrieved")


if __name__ == "__main__":
    main()
