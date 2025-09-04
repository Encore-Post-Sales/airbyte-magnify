# Gainsight PX API Test Script

This Python script allows you to test the Gainsight PX API directly, making HTTP calls to ingest account records with proper rate limiting.

## Setup

1. **Install dependencies:**

   ```bash
   pip install -r requirements_test.txt
   ```

2. **Configure your API key:**
   Make sure you have your API key in `secrets/config.json`:
   ```json
   {
     "api_key": "your-gainsight-px-api-key-here"
   }
   ```

## Usage

Run the script:

```bash
python test_gainsight_api.py
```

## Features

- **Rate Limiting**: Automatically waits 5 seconds between requests (matching your manifest configuration)
- **Pagination**: Handles scrollId-based pagination automatically
- **Error Handling**: Properly handles 429 rate limit responses and other API errors
- **Progress Logging**: Shows detailed progress and timing information
- **Rate Limit Headers**: Monitors and logs API rate limit headers when available
- **JSON Output**: Saves results to a timestamped JSON file

## Output

The script will:

1. Log progress to the console
2. Save all retrieved accounts to a timestamped JSON file (e.g., `gainsight_accounts_20241212_143022.json`)
3. Show a summary with timing statistics
4. Display a sample record

## Rate Limiting Strategy

The script implements the same 5-second rate limiting strategy as your manifest:

- Waits exactly 5 seconds between requests
- Handles 429 responses with automatic retry
- Respects `Retry-After` headers when provided
- Logs rate limit information from response headers

## Testing Limits

By default, the script limits to 100 records for testing. To retrieve all accounts, remove or modify this line in `main()`:

```python
if len(accounts) >= 100:  # Remove this block for full sync
    logger.info("Reached test limit of 100 records")
    break
```

## Troubleshooting

1. **Config file not found**: Ensure `secrets/config.json` exists with your API key
2. **Rate limiting**: The script automatically handles rate limits - be patient as it waits 5 seconds between requests
3. **API errors**: Check the logs for detailed error messages and HTTP response codes
