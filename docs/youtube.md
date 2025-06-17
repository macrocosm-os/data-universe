# YouTube

[YouTube](https://youtube.com) is one source that Data Universe can pull from using the Apify-based transcript scraper.

## YouTube API Setup

### Step-by-Step Instructions

1. **Create a Google Cloud Project**
   - Visit [Google Cloud Console](https://console.cloud.google.com/).
   - Click on the project drop-down and select "New Project".
   - Enter a name for your project (e.g., YouTubeScraper) and click "Create".

2. **Enable the YouTube Data API v3**
   - Search for YouTube Data API v3
   - Click on it and then click "Enable".

3. **Generate an API Key**
   - Go to the Credentials page.
   - Click "Create Credentials" > "API Key".
   - A new API key will be generated. Copy and save it.

4. **Set the API Key in Environment Variables**
   - Add the following to your .env file in the root directory of your project:
   ```bash
   YOUTUBE_API_KEY=your_actual_api_key_here
   ```

## Apify Integration

YouTube transcript scraping is handled through the Apify platform using the `pintostudio/youtube-transcript-scraper` actor. This provides reliable transcript extraction without the need for proxy configuration.

### Environment Variables Required

```bash
# YouTube Data API (for metadata)
YOUTUBE_API_KEY=your_youtube_api_key

# Apify API (for transcript extraction)
APIFY_API_TOKEN=your_apify_api_token
```

## Benefits of the New Implementation

- **No Proxy Configuration Required**: Eliminates the complexity of setting up WebShare or other proxy services
- **Better Reliability**: Uses Apify's robust infrastructure
- **Simplified Setup**: Only requires YouTube API key and Apify token
- **Consistent Integration**: Uses the same actor pattern as other scrapers in the system

## Supported Features

- Channel-based video scraping using channel IDs
- Individual video transcript extraction using video IDs
- Automatic transcript compression for storage efficiency
- Enhanced validation with metadata verification
- Support for both old and new label formats (`#youtube_*` and `#ytc_*`)

The YouTube scraper now provides a much more streamlined experience compared to the previous implementation that required complex proxy configurations.