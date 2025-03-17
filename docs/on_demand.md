# On-Demand Data Request Implementation

## Overview
On-demand data retrieval is ALREADY IMPLEMENTED in both validator and miner templates. This enhanced version now provides richer metadata for X/Twitter content while maintaining the original Reddit implementation.

## For Miners

### X/Twitter Scraping (Enhanced)
The enhanced implementation uses `EnhancedApiDojoTwitterScraper` for X/Twitter which provides:

- **Rich User Metadata**
  - User ID, display name, verification status
  - Follower/following counts
  
- **Complete Tweet Information**
  - Engagement metrics (likes, retweets, replies, quotes, views)
  - Tweet type classification (reply, quote, retweet)
  - Conversation context and threading information
  
- **Media Content**
  - Media URLs and content types
  - Support for photos and videos
  
- **Advanced Formatting**
  - Properly ordered hashtags and cashtags
  - Full conversation context

### Reddit Scraping (Unchanged)
The Reddit implementation remains the same, using the Reddit API.

## Implementation Options

You can:
- Use the enhanced implementation as-is (recommended)
- Modify `handle_on_demand` in miner.py to use your own scrapers
- Build custom scraping logic while maintaining the same request/response format

### Integration Steps:

1. **Simple Integration**: Import the enhanced scraper and provider:
   ```python
   from scraping.x.enhanced_apidojo_scraper import EnhancedApiDojoTwitterScraper
   from scraping.x.on_demand_model import EnhancedXContent
   ```

2. **Update your scraper provider**:
   ```python
   # Create enhanced scraper provider 
   scraper_provider = EnhancedScraperProvider()
   ```

3. **Enjoy richer data**: The enhanced content is automatically used for X/Twitter requests

## Rewards
- Top 50% of miners by stake participate in validation
- 50% chance of validation per request
- Successful validation: +1% credibility
- Failed validation: Proportional credibility decrease

## Response Format Example

```json
{
  "uri": "https://x.com/username/status/123456789",
  "datetime": "2025-03-17T12:34:56+00:00",
  "source": "X",
  "label": "#bitcoin",
  "content": "Tweet text content...",
  "user": {
    "username": "@username",
    "display_name": "User Display Name",
    "id": "12345678",
    "verified": true,
    "followers_count": 10000,
    "following_count": 1000
  },
  "tweet": {
    "id": "123456789",
    "like_count": 500,
    "retweet_count": 100,
    "reply_count": 50,
    "quote_count": 25,
    "hashtags": ["#bitcoin", "#crypto"],
    "is_retweet": false,
    "is_reply": false,
    "is_quote": true,
    "conversation_id": "123456789"
  },
  "media": [
    {"url": "https://pbs.twimg.com/media/image1.jpg", "type": "photo"},
    {"url": "https://video.twimg.com/video1.mp4", "type": "video"}
  ]
}
```

That's it! The enhanced system is ready to use, providing significantly richer data while maintaining compatibility with existing implementations. 🚀