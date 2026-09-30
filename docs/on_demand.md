# On-Demand Data Request Implementation

## Overview
On-demand data retrieval is ALREADY IMPLEMENTED in both validator and miner templates. The unified XContent model now provides richer metadata for X/Twitter content while maintaining compatibility with the original Reddit implementation.

## Request Modes

### X/Twitter Request Types
X/Twitter on-demand requests support three modes:
1. **Username/Keyword Mode**: Search tweets from specific users and/or tweets containing specific keywords/hashtags
2. **URL Mode**: Fetch a specific tweet by its URL (mutually exclusive with username/keyword modes)

### Request Examples

**Username/Keyword Mode:**
```python
OnDemandRequest(
    source="X",
    usernames=["elonmusk", "naval"],
    keywords=["bitcoin", "crypto"],
    keyword_mode="any",
    start_date="2024-01-01T00:00:00Z",
    end_date="2024-12-31T23:59:59Z",
    limit=100
)
```

**URL Mode (Direct Tweet Lookup):**
```python
OnDemandRequest(
    source="X",
    url="https://x.com/elonmusk/status/1234567890",
    start_date="2024-01-01T00:00:00Z",
    end_date="2024-12-31T23:59:59Z",
    limit=10
)
```

**Note**: URL mode is mutually exclusive with `usernames` and `keywords` fields. If `url` is provided, `usernames` and `keywords` must be empty or omitted.

## For Miners

### X/Twitter Scraping (Unified XContent)
The implementation uses the standard `ApiDojoTwitterScraper` with the unified `XContent` model which provides:

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
- Replace the scraper calls in `scrape_on_demand_job` / `loop_poll_on_demand_active_jobs` in `neurons/miner.py` with your own scrapers
- Build custom scraping logic while maintaining the same request/response format

### Integration Steps:

1. **Simple Integration**: Import the standard scraper:
   ```python
   from scraping.x.apidojo_scraper import ApiDojoTwitterScraper
   from scraping.x.model import XContent
   ```

2. **Update your scraper provider**:
   ```python
   # Create scraper provider with unified XContent
   scraper_provider = ScraperProvider()
   ```

3. **Enjoy richer data**: The enhanced content is automatically used for X/Twitter requests

## Rewards

Validators evaluate each miner about once an hour (`vali_utils/miner_evaluator.py`, `_evaluate_od`):

- Submissions for jobs that expired since the last evaluation are listed (3 h window). Zero-byte submissions earn nothing and cost nothing.
- Up to 3 non-empty submissions are sampled at random. For each: 5 entities are checked for format and for matching the request (usernames, keywords, keyword mode, dates, url), then one entity is re-fetched from the live source.
- Pass: the on-demand boost moves toward `1e8 × speed × volume` (EMA, alpha 0.3) and credibility toward 1 (alpha 0.02). Fail: boost × 0.7, credibility − 0.05. Scraper outages and unfetchable content are neutral.
- `speed = clamp(0.5 ^ ((t − 30 s) / 45 s), 0.3, 1.0)` where `t` is submission time minus job creation. `volume = (rows / limit) ^ 1.3` below the limit, with a bonus capped at 1.25 above it.
- A per-platform coverage multiplier applies: submitting nothing on a platform with available jobs is abstention (× 0.3); submitting fewer than 15% of available jobs scales down to a floor of 0.3.

Constants live in `rewards/miner_scorer.py` and `vali_utils/on_demand/on_demand_validation.py`. The composite score is `min(s3_boost, 2 × od) × s3_cred^2.5 + od`, so a miner with no on-demand activity earns nothing from bulk uploads.

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