# Reddit

[Reddit](https://reddit.com) is one source that Data Universe can pull from.

In addition to the [Apify actor based scraping](apify.md) we also support using a personal reddit account.

## Getting a reddit account.

If you already have a reddit account you can use that one. Otherwise [sign up](https://www.reddit.com/register/) for one (must support password based auth).

## Setting up your account for use with a script type app.

Follow the [OAuth2 First Steps guide](https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example#first-steps) to add a script type app to your account and find the associated app client id and app client secret. Do not share your client secret with anyone.

## Providing your information to your miner or validator.

Create a file named`.env` in the `data-universe` directory if it doesn't already exist and add the following to it:
```py
REDDIT_CLIENT_ID="YOUR_REDDIT_CLIENT_ID"
REDDIT_CLIENT_SECRET="YOUR_REDDIT_CLIENT_SECRET"
REDDIT_USERNAME="YOUR_REDDIT_USERNAME"
REDDIT_PASSWORD="YOUR_REDDIT_PASSWORD"
```

## Reddit JSON API Technical Notes

### The `raw_json=1` Parameter

When using Reddit's public JSON API (e.g., `https://reddit.com/r/subreddit.json`), the API returns HTML-encoded entities by default:

| Without `raw_json=1` | With `raw_json=1` |
|---------------------|-------------------|
| `&gt;` | `>` |
| `&lt;` | `<` |
| `&amp;` | `&` |

This causes **validation mismatches** because:
- **PRAW** (Python Reddit API Wrapper) returns **unescaped text**
- **Reddit JSON API** returns **HTML-encoded text** by default

#### Solution

Always append `?raw_json=1` (or `&raw_json=1` if other params exist) to Reddit JSON API URLs:

```python
# Without raw_json=1 (causes body mismatch)
url = "https://www.reddit.com/r/python/new.json?limit=10"

# With raw_json=1 (matches PRAW output)
url = "https://www.reddit.com/r/python/new.json?limit=10&raw_json=1"
```

#### Example

```python
# Comment body WITHOUT raw_json=1:
'&gt; Quote from someone\n\nMy response with &amp; symbols'

# Comment body WITH raw_json=1:
'> Quote from someone\n\nMy response with & symbols'
```

#### References

- [JRAW Issue #225](https://github.com/mattbdean/JRAW/issues/225) - Documents the fix for HTML entity escaping
- [Teddit Issue #220](https://codeberg.org/teddit/teddit/issues/220) - Confirms `raw_json=1` returns unescaped content
- [Reddit Archive Wiki - JSON](https://github.com/reddit-archive/reddit/wiki/json) - Official documentation noting HTML escaping

### URL Normalization

When fetching individual posts/comments, normalize the URL before adding `.json?raw_json=1`:

```python
def normalize_reddit_url(url: str) -> str:
    """Normalize Reddit URL for JSON API fetch."""
    clean_url = url.rstrip('/')

    # Remove .json if already present
    if clean_url.endswith('.json'):
        clean_url = clean_url[:-5]

    # Remove existing query parameters
    if '?' in clean_url:
        clean_url = clean_url.split('?')[0]

    # Add .json with raw_json=1
    return f"{clean_url}.json?raw_json=1"
```

This handles edge cases where miners might pass URLs with:
- Trailing slashes (`/r/sub/comments/123/`)
- Already having `.json` suffix
- Existing query parameters