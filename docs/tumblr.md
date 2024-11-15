# Tumblr API Setup Guide

## Step 1: Register Your Application
1. Go to [Tumblr Application Registration](https://www.tumblr.com/oauth/apps)
2. Register a new application with any name/website

## Step 2: Get OAuth Keys
After registration, you'll get two initial keys:
- OAuth Consumer Key (API Key)
- OAuth Consumer Secret

## Step 3: Get Access Tokens
1. Visit [Tumblr API Console](https://api.tumblr.com/console/calls/user/info)
2. Enter your Consumer Key and Consumer Secret from Step 2
3. Click "Authenticate"
4. The console will provide your final OAuth tokens

You need all four keys:
```python
client = pytumblr.TumblrRestClient(
    'CONSUMER_KEY',        # OAuth Consumer Key
    'CONSUMER_SECRET',     # OAuth Consumer Secret
    'OAUTH_TOKEN',         # From API Console after authentication
    'OAUTH_SECRET'         # From API Console after authentication
)
```

## Step 4: Setup Environment Variables
Add to your `.env` file:
```env
TUMBLR_API_KEY="CONSUMER_KEY"              # From app registration
TUMBLR_SECRET_KEY="CONSUMER_SECRET"        # From app registration
TUMBLR_USER_KEY="OAUTH_TOKEN"             # From API Console
TUMBLR_USER_SECRET_KEY="OAUTH_SECRET"     # From API Console
```

## Quick Test
Verify your setup:
```python
from dotenv import load_dotenv
import os
import pytumblr

load_dotenv()

client = pytumblr.TumblrRestClient(
    os.getenv('TUMBLR_API_KEY'),
    os.getenv('TUMBLR_SECRET_KEY'),
    os.getenv('TUMBLR_USER_KEY'),
    os.getenv('TUMBLR_USER_SECRET_KEY')
)

# Test API access
info = client.info()
print("API Connection Successful!" if info else "API Connection Failed!")
```


## Troubleshooting
- If authentication fails, ensure you're using the correct key pairs
- Consumer keys come from app registration
- OAuth tokens come from API Console
- All keys should be different from each other

Need any adjustments or additional sections?