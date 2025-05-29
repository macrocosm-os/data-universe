Step-by-Step Instructions
1. Create a Google Cloud Project
- Visit [Google Cloud Console](https://console.cloud.google.com/).
- Click on the project drop-down and select “New Project”.
- Enter a name for your project (e.g., YouTubeScraper) and click “Create”.

2. Enable the YouTube Data API v3
- Search for YouTube Data API v3
- Click on it and then click “Enable”.

3. Generate an API Key
- Go to the Credentials page.
- Click “Create Credentials” > “API Key”.
- A new API key will be generated. Copy and save it.

4. Set the API Key in Environment Variables
- Add the following to your .env file in the root directory of your project:
`YOUTUBE_API_KEY=your_actual_api_key_here
`

### Working with proxies ( webShare)
If you are using cloud provider that belong to popular cloud providers (like AWS, Google Cloud Platform, Azure, DO etc.),

You will need to add WebShare proxy
Once you have created a Webshare account and purchased a "Residential" proxy package that suits your workload 5 GB for a vali (make sure NOT to purchase "Proxy Server" or "Static Residential"!), 
open the Webshare Proxy Settings to retrieve your "Proxy Username" and "Proxy Password". Using this information you can initialize the validator as follows:
`WEB_SHARE_PROXY_USERNAME=WEB_SHARE_PROXY_USERNAME
WEB_SHARE_PROXY_PASSWORD=WEB_SHARE_PROXY_PASSWORD`

### Working with any other proxies 

If you don't like webshare, you can define this works with any http proxy!

YTT_PROXY_HOST=127.0.0.01
YTT_PROXY_PORT=7777
YTT_PROXY_USERNAME=myusername
YTT_PROXY_PASSWORD=mypassword
