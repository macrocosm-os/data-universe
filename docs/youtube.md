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


