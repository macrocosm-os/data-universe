# Apify

[Apify](http://apify.com) is a popular platform and market place for web scraping tools.

Data Universe uses Apify to scrape certain DataSources. At this time, all Validators use Apify to validate miner-submitted data. Apify is optional for miners. We recommend miners build a custom scraper for economic purposes and to stay competitive on the subnet. 

## Setting your API Token

1. Create an Apify account
2. Got to your Console -> Settings -> Integrations and copy your Personal API token
3. Create a file named `.env` in the `data-universe` directory if it doesn't already exist and add the following to it:
```py
APIFY_API_TOKEN="YOUR_APIFY_API_TOKEN"
```
