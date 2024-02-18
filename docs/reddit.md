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