# On-Demand Data Request Implementation

## Overview
On-demand data retrieval is ALREADY IMPLEMENTED in both validator and miner templates. Miners have flexibility to customize/modify scraping logic if needed.

## For Miners
The base implementation uses Apidojo for X and Reddit API for Reddit scraping. You can:
- Use the default implementation as-is (recommended)
- Modify `handle_on_demand` in miner.py to use your own scrapers
- Build custom scraping logic while maintaining the same request/response format

## Rewards
- Top 50% of miners by stake participate in validation
- 50% chance of validation per request
- Successful validation: +1% credibility
- Failed validation: Proportional credibility decrease

That's it! The system is ready to use, but open for customization if you want to optimize your miner's performance. 🚀